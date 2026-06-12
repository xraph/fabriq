package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/xraph/fabriq/core/registry"
)

// targetVersion parses a rebuild target tag ("v3" -> 3); "" means live.
func targetVersion(target string) (int, error) {
	if target == "" {
		return 0, nil
	}
	if !strings.HasPrefix(target, "v") {
		return 0, fmt.Errorf("fabriq: search target must be \"\" or \"vN\", got %q", target)
	}
	n, err := strconv.Atoi(target[1:])
	if err != nil || n < 1 {
		return 0, fmt.Errorf("fabriq: search target must be \"\" or \"vN\", got %q", target)
	}
	return n, nil
}

// SearchTargetName is the Rebuilder naming function for the search
// projection: targets are version tags interpreted by this sink.
func SearchTargetName(_ string, modelVersion int) string {
	return fmt.Sprintf("v%d", modelVersion)
}

// ensureIndex creates an index once (idempotent, cached).
func (a *Adapter) ensureIndex(ctx context.Context, index string) error {
	a.mu.Lock()
	_, ok := a.ensured[index]
	a.mu.Unlock()
	if ok {
		return nil
	}
	res, err := a.es.Indices.Create(index, a.es.Indices.Create.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("fabriq: create index %s: %w", index, err)
	}
	defer drainAndClose(res.Body)
	if res.IsError() && res.StatusCode != 400 { // 400 = already exists
		return fmt.Errorf("fabriq: create index %s: %s", index, res.String())
	}
	a.mu.Lock()
	a.ensured[index] = struct{}{}
	a.mu.Unlock()
	return nil
}

// ensureAlias points the tenant alias at the live versioned index once.
func (a *Adapter) ensureAlias(ctx context.Context, alias, index string) error {
	a.mu.Lock()
	_, ok := a.aliased[alias]
	a.mu.Unlock()
	if ok {
		return nil
	}
	res, err := a.es.Indices.PutAlias([]string{index}, alias, a.es.Indices.PutAlias.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("fabriq: put alias %s -> %s: %w", alias, index, err)
	}
	defer drainAndClose(res.Body)
	if res.IsError() {
		return fmt.Errorf("fabriq: put alias %s -> %s: %s", alias, index, res.String())
	}
	a.mu.Lock()
	a.aliased[alias] = struct{}{}
	a.mu.Unlock()
	return nil
}

// FlipAliases atomically repoints every searchable entity's tenant alias
// at the new model version's indexes — the search projection's blue-green
// cutover, one _aliases call. Removals are derived from what ACTUALLY
// holds each alias (entities that never indexed a document have no old
// index, and a remove for a nonexistent pair would fail the whole atomic
// action set). Wire it as the Rebuilder's OnFlip.
func (a *Adapter) FlipAliases(ctx context.Context, tenantID string, _, newVersion int) error {
	var actions []string
	for _, ent := range a.reg.All() {
		base := ent.Spec.Search.Index
		if base == "" {
			continue
		}
		alias := registry.SearchIndexAlias(tenantID, base)
		newIdx := registry.SearchIndexVersioned(tenantID, base, newVersion)
		if err := a.ensureIndex(ctx, newIdx); err != nil {
			return err
		}
		current, err := a.aliasHolders(ctx, alias)
		if err != nil {
			return err
		}
		for _, holder := range current {
			if holder != newIdx {
				actions = append(actions, fmt.Sprintf(`{"remove":{"index":%q,"alias":%q}}`, holder, alias))
			}
		}
		actions = append(actions, fmt.Sprintf(`{"add":{"index":%q,"alias":%q}}`, newIdx, alias))
	}
	if len(actions) == 0 {
		return nil
	}
	body := fmt.Sprintf(`{"actions":[%s]}`, strings.Join(actions, ","))
	res, err := a.es.Indices.UpdateAliases(strings.NewReader(body), a.es.Indices.UpdateAliases.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("fabriq: swap aliases for %s: %w", tenantID, err)
	}
	defer drainAndClose(res.Body)
	if res.IsError() {
		return fmt.Errorf("fabriq: swap aliases for %s: %s", tenantID, res.String())
	}
	// Aliases moved: drop the memoization so live writes re-resolve.
	a.mu.Lock()
	a.aliased = map[string]struct{}{}
	a.mu.Unlock()
	return nil
}

// aliasHolders lists the indexes currently holding an alias (empty when
// the alias does not exist yet).
func (a *Adapter) aliasHolders(ctx context.Context, alias string) ([]string, error) {
	res, err := a.es.Indices.GetAlias(
		a.es.Indices.GetAlias.WithContext(ctx),
		a.es.Indices.GetAlias.WithName(alias),
	)
	if err != nil {
		return nil, fmt.Errorf("fabriq: get alias %s: %w", alias, err)
	}
	defer drainAndClose(res.Body)
	if res.StatusCode == 404 {
		return nil, nil
	}
	if res.IsError() {
		return nil, fmt.Errorf("fabriq: get alias %s: %s", alias, res.String())
	}
	var parsed map[string]any // index name -> {aliases: {...}}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	holders := make([]string, 0, len(parsed))
	for idx := range parsed {
		holders = append(holders, idx)
	}
	sort.Strings(holders)
	return holders, nil
}

// DropTarget deletes one model version's indexes for a tenant (rebuild
// cleanup). target is a "vN" tag.
func (a *Adapter) DropTarget(ctx context.Context, target string) error {
	// The tenant rides on ctx for sinks; DropTarget is called with the
	// rebuild tenant stamped.
	version, err := targetVersion(target)
	if err != nil || version == 0 {
		return fmt.Errorf("fabriq: drop search target %q: need a vN tag", target)
	}
	tenantID, terr := tenantFrom(ctx)
	if terr != nil {
		return terr
	}
	for _, ent := range a.reg.All() {
		base := ent.Spec.Search.Index
		if base == "" {
			continue
		}
		index := registry.SearchIndexVersioned(tenantID, base, version)
		res, err := a.es.Indices.Delete([]string{index},
			a.es.Indices.Delete.WithContext(ctx), a.es.Indices.Delete.WithIgnoreUnavailable(true))
		if err != nil {
			return fmt.Errorf("fabriq: delete index %s: %w", index, err)
		}
		drainAndClose(res.Body)
		a.mu.Lock()
		delete(a.ensured, index)
		a.mu.Unlock()
	}
	return nil
}
