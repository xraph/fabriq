package falkordb

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/xraph/fabriq/core/projection"
)

// This file is the ONLY place graph dialect lives. Appliers emit
// engine-neutral mutations; cypherFor translates them into the openCypher
// COMMON SUBSET (no FalkorDB-specific functions), which is what the
// adapters/graphtest conformance suite gates. Swapping in Memgraph, Neo4j
// or Kùzu means re-implementing this translation against the same suite.
//
// Replay safety: every write is gated on the aggregate node's stored
// version (<= incoming), so at-least-once delivery and out-of-order
// retries cannot regress the graph.

// identPattern validates labels, relationship types, property keys and
// parameter names. They are interpolated into Cypher text (identifiers
// cannot be parameterized); everything else travels as a parameter.
var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func validIdent(s string) bool { return identPattern.MatchString(s) }

// cypherFor renders one mutation as (cypher, params).
func cypherFor(m projection.Mutation) (cypher string, params map[string]any, err error) {
	switch mut := m.(type) {
	case projection.NodeUpsert:
		if !validIdent(mut.Label) {
			return "", nil, fmt.Errorf("fabriq: invalid graph label %q", mut.Label)
		}
		params = map[string]any{"id": mut.ID, "version": mut.Version}
		// Per-property SET with scalar params (map-valued parameters are
		// not portable across engines). Keys are registry column names;
		// anything failing identifier validation is dropped.
		keys := make([]string, 0, len(mut.Props))
		for k := range mut.Props {
			if validIdent(k) && k != "id" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		sets := make([]string, 0, len(keys))
		for _, k := range keys {
			params["p_"+k] = mut.Props[k]
			sets = append(sets, fmt.Sprintf("n.%s = $p_%s", k, k))
		}
		cy := fmt.Sprintf(`MERGE (n:%s {id: $id})
WITH n
WHERE coalesce(n.version, 0) <= $version`, mut.Label)
		if len(sets) > 0 {
			cy += "\nSET " + strings.Join(sets, ", ")
		}
		return cy, params, nil

	case projection.EdgeUpsert:
		if !validIdent(mut.Rel) || !validIdent(mut.FromLabel) || !validIdent(mut.ToLabel) {
			return "", nil, fmt.Errorf("fabriq: invalid edge identifiers %q/%q/%q", mut.Rel, mut.FromLabel, mut.ToLabel)
		}
		// Gate on the FROM node (its NodeUpsert lands first in the same
		// event's batch); FK semantics: one outgoing edge per rel type,
		// stale targets removed after the merge.
		cy := fmt.Sprintf(`MATCH (from:%[1]s {id: $from_id})
WHERE coalesce(from.version, 0) <= $version
MERGE (to:%[3]s {id: $to_id})
MERGE (from)-[r:%[2]s]->(to)
SET r.version = $version
WITH from
OPTIONAL MATCH (from)-[stale:%[2]s]->(old)
WHERE old.id <> $to_id
DELETE stale`, mut.FromLabel, mut.Rel, mut.ToLabel)
		return cy, map[string]any{"from_id": mut.FromID, "to_id": mut.ToID, "version": mut.Version}, nil

	case projection.NodeDelete:
		if !validIdent(mut.Label) {
			return "", nil, fmt.Errorf("fabriq: invalid graph label %q", mut.Label)
		}
		cy := fmt.Sprintf(`MATCH (n:%s {id: $id})
DETACH DELETE n`, mut.Label)
		return cy, map[string]any{"id": mut.ID}, nil

	case projection.EdgeDelete:
		if !validIdent(mut.Rel) || !validIdent(mut.FromLabel) {
			return "", nil, fmt.Errorf("fabriq: invalid edge identifiers %q/%q", mut.Rel, mut.FromLabel)
		}
		cy := fmt.Sprintf(`MATCH (from:%s {id: $from_id})-[r:%s]->()
WHERE coalesce(from.version, 0) <= $version
DELETE r`, mut.FromLabel, mut.Rel)
		return cy, map[string]any{"from_id": mut.FromID, "version": mut.Version}, nil

	default:
		return "", nil, fmt.Errorf("fabriq: mutation %T is not a graph mutation", m)
	}
}

// cypherParams renders the FalkorDB "CYPHER k=v ..." parameter prefix.
// Values are serialized as Cypher literals (FalkorDB has no wire-level
// parameter binding); strings are quote-escaped, names validated.
func cypherParams(params map[string]any) (string, error) {
	if len(params) == 0 {
		return "", nil
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		if !validIdent(k) {
			return "", fmt.Errorf("fabriq: invalid cypher parameter name %q", k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString("CYPHER ")
	for _, k := range keys {
		lit, err := cypherLiteral(params[k])
		if err != nil {
			return "", fmt.Errorf("fabriq: parameter %q: %w", k, err)
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(lit)
		sb.WriteByte(' ')
	}
	return sb.String(), nil
}

func cypherLiteral(v any) (string, error) {
	switch val := v.(type) {
	case nil:
		return "null", nil
	case string:
		return quoteCypherString(val), nil
	case bool:
		if val {
			return "true", nil
		}
		return "false", nil
	case int:
		return fmt.Sprintf("%d", val), nil
	case int32:
		return fmt.Sprintf("%d", val), nil
	case int64:
		return fmt.Sprintf("%d", val), nil
	case float32:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", val), "0"), "."), nil
	case float64:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", val), "0"), "."), nil
	case []string:
		parts := make([]string, len(val))
		for i, s := range val {
			parts[i] = quoteCypherString(s)
		}
		return "[" + strings.Join(parts, ", ") + "]", nil
	case []any:
		parts := make([]string, len(val))
		for i, item := range val {
			lit, err := cypherLiteral(item)
			if err != nil {
				return "", err
			}
			parts[i] = lit
		}
		return "[" + strings.Join(parts, ", ") + "]", nil
	default:
		return "", fmt.Errorf("unsupported cypher literal type %T", v)
	}
}

func quoteCypherString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}
