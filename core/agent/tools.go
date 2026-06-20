// core/agent/tools.go
package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/fabriq/core/query"
)

// Tool is a transport-neutral agent tool descriptor. The MCP adapter maps each
// Tool to an MCP tool 1:1; Go agents can dispatch through Tools() or call the
// typed methods (e.g. Toolkit.Recall) directly.
type Tool struct {
	Name        string
	Description string
	InputSchema json.RawMessage
	Handler     func(ctx context.Context, args json.RawMessage) (any, error)
}

// Tools returns the agent-facing tool surface. Phase 1b exposes recall plus
// the four read primitives: vector_similar, search, graph_traverse, get.
// Phase 3 adds the guarded write tool: remember.
// Phase 4 adds the context-distillation tools: map, digest, resolve.
func (t *Toolkit) Tools() []Tool {
	return []Tool{
		t.recallTool(),
		t.vectorSimilarTool(),
		t.searchTool(),
		t.graphTraverseTool(),
		t.getTool(),
		t.rememberTool(),
		t.mapTool(),
		t.digestTool(),
		t.resolveTool(),
	}
}

func (t *Toolkit) recallTool() Tool {
	return Tool{
		Name:        "recall",
		Description: "Retrieve a ranked, token-budgeted context pack for a natural-language query across the knowledge base.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "required": ["query", "budget", "entities"],
  "properties": {
    "query": {"type": "string", "description": "natural-language query"},
    "budget": {"type": "integer", "description": "token budget for the returned pack"},
    "entities": {"type": "array", "items": {"type": "string"}, "description": "entity types to recall from"},
    "k": {"type": "integer", "description": "candidates per channel (default 24)"},
    "hops": {"type": "integer", "description": "graph expansion depth (default 1)"}
  }
}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var req RecallRequest
			if err := json.Unmarshal(args, &req); err != nil {
				return nil, fmt.Errorf("agent: recall args: %w", err)
			}
			return t.Recall(ctx, req)
		},
	}
}

func (t *Toolkit) vectorSimilarTool() Tool {
	return Tool{
		Name:        "vector_similar",
		Description: "Semantic nearest-neighbour search for an entity by query text.",
		InputSchema: json.RawMessage(`{"type":"object","required":["entity","query"],"properties":{"entity":{"type":"string"},"query":{"type":"string"},"k":{"type":"integer"}}}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var a struct {
				Entity string `json:"entity"`
				Query  string `json:"query"`
				K      int    `json:"k"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, fmt.Errorf("agent: vector_similar args: %w", err)
			}
			if t.emb == nil {
				return nil, fmt.Errorf("agent: vector_similar requires an embedder")
			}
			vecs, err := t.emb.Embed(ctx, []string{a.Query})
			if err != nil {
				return nil, fmt.Errorf("agent: vector_similar embed: %w", err)
			}
			if len(vecs) != 1 {
				return nil, fmt.Errorf("agent: vector_similar embed returned %d vectors", len(vecs))
			}
			k := a.K
			if k <= 0 {
				k = t.cfg.K
			}
			var matches []query.VectorMatch
			if err := t.fab.Vector().Similar(ctx, query.VectorQuery{Entity: a.Entity, Embedding: vecs[0], K: k}, &matches); err != nil {
				return nil, fmt.Errorf("agent: vector_similar: %w", err)
			}
			return matches, nil
		},
	}
}

func (t *Toolkit) searchTool() Tool {
	return Tool{
		Name:        "search",
		Description: "Full-text search over an entity's indexed fields.",
		InputSchema: json.RawMessage(`{"type":"object","required":["entity","query"],"properties":{"entity":{"type":"string"},"query":{"type":"string"},"limit":{"type":"integer"},"offset":{"type":"integer"}}}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var a struct {
				Entity string `json:"entity"`
				Query  string `json:"query"`
				Limit  int    `json:"limit"`
				Offset int    `json:"offset"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, fmt.Errorf("agent: search args: %w", err)
			}
			var hits []map[string]any
			if err := t.fab.Search().Search(ctx, query.SearchQuery{Entity: a.Entity, Query: a.Query, Limit: a.Limit, Offset: a.Offset}, &hits); err != nil {
				return nil, fmt.Errorf("agent: search: %w", err)
			}
			return hits, nil
		},
	}
}

func (t *Toolkit) graphTraverseTool() Tool {
	return Tool{
		Name: "graph_traverse",
		// NOTE: this tool passes caller-supplied cypher to the graph engine with
		// a defense-in-depth denylist guard (readOnlyCypher) rejecting mutating
		// clauses. For deployments handling untrusted callers, a read-only graph
		// connection or driver-level flag is still recommended as a second layer.
		Description: "Run a read-only openCypher traversal (caller-supplied) returning column-keyed rows.",
		InputSchema: json.RawMessage(`{"type":"object","required":["cypher"],"properties":{"cypher":{"type":"string"},"params":{"type":"object"}}}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var a struct {
				Cypher string         `json:"cypher"`
				Params map[string]any `json:"params"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, fmt.Errorf("agent: graph_traverse args: %w", err)
			}
			if err := readOnlyCypher(a.Cypher); err != nil {
				return nil, err
			}
			var rows []map[string]any
			if err := t.fab.Graph().Query(ctx, a.Cypher, a.Params, &rows); err != nil {
				return nil, fmt.Errorf("agent: graph_traverse: %w", err)
			}
			return rows, nil
		},
	}
}

func (t *Toolkit) rememberTool() Tool {
	return Tool{
		Name:        "remember",
		Description: "Create, update, upsert, or delete an entity (subject to the deployment's write policy).",
		InputSchema: json.RawMessage(`{"type":"object","required":["entity","op"],"properties":{"entity":{"type":"string"},"op":{"type":"string","enum":["create","update","upsert","delete"]},"aggId":{"type":"string"},"payload":{"type":"object"},"expectedVersion":{"type":"integer"}}}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var req RememberRequest
			if err := json.Unmarshal(args, &req); err != nil {
				return nil, fmt.Errorf("agent: remember args: %w", err)
			}
			return t.Remember(ctx, req)
		},
	}
}

func (t *Toolkit) getTool() Tool {
	return Tool{
		Name:        "get",
		Description: "Fetch one entity row by id as a JSON object.",
		InputSchema: json.RawMessage(`{"type":"object","required":["entity","id"],"properties":{"entity":{"type":"string"},"id":{"type":"string"}}}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var a struct {
				Entity string `json:"entity"`
				ID     string `json:"id"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, fmt.Errorf("agent: get args: %w", err)
			}
			rows, err := t.hydrate(ctx, a.Entity, []string{a.ID})
			if err != nil {
				return nil, fmt.Errorf("agent: get: %w", err)
			}
			raw, ok := rows[a.ID]
			if !ok {
				return nil, fmt.Errorf("agent: get: %s %q not found", a.Entity, a.ID)
			}
			return raw, nil
		},
	}
}

// mapTool returns the "map" tool descriptor. No required fields: all
// MapRequest fields are optional. Tolerates nil/empty args gracefully.
func (t *Toolkit) mapTool() Tool {
	return Tool{
		Name:        "map",
		Description: "Bird's-eye outline of the distillation tree (tenant → scope/cluster → entity), each line carrying its ContentHash + SemHash. Pass knownHashes to get a Merkle diff (unchanged subtrees flagged).",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"scope":{"type":"string","description":"restrict outline to nodes matching this scope_id (plus the tenant root)"},"knownHashes":{"type":"object","additionalProperties":{"type":"string"},"description":"caller's local snapshot map[nodeID]contentHash — matching nodes are marked unchanged"}}}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var req MapRequest
			if len(args) > 0 {
				if err := json.Unmarshal(args, &req); err != nil {
					return nil, fmt.Errorf("agent: map args: %w", err)
				}
			}
			return t.Map(ctx, req)
		},
	}
}

// digestTool returns the "digest" tool descriptor. Requires nodeId.
func (t *Toolkit) digestTool() Tool {
	return Tool{
		Name:        "digest",
		Description: "Drill into one context-distillation node: returns its MapLine, summary text (from CAS when configured), and immediate children with their hashes and summaries.",
		InputSchema: json.RawMessage(`{"type":"object","required":["nodeId"],"properties":{"nodeId":{"type":"string","description":"id of the digest_node to inspect"}}}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var a struct {
				NodeID string `json:"nodeId"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, fmt.Errorf("agent: digest args: %w", err)
			}
			return t.Digest(ctx, a.NodeID)
		},
	}
}

// resolveTool returns the "resolve" tool descriptor. Requires hash.
func (t *Toolkit) resolveTool() Tool {
	return Tool{
		Name:        "resolve",
		Description: "Quick hash reference lookup: Exact match on ContentHash + Near match on SemHash (Hamming distance ≤ 8). No re-embedding required.",
		InputSchema: json.RawMessage(`{"type":"object","required":["hash"],"properties":{"hash":{"type":"string","description":"ContentHash or 16-hex SemHash to look up"}}}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var a struct {
				Hash string `json:"hash"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, fmt.Errorf("agent: resolve args: %w", err)
			}
			return t.Resolve(ctx, a.Hash)
		},
	}
}
