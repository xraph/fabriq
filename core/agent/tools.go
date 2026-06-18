// core/agent/tools.go
package agent

import (
	"context"
	"encoding/json"
	"fmt"
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

// Tools returns the agent-facing tool surface. Phase 1a exposes recall; read
// primitives, remember, and watch arrive in later phases.
func (t *Toolkit) Tools() []Tool {
	return []Tool{t.recallTool()}
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
