// Package agentmcp exposes the fabriq agent toolkit over MCP (JSON-RPC 2.0).
package agentmcp

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/xraph/fabriq/core/agent"
)

// JSON-RPC 2.0 + MCP wire types (minimal, single-request).
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type mcpTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type callResult struct {
	Content []contentBlock `json:"content"`
	IsError bool           `json:"isError,omitempty"`
}

// JSON-RPC standard error codes.
const (
	codeParseError     = -32700
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
)

// Dispatch handles one MCP JSON-RPC request against the toolkit and returns the
// response bytes. Transport errors (parse/method/params) become JSON-RPC error
// objects; tool execution errors become MCP results with isError=true.
// Note: JSON-RPC notifications (requests without an "id") and the jsonrpc
// version field are not specially validated — this is the documented minimal
// single-request scope.
func Dispatch(ctx context.Context, tk *agent.Toolkit, body []byte) []byte {
	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return marshal(rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: codeParseError, Message: "parse error"}})
	}
	switch req.Method {
	case "tools/list":
		tools := tk.Tools()
		mcpTools := make([]mcpTool, 0, len(tools))
		for _, t := range tools {
			mcpTools = append(mcpTools, mcpTool{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema})
		}
		return marshal(rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: map[string]any{"tools": mcpTools}})

	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return marshal(rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: codeInvalidParams, Message: "invalid params"}})
		}
		tools := tk.Tools()
		var found *agent.Tool
		for i := range tools {
			if tools[i].Name == p.Name {
				t := tools[i]
				found = &t
				break
			}
		}
		if found == nil {
			return marshal(rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: codeInvalidParams, Message: "unknown tool: " + p.Name}})
		}
		out, err := found.Handler(ctx, p.Arguments)
		if err != nil {
			return marshal(rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: callResult{
				IsError: true,
				Content: []contentBlock{{Type: "text", Text: errorText(err)}},
			}})
		}
		text, _ := json.Marshal(out)
		return marshal(rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: callResult{
			Content: []contentBlock{{Type: "text", Text: string(text)}},
		}})

	default:
		return marshal(rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: codeMethodNotFound, Message: "method not found: " + req.Method}})
	}
}

// errorText renders a tool error as a human-readable string. For WriteError the
// message already embeds the code (e.g. "agent: write not_allowed: …"), so we
// return we.Error() directly to avoid printing the code twice.
func errorText(err error) string {
	var we *agent.WriteError
	if errors.As(err, &we) {
		return we.Error()
	}
	return err.Error()
}

func marshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte(`{"jsonrpc":"2.0","error":{"code":-32603,"message":"internal error"}}`)
	}
	return b
}
