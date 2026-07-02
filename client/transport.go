package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// APIError represents a non-2xx response from the fabriq admin API. Status
// is always authoritative (the real HTTP status code); Code and Message are
// a best-effort parse of the response body, which the adminapi emits in
// several different shapes depending on which layer produced the error.
type APIError struct {
	Status  int
	Code    string
	Message string
	Body    string
}

// Error implements the error interface.
func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("fabriq: %d %s: %s", e.Status, httpStatusOrCode(e), e.Message)
	}
	if e.Body != "" {
		return fmt.Sprintf("fabriq: %d %s: %s", e.Status, httpStatusOrCode(e), e.Body)
	}
	return fmt.Sprintf("fabriq: %d %s", e.Status, httpStatusOrCode(e))
}

func httpStatusOrCode(e *APIError) string {
	if e.Code != "" {
		return e.Code
	}
	return http.StatusText(e.Status)
}

// do issues an HTTP request against the fabriq admin API and decodes the
// response. method/path/query/body describe the request; on a 2xx response
// the body is JSON-decoded into out (skipped if out is nil). On a non-2xx
// response, do returns a *APIError describing the failure - it never fails
// the call just because the error body could not be parsed.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	fullURL := c.baseURL + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	var reqBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("client: encode request body: %w", err)
		}
		reqBody = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
	if err != nil {
		return fmt.Errorf("client: build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.key)
	if c.tenant != "" {
		req.Header.Set("X-Tenant-ID", c.tenant)
	}
	req.Header.Set("X-Fabriq-Api-Version", strconv.Itoa(c.version))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	hc := c.hc
	if hc == nil {
		hc = http.DefaultClient
	}

	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("client: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("client: read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseAPIError(resp.StatusCode, respBody)
	}

	if out == nil || len(respBody) == 0 {
		return nil
	}

	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("client: decode response body: %w", err)
	}

	return nil
}

// parseAPIError best-effort parses a non-2xx response body into an
// *APIError. The adminapi emits several different error JSON shapes
// depending on which layer produced the error:
//
//   - renderError:                {"error":{"code":"...","message":"...","retryable":...}}
//   - forge.InternalError/BadRequest: {"code":<int-or-string>,"details":"...","error":""}
//   - auth deny() middleware:     {"error":"...","code":"<StatusText>"}
//
// Status is always the real HTTP status code, regardless of what (if
// anything) the body contains. Parsing never fails the request - if the
// body doesn't match any known shape, Message/Code are left empty and the
// raw body is preserved in Body for debugging.
func parseAPIError(status int, raw []byte) *APIError {
	apiErr := &APIError{
		Status: status,
		Body:   string(raw),
	}

	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return apiErr
	}

	// Nested "error" object shape: {"error":{"code":"...","message":"..."}}
	if nested, ok := generic["error"].(map[string]any); ok {
		if msg, ok := nested["message"].(string); ok && msg != "" {
			apiErr.Message = msg
		}
		apiErr.Code = stringifyCode(nested["code"])
	}

	// Top-level "message" field.
	if apiErr.Message == "" {
		if msg, ok := generic["message"].(string); ok && msg != "" {
			apiErr.Message = msg
		}
	}

	// Top-level "details" field (forge.InternalError/BadRequest shape).
	if apiErr.Message == "" {
		if details, ok := generic["details"].(string); ok && details != "" {
			apiErr.Message = details
		}
	}

	// Top-level "error" as a plain string: {"error":"...","code":"..."}
	if apiErr.Message == "" {
		if errStr, ok := generic["error"].(string); ok && errStr != "" {
			apiErr.Message = errStr
		}
	}

	// Top-level "code" (only if we didn't already get one from the nested
	// error object above).
	if apiErr.Code == "" {
		apiErr.Code = stringifyCode(generic["code"])
	}

	// Fall back to the raw body if nothing usable was found.
	if apiErr.Message == "" {
		apiErr.Message = strings.TrimSpace(apiErr.Body)
	}

	return apiErr
}

// stringifyCode normalizes a "code" field that may be a JSON number or
// string into a string, per the CONTROLLER RECON spec ("Code: nested
// error.code or top-level code (stringified)").
func stringifyCode(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		// json.Unmarshal decodes JSON numbers as float64.
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return ""
	}
}
