package client

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// FileNode is a single fs_node (file or folder), as returned by the file
// listing/detail endpoints. It mirrors adminapi's fileNode JSON exactly:
// {id, name, kind, size, contentType, parentId, updatedAt}.
type FileNode struct {
	// ID is the fs_node aggregate id.
	ID string `json:"id"`
	// Name is the node's name within its folder.
	Name string `json:"name"`
	// Kind is "folder" or "file".
	Kind string `json:"kind"`
	// Size is the byte size for files; 0 for folders.
	Size int64 `json:"size"`
	// ContentType is the file's MIME type; empty for folders.
	ContentType string `json:"contentType"`
	// ParentID is the parent folder's id; "" for root-level nodes.
	ParentID string `json:"parentId"`
	// UpdatedAt is the last-modified timestamp in RFC 3339, when known.
	UpdatedAt string `json:"updatedAt,omitempty"`
}

// FileListPage is the payload for GET {BasePath}/files. It mirrors
// adminapi's fileListResponse JSON exactly: {items}.
type FileListPage struct {
	Items []FileNode `json:"items"`
}

// ListFilesParams are the query parameters for ListFiles. All are optional:
// Parent absent/empty lists the root; Limit/Offset default server-side.
type ListFilesParams struct {
	// Parent is the folder id whose children to list; "" lists the root.
	Parent string
	// Limit caps the page size (server default 100, capped server-side).
	// Zero omits the query param and defers to the server default.
	Limit int
	// Offset paginates past earlier results. Zero omits the query param.
	Offset int
}

// ListFiles lists a folder's children (root when Parent is empty). It calls
// GET {BasePath}/files?parent=<id>[&limit=<n>][&offset=<n>].
func (c *Client) ListFiles(ctx context.Context, params ListFilesParams) (FileListPage, error) {
	q := url.Values{}
	if params.Parent != "" {
		q.Set("parent", params.Parent)
	}
	if params.Limit > 0 {
		q.Set("limit", strconv.Itoa(params.Limit))
	}
	if params.Offset > 0 {
		q.Set("offset", strconv.Itoa(params.Offset))
	}

	var out FileListPage
	if err := c.do(ctx, http.MethodGet, "/files", q, nil, &out); err != nil {
		return FileListPage{}, err
	}
	return out, nil
}

// GetFile fetches a single file/folder node's metadata by id. It calls
// GET {BasePath}/files/:id.
func (c *Client) GetFile(ctx context.Context, id string) (FileNode, error) {
	var out FileNode
	if err := c.do(ctx, http.MethodGet, "/files/"+url.PathEscape(id), nil, nil, &out); err != nil {
		return FileNode{}, err
	}
	return out, nil
}

// CreateFolderInput is the request body for CreateFolder. It mirrors
// adminapi's createFolderRequest JSON exactly: {parentId, name}.
type CreateFolderInput struct {
	// ParentID is the destination folder id; "" creates at root.
	ParentID string `json:"parentId,omitempty"`
	// Name is the new folder's name (required).
	Name string `json:"name"`
}

// CreateFolder creates a folder. It calls POST {BasePath}/files/folder with
// body {parentId, name}.
func (c *Client) CreateFolder(ctx context.Context, input CreateFolderInput) (FileNode, error) {
	var out FileNode
	if err := c.do(ctx, http.MethodPost, "/files/folder", nil, input, &out); err != nil {
		return FileNode{}, err
	}
	return out, nil
}

// UploadFileInput is the request body for UploadFile. It mirrors adminapi's
// createFileRequest JSON exactly: {parentId, name, contentType, dataBase64}.
// Data is carried as base64-encoded JSON, matching the server contract (not
// a multipart/raw-bytes upload).
type UploadFileInput struct {
	// ParentID is the destination folder id; "" creates at root.
	ParentID string `json:"parentId,omitempty"`
	// Name is the new file's name (required).
	Name string `json:"name"`
	// ContentType is the file's MIME type (optional).
	ContentType string `json:"contentType,omitempty"`
	// Data is the raw file body; UploadFile base64-encodes it into the
	// request's dataBase64 field.
	Data []byte `json:"-"`
}

// uploadFileWireRequest is the actual JSON shape sent over the wire (Data
// base64-encoded into DataBase64), mirroring adminapi's createFileRequest.
type uploadFileWireRequest struct {
	ParentID    string `json:"parentId,omitempty"`
	Name        string `json:"name"`
	ContentType string `json:"contentType,omitempty"`
	DataBase64  string `json:"dataBase64"`
}

// UploadFile uploads a file. It calls POST {BasePath}/files with body
// {parentId, name, contentType, dataBase64} - the file bytes are
// base64-encoded into the JSON body (there is no multipart/raw-bytes upload
// path on the server).
func (c *Client) UploadFile(ctx context.Context, input UploadFileInput) (FileNode, error) {
	wire := uploadFileWireRequest{
		ParentID:    input.ParentID,
		Name:        input.Name,
		ContentType: input.ContentType,
		DataBase64:  base64.StdEncoding.EncodeToString(input.Data),
	}

	var out FileNode
	if err := c.do(ctx, http.MethodPost, "/files", nil, wire, &out); err != nil {
		return FileNode{}, err
	}
	return out, nil
}

// DeleteFile trashes (soft-deletes) a file or folder node and its subtree.
// It calls DELETE {BasePath}/files/:id.
func (c *Client) DeleteFile(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/files/"+url.PathEscape(id), nil, nil, nil)
}

// DownloadFile downloads a file's raw bytes. It calls
// GET {BasePath}/files/:id/content, which - unlike every other endpoint in
// this client - returns the raw file body (not JSON), with the filename
// carried in the Content-Disposition response header. Callers MUST close
// the returned ReadCloser.
//
// On a non-2xx response, the body is read and closed and a *APIError is
// returned instead (mirroring c.do's error handling, since this method
// bypasses c.do entirely to avoid its JSON decoding).
func (c *Client) DownloadFile(ctx context.Context, id string) (io.ReadCloser, string, error) {
	fullURL := c.baseURL + "/files/" + url.PathEscape(id) + "/content"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, http.NoBody)
	if err != nil {
		return nil, "", fmt.Errorf("client: build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.key)
	if c.tenant != "" {
		req.Header.Set("X-Tenant-ID", c.tenant)
	}
	req.Header.Set("X-Fabriq-Api-Version", strconv.Itoa(c.version))

	hc := c.hc
	if hc == nil {
		hc = http.DefaultClient
	}

	resp, err := hc.Do(req) // #nosec G704 -- URL is built from the configured base URL plus a path-escaped id, not attacker-controlled.
	if err != nil {
		return nil, "", fmt.Errorf("client: request failed: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, "", fmt.Errorf("client: read response body: %w", readErr)
		}
		return nil, "", parseAPIError(resp.StatusCode, respBody)
	}

	filename := parseContentDispositionFilename(resp.Header.Get("Content-Disposition"))
	if filename == "" {
		filename = id
	}

	return resp.Body, filename, nil
}

// parseContentDispositionFilename extracts a filename from a
// Content-Disposition header value, supporting both the RFC 5987
// filename*= form and the plain quoted/unquoted filename= form. It mirrors
// the fabriq-admin TS client's parseContentDispositionFilename. Returns ""
// when no filename can be found.
func parseContentDispositionFilename(value string) string {
	if value == "" {
		return ""
	}

	// Prefer RFC 5987 filename*=UTF-8''<encoded>.
	if idx := strings.Index(value, "filename*="); idx != -1 {
		rest := value[idx+len("filename*="):]
		if semi := strings.IndexByte(rest, ';'); semi != -1 {
			rest = rest[:semi]
		}
		rest = strings.TrimSpace(rest)
		rest = strings.TrimPrefix(rest, "UTF-8''")
		rest = strings.TrimPrefix(rest, "utf-8''")
		if decoded, err := url.QueryUnescape(rest); err == nil && decoded != "" {
			return decoded
		}
	}

	// Fall back to the plain filename= form (quoted or unquoted).
	if idx := strings.Index(value, "filename="); idx != -1 {
		rest := value[idx+len("filename="):]
		if semi := strings.IndexByte(rest, ';'); semi != -1 {
			rest = rest[:semi]
		}
		rest = strings.TrimSpace(rest)
		rest = strings.Trim(rest, `"`)
		if rest != "" {
			return rest
		}
	}

	// Last resort: let mime.ParseMediaType have a shot (handles some edge
	// cases the manual parse above may miss).
	if _, params, err := mime.ParseMediaType(value); err == nil {
		if fn, ok := params["filename"]; ok && fn != "" {
			return fn
		}
	}

	return ""
}
