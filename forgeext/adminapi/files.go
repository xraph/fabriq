package adminapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/domain"
)

// defaultFileLimit is the default page size for the folder-children listing.
const defaultFileLimit = 100

// rootParentID is the conventional parent id of root-level nodes. The fs_node
// tree addresses the root with the empty parent id (see fabriq.CreateFolder /
// fabriq.ListChildren: parentID == "" is root).
const rootParentID = ""

// fileNode is the admin-facing JSON projection of a domain.FsNode. It is a
// stable, camelCase shape the SPA file browser renders: the tree-structural
// fields plus the file facets (size/contentType). The blob reference (blobId)
// is intentionally omitted — the dashboard never addresses bytes directly; it
// downloads via GET {base}/files/:id/content.
type fileNode struct {
	// ID is the fs_node aggregate id.
	ID string `json:"id"`
	// Name is the node's name within its folder.
	Name string `json:"name"`
	// Kind is "folder" or "file" (mapped from domain.FsNode.NodeType).
	Kind string `json:"kind"`
	// Size is the byte size for files; 0 for folders.
	Size int64 `json:"size"`
	// ContentType is the file's MIME type; empty for folders.
	ContentType string `json:"contentType"`
	// ParentID is the parent folder's id; "" for root-level nodes.
	ParentID string `json:"parentId"`
	// UpdatedAt is the last-modified timestamp in RFC 3339.
	UpdatedAt string `json:"updatedAt,omitempty"`
}

// fileListResponse is the payload for GET {BasePath}/files.
type fileListResponse struct {
	Items []fileNode `json:"items"`
}

// createFolderRequest is the body for POST {BasePath}/files/folder.
type createFolderRequest struct {
	// ParentID is the destination folder id; "" (or absent) creates at root.
	ParentID string `json:"parentId"`
	// Name is the new folder's name (required).
	Name string `json:"name"`
}

// createFileRequest is the body for POST {BasePath}/files.
type createFileRequest struct {
	// ParentID is the destination folder id; "" (or absent) creates at root.
	ParentID string `json:"parentId"`
	// Name is the new file's name (required).
	Name string `json:"name"`
	// ContentType is the file's MIME type (optional).
	ContentType string `json:"contentType"`
	// DataBase64 is the file body, base64-encoded (required).
	DataBase64 string `json:"dataBase64"`
}

// toFileNode maps a domain.FsNode to its admin JSON projection. NodeType maps
// to kind 1:1 ("folder" | "file"); folders carry zero size and empty content
// type by construction.
func toFileNode(n domain.FsNode) fileNode {
	out := fileNode{
		ID:          n.ID,
		Name:        n.Name,
		Kind:        n.NodeType,
		Size:        n.Size,
		ContentType: n.ContentType,
		ParentID:    n.ParentID,
	}
	if !n.UpdatedAt.IsZero() {
		out.UpdatedAt = n.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	return out
}

// registerFileRoutes wires the file-plane (fs_node tree + blob byte plane)
// routes onto the given router. They share the same route options
// (auth/tenant middleware) as the rest of the admin surface so the host
// controls the security boundary uniformly.
//
// Routes:
//
//	GET    {base}/files?parent=&limit=&offset=  list a folder's children (root when absent)
//	POST   {base}/files/folder                  create a folder
//	POST   {base}/files                         upload a file (base64 body)
//	GET    {base}/files/:id                     node metadata
//	GET    {base}/files/:id/content             stream the file bytes
//	DELETE {base}/files/:id                     trash (soft-delete) a node + subtree
func (c *adminController) registerFileRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	listOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.files.list"),
		forge.WithSummary("List a folder's children (root when ?parent= is absent)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/files", c.handleListFiles, listOpts...); err != nil {
		return err
	}

	// Register the static /files/folder POST before the dynamic /files/:id
	// routes so it is not captured as an :id by registration-order routers.
	folderOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.files.folder.create"),
		forge.WithSummary("Create a folder (body: {parentId?, name})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.POST(base+"/files/folder", c.handleCreateFolder, folderOpts...); err != nil {
		return err
	}

	uploadOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.files.upload"),
		forge.WithSummary("Upload a file (body: {parentId?, name, contentType?, dataBase64})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.POST(base+"/files", c.handleUploadFile, uploadOpts...); err != nil {
		return err
	}

	contentOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.files.content"),
		forge.WithSummary("Download a file's bytes"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/files/:id/content", c.handleFileContent, contentOpts...); err != nil {
		return err
	}

	nodeOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.files.get"),
		forge.WithSummary("Get a file/folder node by id"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/files/:id", c.handleGetFile, nodeOpts...); err != nil {
		return err
	}

	deleteOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.files.delete"),
		forge.WithSummary("Trash (soft-delete) a node and its subtree"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.DELETE(base+"/files/:id", c.handleDeleteFile, deleteOpts...)
}

// handleListFiles serves GET {BasePath}/files.
//
// Optional query params:
//
//	parent  folder id whose children to list; absent/empty = root
//	limit   page size (default 100, capped at maxLimit)
//	offset  zero-based row offset
//
// Returns 501 when the instance has no blob/files backend configured.
func (c *adminController) handleListFiles(ctx forge.Context) error {
	fab, err := c.ext.resolveFabriq()
	if err != nil {
		return forge.InternalError(err)
	}
	if !c.filesConfigured(fab) {
		return c.filesNotConfigured(ctx)
	}

	parent := ctx.Query("parent") // "" = root by convention
	limit := defaultFileLimit
	if lStr := ctx.Query("limit"); lStr != "" {
		l, parseErr := strconv.Atoi(lStr)
		if parseErr != nil || l < 1 {
			return forge.BadRequest("query param 'limit' must be a positive integer")
		}
		if l > maxLimit {
			l = maxLimit
		}
		limit = l
	}
	offset := 0
	if oStr := ctx.Query("offset"); oStr != "" {
		o, parseErr := strconv.Atoi(oStr)
		if parseErr != nil || o < 0 {
			return forge.BadRequest("query param 'offset' must be a non-negative integer")
		}
		offset = o
	}

	reqCtx := ctx.Request().Context()
	nodes, listErr := fab.ListChildren(reqCtx, parent, limit, offset)
	if listErr != nil {
		return mapFileError(ctx, c, listErr)
	}
	items := make([]fileNode, 0, len(nodes))
	for _, n := range nodes {
		items = append(items, toFileNode(n))
	}
	return ctx.JSON(http.StatusOK, fileListResponse{Items: items})
}

// handleCreateFolder serves POST {BasePath}/files/folder.
//
// Body: {parentId?, name}. Returns 201 with the new folder node, 400 when name
// is missing, and 501 when the blob/files backend is unconfigured.
func (c *adminController) handleCreateFolder(ctx forge.Context) error {
	fab, err := c.ext.resolveFabriq()
	if err != nil {
		return forge.InternalError(err)
	}

	var req createFolderRequest
	if decErr := ctx.BindJSON(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if req.Name == "" {
		return forge.BadRequest("field 'name' is required")
	}
	if !c.filesConfigured(fab) {
		return c.filesNotConfigured(ctx)
	}

	parent := req.ParentID
	if parent == "" {
		parent = rootParentID
	}
	reqCtx := ctx.Request().Context()
	ref, createErr := fab.CreateFolder(reqCtx, parent, req.Name)
	if createErr != nil {
		return mapFileError(ctx, c, createErr)
	}

	node, getErr := fab.GetNode(reqCtx, ref.ID)
	if getErr != nil {
		return mapFileError(ctx, c, getErr)
	}
	return ctx.JSON(http.StatusCreated, toFileNode(node))
}

// handleUploadFile serves POST {BasePath}/files.
//
// Body: {parentId?, name, contentType?, dataBase64}. Returns 201 with the new
// file node, 400 when name or dataBase64 is missing/invalid, and 501 when the
// blob/files backend is unconfigured.
func (c *adminController) handleUploadFile(ctx forge.Context) error {
	fab, err := c.ext.resolveFabriq()
	if err != nil {
		return forge.InternalError(err)
	}

	var req createFileRequest
	if decErr := ctx.BindJSON(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if req.Name == "" {
		return forge.BadRequest("field 'name' is required")
	}
	if req.DataBase64 == "" {
		return forge.BadRequest("field 'dataBase64' is required")
	}
	data, decErr := base64.StdEncoding.DecodeString(req.DataBase64)
	if decErr != nil {
		return forge.BadRequest("field 'dataBase64' is not valid base64: " + decErr.Error())
	}
	if !c.filesConfigured(fab) {
		return c.filesNotConfigured(ctx)
	}

	parent := req.ParentID
	if parent == "" {
		parent = rootParentID
	}
	reqCtx := ctx.Request().Context()
	ref, createErr := fab.CreateFile(reqCtx, parent, req.Name, bytes.NewReader(data), fabriq.CreateFileOpts{ContentType: req.ContentType})
	if createErr != nil {
		return mapFileError(ctx, c, createErr)
	}

	node, getErr := fab.GetNode(reqCtx, ref.ID)
	if getErr != nil {
		return mapFileError(ctx, c, getErr)
	}
	return ctx.JSON(http.StatusCreated, toFileNode(node))
}

// handleGetFile serves GET {BasePath}/files/:id.
//
// Returns the node metadata, 404 when the node is absent, and 501 when the
// blob/files backend is unconfigured.
func (c *adminController) handleGetFile(ctx forge.Context) error {
	fab, err := c.ext.resolveFabriq()
	if err != nil {
		return forge.InternalError(err)
	}
	if !c.filesConfigured(fab) {
		return c.filesNotConfigured(ctx)
	}
	id := ctx.Param("id")
	if id == "" {
		return forge.BadRequest("path param 'id' is required")
	}
	node, getErr := fab.GetNode(ctx.Request().Context(), id)
	if getErr != nil {
		return mapFileError(ctx, c, getErr)
	}
	// GetNode loads any state, including trashed nodes; the admin surface only
	// exposes live nodes (the browser lists/restores via the live tree), so a
	// trashed node reads as not-found.
	if node.DeletedAt != nil {
		return forge.NotFound("node not found")
	}
	return ctx.JSON(http.StatusOK, toFileNode(node))
}

// handleFileContent serves GET {BasePath}/files/:id/content.
//
// It resolves the node, opens its blob bytes, and streams them with a
// Content-Type and an attachment Content-Disposition. Returns 404 when the
// node or its blob is absent, 400 when the node is a folder, and 501 when the
// blob/files backend (or the CAS byte plane) is unconfigured.
func (c *adminController) handleFileContent(ctx forge.Context) error {
	fab, err := c.ext.resolveFabriq()
	if err != nil {
		return forge.InternalError(err)
	}
	if !c.filesConfigured(fab) {
		return c.filesNotConfigured(ctx)
	}
	id := ctx.Param("id")
	if id == "" {
		return forge.BadRequest("path param 'id' is required")
	}

	reqCtx := ctx.Request().Context()
	node, getErr := fab.GetNode(reqCtx, id)
	if getErr != nil {
		return mapFileError(ctx, c, getErr)
	}
	// A trashed node is not downloadable (see handleGetFile): GetNode returns
	// any state, but the admin surface serves only live nodes.
	if node.DeletedAt != nil {
		return forge.NotFound("node not found")
	}
	if node.NodeType != "file" {
		return forge.BadRequest("node is not a file")
	}
	if node.BlobID == "" {
		return forge.NotFound("file has no blob")
	}

	rc, _, blobErr := fab.GetBlob(reqCtx, node.BlobID)
	if blobErr != nil {
		if errors.Is(blobErr, fabriqerr.ErrStoreNotConfigured) {
			return c.filesNotConfigured(ctx)
		}
		if errors.Is(blobErr, fabriqerr.ErrNotFound) {
			return forge.NotFound("blob not found")
		}
		return forge.InternalError(blobErr)
	}
	defer func() { _ = rc.Close() }()

	contentType := node.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w := ctx.Response()
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", `attachment; filename="`+sanitizeFilename(node.Name)+`"`)
	if node.Size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(node.Size, 10))
	}
	w.WriteHeader(http.StatusOK)
	if _, copyErr := io.Copy(w, rc); copyErr != nil {
		// Headers/body already (partially) written; the connection is the only
		// signal left. Return the error for the host's logging middleware.
		return copyErr
	}
	return nil
}

// handleDeleteFile serves DELETE {BasePath}/files/:id.
//
// It soft-deletes (trashes) the node and its whole subtree. Returns 204 on
// success, 404 when the node is absent, and 501 when the blob/files backend is
// unconfigured.
func (c *adminController) handleDeleteFile(ctx forge.Context) error {
	fab, err := c.ext.resolveFabriq()
	if err != nil {
		return forge.InternalError(err)
	}
	if !c.filesConfigured(fab) {
		return c.filesNotConfigured(ctx)
	}
	id := ctx.Param("id")
	if id == "" {
		return forge.BadRequest("path param 'id' is required")
	}

	reqCtx := ctx.Request().Context()
	// TrashNode loads the node first (GetNode), so a missing id surfaces as
	// ErrNotFound → 404 rather than a silent no-op.
	if trashErr := fab.TrashNode(reqCtx, id); trashErr != nil {
		return mapFileError(ctx, c, trashErr)
	}
	return ctx.NoContent(http.StatusNoContent)
}

// filesConfigured reports whether the instance has a blob/files backend wired.
// It reuses the same side-effect-free Head probe the capabilities endpoint
// uses (blobConfigured) so the file routes and the capability flag agree.
func (c *adminController) filesConfigured(fab *fabriq.Fabriq) bool {
	return blobConfigured(context.Background(), fab.Blob())
}

// filesNotConfigured returns the 501 response used when the instance has no
// blob/files backend wired. It mirrors the not-configured shape used across
// the admin surface so the SPA can branch on a stable error payload.
func (c *adminController) filesNotConfigured(ctx forge.Context) error {
	return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "files not configured"})
}

// mapFileError translates fs_node / blob domain errors to forge HTTP errors.
// A not-configured store (e.g. CAS-less CreateFile) maps to 501 to match the
// instance capability; not-found maps to 404; name conflicts and
// non-container parents map to 400; everything else is a 500.
func mapFileError(ctx forge.Context, c *adminController, err error) error {
	switch {
	case errors.Is(err, fabriqerr.ErrStoreNotConfigured):
		return c.filesNotConfigured(ctx)
	case errors.Is(err, fabriqerr.ErrNotFound):
		return forge.NotFound("node not found")
	case errors.Is(err, fabriq.ErrNodeNameConflict):
		return forge.BadRequest(err.Error())
	case errors.Is(err, fabriq.ErrNotContainer):
		return forge.BadRequest(err.Error())
	case errors.Is(err, fabriq.ErrNodeLocked):
		return forge.BadRequest(err.Error())
	default:
		return forge.InternalError(err)
	}
}

// sanitizeFilename strips characters that would break a Content-Disposition
// filename token (quotes, CR, LF) so a node name cannot inject header content.
func sanitizeFilename(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch r {
		case '"', '\r', '\n':
			out = append(out, '_')
		default:
			out = append(out, r)
		}
	}
	return string(out)
}
