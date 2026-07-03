package fabriq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/crypto"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

// SourceInput carries a blob_source's fields with PLAINTEXT auth (encrypted at
// the boundary).
type SourceInput struct {
	ProjectID   string            `json:"projectId"`
	Name        string            `json:"name"`
	Provider    string            `json:"provider"`
	Endpoint    string            `json:"endpoint"`
	BasePath    string            `json:"basePath"`
	Auth        map[string]any    `json:"auth"`
	WatchConfig map[string]any    `json:"watchConfig"`
	FileFilter  map[string]any    `json:"fileFilter"`
	Tags        map[string]string `json:"tags"`
	Enabled     bool              `json:"enabled"`
}

// BlobSourceView is a decrypted read of a blob_source.
type BlobSourceView struct {
	ID          string            `json:"id"`
	ProjectID   string            `json:"projectId"`
	Name        string            `json:"name"`
	Provider    string            `json:"provider"`
	Endpoint    string            `json:"endpoint"`
	BasePath    string            `json:"basePath"`
	Auth        map[string]any    `json:"auth"`
	WatchConfig map[string]any    `json:"watchConfig"`
	FileFilter  map[string]any    `json:"fileFilter"`
	Tags        map[string]string `json:"tags"`
	Enabled     bool              `json:"enabled"`
	Version     int64             `json:"version"`
}

// SourceRef identifies a created/updated blob_source.
type SourceRef struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

// encryptAuth marshals + encrypts the auth map, binding the tenant as AAD.
// Returns (nil, nil) when auth is empty; fails closed when a key is absent.
func (f *Fabriq) encryptAuth(ctx context.Context, auth map[string]any) ([]byte, error) {
	if len(auth) == 0 {
		return nil, nil
	}
	if f.crypto == nil {
		return nil, crypto.ErrNotConfigured
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	plaintext, err := json.Marshal(auth)
	if err != nil {
		return nil, err
	}
	return f.crypto.Encrypt(plaintext, []byte(tid))
}

func (f *Fabriq) decryptAuth(ctx context.Context, enc []byte) (map[string]any, error) {
	if len(enc) == 0 {
		return nil, nil
	}
	if f.crypto == nil {
		return nil, crypto.ErrNotConfigured
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	plaintext, err := f.crypto.Decrypt(enc, []byte(tid))
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(plaintext, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (f *Fabriq) sourceModel(ctx context.Context, in SourceInput) (*domain.BlobSource, error) {
	authEnc, err := f.encryptAuth(ctx, in.Auth)
	if err != nil {
		return nil, err
	}
	// Normalize nil maps to empty maps so grove serializes {} instead of NULL
	// for JSONB NOT NULL columns.
	watchConfig := in.WatchConfig
	if watchConfig == nil {
		watchConfig = map[string]any{}
	}
	fileFilter := in.FileFilter
	if fileFilter == nil {
		fileFilter = map[string]any{}
	}
	tags := in.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	return &domain.BlobSource{
		ProjectID: in.ProjectID, Name: in.Name, Provider: in.Provider, Endpoint: in.Endpoint,
		BasePath: in.BasePath, AuthEnc: authEnc, WatchConfig: watchConfig, FileFilter: fileFilter,
		Tags: tags, Enabled: in.Enabled,
	}, nil
}

// CreateSource persists a new blob_source with encrypted credentials.
func (f *Fabriq) CreateSource(ctx context.Context, in SourceInput) (SourceRef, error) {
	m, err := f.sourceModel(ctx, in)
	if err != nil {
		return SourceRef{}, fmt.Errorf("fabriq: CreateSource: %w", err)
	}
	res, err := f.exec.Exec(ctx, command.Command{Entity: "blob_source", Op: command.OpCreate, Payload: m})
	if err != nil {
		return SourceRef{}, fmt.Errorf("fabriq: CreateSource: %w", err)
	}
	return SourceRef{ID: res.AggID, Version: res.Version}, nil
}

// GetSource reads and decrypts a blob_source.
func (f *Fabriq) GetSource(ctx context.Context, id string) (BlobSourceView, error) {
	var s domain.BlobSource
	if err := f.Relational().Get(ctx, "blob_source", id, &s); err != nil {
		return BlobSourceView{}, fmt.Errorf("fabriq: GetSource: %w", err)
	}
	auth, err := f.decryptAuth(ctx, s.AuthEnc)
	if err != nil {
		return BlobSourceView{}, fmt.Errorf("fabriq: GetSource: decrypt: %w", err)
	}
	return toSourceView(s, auth), nil
}

// ListSources reads and decrypts all of the tenant's fabriq_blob_sources.
func (f *Fabriq) ListSources(ctx context.Context) ([]BlobSourceView, error) {
	var rows []domain.BlobSource
	if err := f.Relational().List(ctx, "blob_source", query.ListQuery{OrderBy: "name ASC"}, &rows); err != nil {
		return nil, fmt.Errorf("fabriq: ListSources: %w", err)
	}
	out := make([]BlobSourceView, 0, len(rows))
	for i := range rows {
		auth, err := f.decryptAuth(ctx, rows[i].AuthEnc)
		if err != nil {
			return nil, fmt.Errorf("fabriq: ListSources: decrypt %s: %w", rows[i].ID, err)
		}
		out = append(out, toSourceView(rows[i], auth))
	}
	return out, nil
}

// UpdateSource replaces a blob_source (re-encrypting auth), bumping version.
func (f *Fabriq) UpdateSource(ctx context.Context, id string, in SourceInput) (SourceRef, error) {
	m, err := f.sourceModel(ctx, in)
	if err != nil {
		return SourceRef{}, fmt.Errorf("fabriq: UpdateSource: %w", err)
	}
	res, err := f.exec.Exec(ctx, command.Command{Entity: "blob_source", Op: command.OpUpdate, AggID: id, Payload: m})
	if err != nil {
		return SourceRef{}, fmt.Errorf("fabriq: UpdateSource: %w", err)
	}
	return SourceRef{ID: id, Version: res.Version}, nil
}

// DeleteSource removes a blob_source.
func (f *Fabriq) DeleteSource(ctx context.Context, id string) error {
	if _, err := f.exec.Exec(ctx, command.Command{Entity: "blob_source", Op: command.OpDelete, AggID: id}); err != nil {
		return fmt.Errorf("fabriq: DeleteSource: %w", err)
	}
	return nil
}

func toSourceView(s domain.BlobSource, auth map[string]any) BlobSourceView {
	return BlobSourceView{
		ID: s.ID, ProjectID: s.ProjectID, Name: s.Name, Provider: s.Provider, Endpoint: s.Endpoint,
		BasePath: s.BasePath, Auth: auth, WatchConfig: s.WatchConfig, FileFilter: s.FileFilter,
		Tags: s.Tags, Enabled: s.Enabled, Version: s.Version,
	}
}
