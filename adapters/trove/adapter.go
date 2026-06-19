// Package trovestore implements fabriq's core/blob.Store over the Trove byte
// engine used as a LIBRARY (trove.Open + the driver registry). It imports only
// the trove library packages — never trove/extension or its metadata store — so
// Trove holds no catalog and can never be a source of truth.
package trovestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/xraph/trove"
	trovedriver "github.com/xraph/trove/driver"
	_ "github.com/xraph/trove/drivers/localdriver" // register file:// and local:// schemes
	_ "github.com/xraph/trove/drivers/memdriver"   // register mem:// scheme

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// Adapter is a blob.Store backed by a single Trove bucket.
type Adapter struct {
	t      *trove.Trove
	bucket string
}

var _ blob.Store = (*Adapter)(nil)

// New wraps an already-opened trove.Trove. The caller owns the Trove lifecycle
// (it was built from a driver + options in the forgeext provider).
func New(t *trove.Trove, bucket string) *Adapter {
	return &Adapter{t: t, bucket: bucket}
}

// Put stores an object and returns its metadata.
func (a *Adapter) Put(ctx context.Context, key string, r io.Reader, o blob.PutOpts) (blob.ObjectInfo, error) {
	var opts []trovedriver.PutOption
	if o.ContentType != "" {
		opts = append(opts, trovedriver.WithContentType(o.ContentType))
	}
	info, err := a.t.Put(ctx, a.bucket, key, r, opts...)
	if err != nil {
		return blob.ObjectInfo{}, mapErr(err)
	}
	return toInfo(info), nil
}

// Get retrieves an object body and its metadata.
func (a *Adapter) Get(ctx context.Context, key string) (io.ReadCloser, blob.ObjectInfo, error) {
	obj, err := a.t.Get(ctx, a.bucket, key)
	if err != nil {
		return nil, blob.ObjectInfo{}, mapErr(err)
	}
	return obj.ReadCloser, toInfo(obj.Info), nil
}

// Head returns object metadata without content.
func (a *Adapter) Head(ctx context.Context, key string) (blob.ObjectInfo, error) {
	info, err := a.t.Head(ctx, a.bucket, key)
	if err != nil {
		return blob.ObjectInfo{}, mapErr(err)
	}
	return toInfo(info), nil
}

// Delete removes an object.
func (a *Adapter) Delete(ctx context.Context, key string) error {
	return mapErr(a.t.Delete(ctx, a.bucket, key))
}

// List returns all objects whose keys share the given prefix.
func (a *Adapter) List(ctx context.Context, prefix string) ([]blob.ObjectInfo, error) {
	it, err := a.t.List(ctx, a.bucket, trovedriver.WithPrefix(prefix))
	if err != nil {
		return nil, mapErr(err)
	}
	// ObjectIterator.Next requires a context (differs from brief's no-arg call).
	var out []blob.ObjectInfo
	for {
		info, ierr := it.Next(ctx)
		if errors.Is(ierr, io.EOF) {
			break
		}
		if ierr != nil {
			return nil, mapErr(ierr)
		}
		out = append(out, toInfo(info))
	}
	return out, nil
}

// Copy copies an object within the same bucket.
func (a *Adapter) Copy(ctx context.Context, srcKey, dstKey string) (blob.ObjectInfo, error) {
	info, err := a.t.Copy(ctx, a.bucket, srcKey, a.bucket, dstKey)
	if err != nil {
		return blob.ObjectInfo{}, mapErr(err)
	}
	return toInfo(info), nil
}

// ErrUnsupported is returned by a capability method when the underlying driver
// does not provide that capability (Capabilities() reports it false).
var ErrUnsupported = errors.New("fabriq: trove blob: capability not supported by driver")

// Compile-time assertions: Adapter structurally satisfies the optional interfaces.
var (
	_ blob.Presigner = (*Adapter)(nil)
	_ blob.Multipart = (*Adapter)(nil)
	_ blob.Ranger    = (*Adapter)(nil)
)

// Capabilities reports what the underlying driver supports by type-asserting
// against the Trove driver capability interfaces.
func (a *Adapter) Capabilities() blob.Caps { return a.caps() }

// caps type-asserts the driver to detect which optional capabilities it provides.
func (a *Adapter) caps() blob.Caps {
	d := a.t.Driver()
	_, presign := d.(trovedriver.PresignDriver)
	_, multipart := d.(trovedriver.MultipartDriver)
	_, rng := d.(trovedriver.RangeDriver)
	return blob.Caps{Presign: presign, Multipart: multipart, Range: rng}
}

// PresignGet returns a pre-signed GET URL for the given key. Returns
// ErrUnsupported when the underlying driver does not implement PresignDriver.
func (a *Adapter) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	d, ok := a.t.Driver().(trovedriver.PresignDriver)
	if !ok {
		return "", ErrUnsupported
	}
	url, err := d.PresignGet(ctx, a.bucket, key, ttl)
	return url, mapErr(err)
}

// PresignPut returns a pre-signed PUT URL for the given key. Returns
// ErrUnsupported when the underlying driver does not implement PresignDriver.
func (a *Adapter) PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error) {
	d, ok := a.t.Driver().(trovedriver.PresignDriver)
	if !ok {
		return "", ErrUnsupported
	}
	url, err := d.PresignPut(ctx, a.bucket, key, ttl)
	return url, mapErr(err)
}

// InitiateMultipart starts a multipart upload and returns the upload ID.
// Returns ErrUnsupported when the underlying driver does not implement MultipartDriver.
func (a *Adapter) InitiateMultipart(ctx context.Context, key string, o blob.PutOpts) (string, error) {
	d, ok := a.t.Driver().(trovedriver.MultipartDriver)
	if !ok {
		return "", ErrUnsupported
	}
	var opts []trovedriver.PutOption
	if o.ContentType != "" {
		opts = append(opts, trovedriver.WithContentType(o.ContentType))
	}
	id, err := d.InitiateMultipart(ctx, a.bucket, key, opts...)
	return id, mapErr(err)
}

// UploadPart uploads a single part of a multipart upload. Returns
// ErrUnsupported when the underlying driver does not implement MultipartDriver.
func (a *Adapter) UploadPart(ctx context.Context, key, uploadID string, part int, r io.Reader) (blob.PartInfo, error) {
	d, ok := a.t.Driver().(trovedriver.MultipartDriver)
	if !ok {
		return blob.PartInfo{}, ErrUnsupported
	}
	p, err := d.UploadPart(ctx, a.bucket, key, uploadID, part, r)
	if err != nil {
		return blob.PartInfo{}, mapErr(err)
	}
	// driver.PartInfo fields: PartNumber int, ETag string, Size int64
	return blob.PartInfo{Part: p.PartNumber, ETag: p.ETag}, nil
}

// CompleteMultipart finalises a multipart upload. Returns ErrUnsupported when
// the underlying driver does not implement MultipartDriver.
func (a *Adapter) CompleteMultipart(ctx context.Context, key, uploadID string, parts []blob.PartInfo) (blob.ObjectInfo, error) {
	d, ok := a.t.Driver().(trovedriver.MultipartDriver)
	if !ok {
		return blob.ObjectInfo{}, ErrUnsupported
	}
	dparts := make([]trovedriver.PartInfo, len(parts))
	for i, p := range parts {
		dparts[i] = trovedriver.PartInfo{PartNumber: p.Part, ETag: p.ETag}
	}
	info, err := d.CompleteMultipart(ctx, a.bucket, key, uploadID, dparts)
	if err != nil {
		return blob.ObjectInfo{}, mapErr(err)
	}
	return toInfo(info), nil
}

// AbortMultipart cancels an in-progress multipart upload. Returns ErrUnsupported
// when the underlying driver does not implement MultipartDriver.
func (a *Adapter) AbortMultipart(ctx context.Context, key, uploadID string) error {
	d, ok := a.t.Driver().(trovedriver.MultipartDriver)
	if !ok {
		return ErrUnsupported
	}
	return mapErr(d.AbortMultipart(ctx, a.bucket, key, uploadID))
}

// GetRange retrieves a byte range of an object. Returns ErrUnsupported when
// the underlying driver does not implement RangeDriver.
func (a *Adapter) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	d, ok := a.t.Driver().(trovedriver.RangeDriver)
	if !ok {
		return nil, ErrUnsupported
	}
	obj, err := d.GetRange(ctx, a.bucket, key, offset, length)
	if err != nil {
		return nil, mapErr(err)
	}
	// ObjectReader embeds io.ReadCloser directly.
	return obj, nil
}

// toInfo converts a Trove driver ObjectInfo to the blob port type.
func toInfo(i *trovedriver.ObjectInfo) blob.ObjectInfo {
	if i == nil {
		return blob.ObjectInfo{}
	}
	return blob.ObjectInfo{
		Key:         i.Key,
		Size:        i.Size,
		Checksum:    i.ETag,
		ContentType: i.ContentType,
		ModifiedAt:  i.LastModified,
	}
}

// Config describes how to build a Trove-backed blob.Store from a DSN.
type Config struct {
	// StorageDriver is the backend DSN, e.g. "file:///data/blobs" or "mem://".
	StorageDriver string `json:"storageDriver" yaml:"storageDriver"`
	// DefaultBucket is the bucket all keys live under (created on Open).
	DefaultBucket string `json:"defaultBucket" yaml:"defaultBucket"`
}

// Open builds a Trove instance from the config's DSN (via the driver registry),
// ensures the default bucket exists, and returns an Adapter. The Adapter owns
// the Trove handle; call Close to release it.
func Open(ctx context.Context, cfg Config) (*Adapter, error) {
	if cfg.StorageDriver == "" {
		return nil, fmt.Errorf("fabriq: trove storage: storageDriver (DSN) is required")
	}
	bucket := cfg.DefaultBucket
	if bucket == "" {
		bucket = "default"
	}

	dsn, err := trovedriver.ParseDSN(cfg.StorageDriver)
	if err != nil {
		return nil, fmt.Errorf("fabriq: trove storage: parse DSN: %w", err)
	}
	factory, ok := trovedriver.Lookup(dsn.Scheme)
	if !ok {
		return nil, fmt.Errorf("fabriq: trove storage: unknown driver %q (registered: %v)", dsn.Scheme, trovedriver.Drivers())
	}
	drv := factory()
	err = drv.Open(ctx, cfg.StorageDriver)
	if err != nil {
		return nil, fmt.Errorf("fabriq: trove storage: open driver: %w", err)
	}

	tr, err := trove.Open(drv, trove.WithDefaultBucket(bucket))
	if err != nil {
		return nil, fmt.Errorf("fabriq: trove storage: open trove: %w", err)
	}
	// CreateBucket is idempotent; ignore errors (bucket may already exist).
	_ = tr.CreateBucket(ctx, bucket)

	return New(tr, bucket), nil
}

// Driver returns the underlying trove driver so callers (e.g. Open) can
// construct a CASStore without importing trove/driver directly.
func (a *Adapter) Driver() trovedriver.Driver { return a.t.Driver() }

// Close releases the underlying Trove handle. Safe to call on a nil Adapter.
func (a *Adapter) Close(ctx context.Context) error {
	if a == nil || a.t == nil {
		return nil
	}
	return a.t.Close(ctx)
}

// mapErr normalizes Trove not-found errors into fabriqerr.ErrNotFound.
// Trove's sentinel errors live in the trove package; the mem driver also
// returns plain fmt.Errorf strings like "...object %q not found". The string
// fallback requires BOTH "object" and "not found" so unrelated errors that
// merely contain "not found" (e.g. a network "route not found") are not
// misclassified as a missing object.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	if errors.Is(err, trove.ErrNotFound) ||
		errors.Is(err, trove.ErrObjectNotFound) ||
		(strings.Contains(msg, "object") && strings.Contains(msg, "not found")) {
		return fabriqerr.ErrNotFound
	}
	return fmt.Errorf("fabriq: trove blob: %w", err)
}
