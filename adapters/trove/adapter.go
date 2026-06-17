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

	"github.com/xraph/trove"
	trovedriver "github.com/xraph/trove/driver"

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

// Capabilities reports what the underlying driver supports (Task 5 fills these
// by type-asserting the driver; the zero value here means "core only").
func (a *Adapter) Capabilities() blob.Caps { return a.caps() }

// caps is the Task-5 stub — returns zero Caps until the capability
// introspection is wired in the next task.
func (a *Adapter) caps() blob.Caps { return blob.Caps{} }

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
