// Package blob is fabriq's byte-plane capability port. It is pure fabriq
// vocabulary: no storage-engine types cross this boundary, so core never
// depends on any object-store library.
package blob

import (
	"context"
	"io"
	"time"
)

// Store is the byte-plane port. Implementations stamp tenant/scope into keys
// structurally; callers pass already-derived keys. Not-found reads return
// fabriqerr.ErrNotFound.
type Store interface {
	Put(ctx context.Context, key string, r io.Reader, o PutOpts) (ObjectInfo, error)
	Get(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error)
	Head(ctx context.Context, key string) (ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	Copy(ctx context.Context, srcKey, dstKey string) (ObjectInfo, error)
	// Capabilities reports which optional sub-interfaces this Store satisfies.
	Capabilities() Caps
}

// PutOpts carries optional metadata for a write. Size is -1 when unknown.
type PutOpts struct {
	ContentType string `json:"contentType"`
	Size        int64  `json:"size"`
}

// ObjectInfo is the stored-object metadata returned by the port.
type ObjectInfo struct {
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	Checksum    string    `json:"checksum"`
	ContentType string    `json:"contentType"`
	ModifiedAt  time.Time `json:"modifiedAt"`
}

// Caps reports optional capabilities a Store supports (reflecting the
// underlying driver). Absent capabilities are reported, never faked.
type Caps struct {
	Presign   bool `json:"presign"`
	Multipart bool `json:"multipart"`
	Range     bool `json:"range"`
}

// PartInfo identifies an uploaded multipart part.
type PartInfo struct {
	Part int    `json:"part"`
	ETag string `json:"etag"`
}

// Presigner issues client-direct URLs. A Store supports it iff Caps.Presign.
type Presigner interface {
	PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error)
	PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error)
}

// Multipart supports resumable multipart uploads. Supported iff Caps.Multipart.
type Multipart interface {
	InitiateMultipart(ctx context.Context, key string, o PutOpts) (uploadID string, err error)
	UploadPart(ctx context.Context, key, uploadID string, part int, r io.Reader) (PartInfo, error)
	CompleteMultipart(ctx context.Context, key, uploadID string, parts []PartInfo) (ObjectInfo, error)
	AbortMultipart(ctx context.Context, key, uploadID string) error
}

// Ranger supports byte-range reads. Supported iff Caps.Range.
type Ranger interface {
	GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}
