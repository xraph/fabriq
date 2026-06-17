package conformance

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// BlobCase is one byte-plane behavior, run identically against every backend
// that wires Env.Blob. Capability-gated cases skip when Requires is unmet.
type BlobCase struct {
	Name     string
	Requires []Capability
	Run      func(t *testing.T, env *Env)
}

// BlobCases is the shared byte-plane case table.
func BlobCases() []BlobCase {
	return []BlobCase{
		{
			Name: "put then get round-trips bytes and metadata",
			Run: func(t *testing.T, env *Env) {
				info, err := env.Blob.Put(env.Ctx, "docs/a.txt", bytes.NewReader([]byte("hello")), blob.PutOpts{ContentType: "text/plain", Size: 5})
				if err != nil {
					t.Fatal(err)
				}
				if info.Size != 5 {
					t.Fatalf("put size = %d, want 5", info.Size)
				}
				rc, gi, err := env.Blob.Get(env.Ctx, "docs/a.txt")
				if err != nil {
					t.Fatal(err)
				}
				got, _ := io.ReadAll(rc)
				_ = rc.Close()
				if string(got) != "hello" {
					t.Fatalf("get body = %q, want hello", got)
				}
				if gi.ContentType != "text/plain" {
					t.Fatalf("get contentType = %q, want text/plain", gi.ContentType)
				}
			},
		},
		{
			Name: "head missing returns ErrNotFound",
			Run: func(t *testing.T, env *Env) {
				if _, err := env.Blob.Head(env.Ctx, "nope/missing"); !errors.Is(err, fabriqerr.ErrNotFound) {
					t.Fatalf("want ErrNotFound, got %v", err)
				}
			},
		},
		{
			Name: "delete removes the object",
			Run: func(t *testing.T, env *Env) {
				if _, err := env.Blob.Put(env.Ctx, "tmp/x", bytes.NewReader([]byte("z")), blob.PutOpts{Size: 1}); err != nil {
					t.Fatal(err)
				}
				if err := env.Blob.Delete(env.Ctx, "tmp/x"); err != nil {
					t.Fatal(err)
				}
				if _, err := env.Blob.Head(env.Ctx, "tmp/x"); !errors.Is(err, fabriqerr.ErrNotFound) {
					t.Fatalf("after delete want ErrNotFound, got %v", err)
				}
			},
		},
		{
			Name: "list returns objects under a prefix",
			Run: func(t *testing.T, env *Env) {
				for _, k := range []string{"p/1", "p/2", "other/3"} {
					if _, err := env.Blob.Put(env.Ctx, k, bytes.NewReader([]byte("v")), blob.PutOpts{Size: 1}); err != nil {
						t.Fatal(err)
					}
				}
				got, err := env.Blob.List(env.Ctx, "p/")
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != 2 || got[0].Key != "p/1" || got[1].Key != "p/2" {
					t.Fatalf("list under p/ = %+v, want p/1,p/2", got)
				}
			},
		},
		{
			Name: "copy duplicates an object",
			Run: func(t *testing.T, env *Env) {
				if _, err := env.Blob.Put(env.Ctx, "src/k", bytes.NewReader([]byte("dup")), blob.PutOpts{Size: 3}); err != nil {
					t.Fatal(err)
				}
				if _, err := env.Blob.Copy(env.Ctx, "src/k", "dst/k"); err != nil {
					t.Fatal(err)
				}
				rc, _, err := env.Blob.Get(env.Ctx, "dst/k")
				if err != nil {
					t.Fatal(err)
				}
				got, _ := io.ReadAll(rc)
				_ = rc.Close()
				if string(got) != "dup" {
					t.Fatalf("copy body = %q, want dup", got)
				}
			},
		},
		{
			Name:     "presign get returns a url",
			Requires: []Capability{CapBlobPresign},
			Run: func(t *testing.T, env *Env) {
				if _, err := env.Blob.Put(env.Ctx, "pre/k", bytes.NewReader([]byte("x")), blob.PutOpts{Size: 1}); err != nil {
					t.Fatal(err)
				}
				p, ok := env.Blob.(blob.Presigner)
				if !ok {
					t.Fatal("Caps.Presign true but Store is not a Presigner")
				}
				url, err := p.PresignGet(env.Ctx, "pre/k", 60_000_000_000) // 60s
				if err != nil {
					t.Fatal(err)
				}
				if url == "" {
					t.Fatal("presigned url is empty")
				}
			},
		},
	}
}

// RunBlob drives a backend through the byte-plane case table. Backends that do
// not wire Env.Blob are skipped; capability-gated cases skip when the backend
// lacks the capability.
func RunBlob(t *testing.T, b Backend) {
	t.Helper()
	if b.Setup(t).Blob == nil {
		t.Skipf("conformance: %s does not implement the blob port", b.Name())
		return
	}
	for _, tc := range BlobCases() {
		tc := tc
		t.Run("blob/"+tc.Name, func(t *testing.T) {
			env := b.Setup(t)
			if miss := b.Capabilities().missing(tc.Requires); len(miss) > 0 {
				t.Skipf("conformance: %s lacks %v", b.Name(), miss)
				return
			}
			tc.Run(t, env)
		})
	}
}
