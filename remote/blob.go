package remote

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// blobChunkSize bounds each upload/download frame so neither side ever buffers a
// whole object.
const blobChunkSize = 256 * 1024 // 256 KiB

// remoteBlobStore is the client face of blob.Store. Put streams the body up in
// chunks, Get streams it down; Head, Delete and the presign bypass are unary.
// List, Copy and multipart/range are follow-ons (ErrNotImplemented; the reported
// caps advertise only presign).
type remoteBlobStore struct{ t Transport }

var (
	_ blob.Store     = remoteBlobStore{}
	_ blob.Presigner = remoteBlobStore{}
)

// Put streams r to the server as a metadata frame followed by data frames.
func (b remoteBlobStore) Put(ctx context.Context, key string, r io.Reader, o blob.PutOpts) (blob.ObjectInfo, error) {
	conn, err := b.t.ClientStream(ctx, MethodPutBlob)
	if err != nil {
		return blob.ObjectInfo{}, err
	}
	meta, err := proto.Marshal(&fabriqpb.BlobChunk{Key: key, ContentType: o.ContentType, Size: o.Size})
	if err != nil {
		_ = conn.Close()
		return blob.ObjectInfo{}, err
	}
	if err := conn.Send(meta); err != nil {
		_ = conn.Close()
		return blob.ObjectInfo{}, err
	}
	buf := make([]byte, blobChunkSize)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			frame, merr := proto.Marshal(&fabriqpb.BlobChunk{Data: buf[:n]})
			if merr != nil {
				_ = conn.Close()
				return blob.ObjectInfo{}, merr
			}
			if serr := conn.Send(frame); serr != nil {
				_ = conn.Close()
				return blob.ObjectInfo{}, serr
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = conn.Close()
			return blob.ObjectInfo{}, rerr
		}
	}
	out, err := conn.CloseAndRecv()
	if err != nil {
		return blob.ObjectInfo{}, err
	}
	var reply fabriqpb.BlobInfoReply
	if err := proto.Unmarshal(out, &reply); err != nil {
		return blob.ObjectInfo{}, fmt.Errorf("remote: decode put reply: %w", err)
	}
	if reply.Error != nil {
		return blob.ObjectInfo{}, errorFromProto(reply.Error)
	}
	return objectInfoFromProto(reply.Info), nil
}

// Get returns a streaming reader over the object's bytes; a missing key surfaces
// as the server's typed ErrNotFound on the first frame.
func (b remoteBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, blob.ObjectInfo, error) {
	in, err := proto.Marshal(&fabriqpb.BlobKey{Key: key})
	if err != nil {
		return nil, blob.ObjectInfo{}, err
	}
	stream, err := b.t.ServerStream(ctx, MethodGetBlob, in)
	if err != nil {
		return nil, blob.ObjectInfo{}, err
	}
	first, err := stream.Recv()
	if err != nil {
		_ = stream.Close()
		return nil, blob.ObjectInfo{}, err
	}
	var head fabriqpb.GetBlobFrame
	if err := proto.Unmarshal(first, &head); err != nil {
		_ = stream.Close()
		return nil, blob.ObjectInfo{}, fmt.Errorf("remote: decode get head: %w", err)
	}
	if head.Error != nil {
		_ = stream.Close()
		return nil, blob.ObjectInfo{}, errorFromProto(head.Error)
	}
	return &blobReader{stream: stream}, objectInfoFromProto(head.Info), nil
}

func (b remoteBlobStore) Head(ctx context.Context, key string) (blob.ObjectInfo, error) {
	in, err := proto.Marshal(&fabriqpb.BlobKey{Key: key})
	if err != nil {
		return blob.ObjectInfo{}, err
	}
	out, err := b.t.Unary(ctx, MethodHeadBlob, in)
	if err != nil {
		return blob.ObjectInfo{}, err
	}
	var reply fabriqpb.BlobInfoReply
	if err := proto.Unmarshal(out, &reply); err != nil {
		return blob.ObjectInfo{}, fmt.Errorf("remote: decode head reply: %w", err)
	}
	if reply.Error != nil {
		return blob.ObjectInfo{}, errorFromProto(reply.Error)
	}
	return objectInfoFromProto(reply.Info), nil
}

func (b remoteBlobStore) Delete(ctx context.Context, key string) error {
	in, err := proto.Marshal(&fabriqpb.BlobKey{Key: key})
	if err != nil {
		return err
	}
	out, err := b.t.Unary(ctx, MethodDeleteBlob, in)
	if err != nil {
		return err
	}
	var reply fabriqpb.BlobAck
	if err := proto.Unmarshal(out, &reply); err != nil {
		return fmt.Errorf("remote: decode delete reply: %w", err)
	}
	return errorFromProto(reply.Error)
}

// List and Copy are follow-ons.
func (b remoteBlobStore) List(context.Context, string) ([]blob.ObjectInfo, error) {
	return nil, ErrNotImplemented
}

func (b remoteBlobStore) Copy(context.Context, string, string) (blob.ObjectInfo, error) {
	return blob.ObjectInfo{}, ErrNotImplemented
}

// Capabilities reports what the REMOTE store wires: presign is, multipart/range
// remoting is a follow-on. (The server's own store may support more.)
func (b remoteBlobStore) Capabilities() blob.Caps {
	return blob.Caps{Presign: true}
}

func (b remoteBlobStore) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	return b.presign(ctx, key, "GET", ttl)
}

func (b remoteBlobStore) PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error) {
	return b.presign(ctx, key, "PUT", ttl)
}

func (b remoteBlobStore) presign(ctx context.Context, key, method string, ttl time.Duration) (string, error) {
	in, err := proto.Marshal(&fabriqpb.BlobPresign{Key: key, Method: method, TtlSeconds: int64(ttl / time.Second)})
	if err != nil {
		return "", err
	}
	out, err := b.t.Unary(ctx, MethodPresignBlob, in)
	if err != nil {
		return "", err
	}
	var reply fabriqpb.BlobPresignReply
	if err := proto.Unmarshal(out, &reply); err != nil {
		return "", fmt.Errorf("remote: decode presign reply: %w", err)
	}
	if reply.Error != nil {
		return "", errorFromProto(reply.Error)
	}
	return reply.Url, nil
}

// blobReader presents the GetBlob data frames as an io.ReadCloser, holding at
// most one frame's worth of bytes at a time.
type blobReader struct {
	stream Stream
	buf    []byte
	done   bool
}

func (r *blobReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.done {
			return 0, io.EOF
		}
		frame, err := r.stream.Recv()
		if err != nil {
			r.done = true
			return 0, err // io.EOF at clean end, or transport error
		}
		var f fabriqpb.GetBlobFrame
		if err := proto.Unmarshal(frame, &f); err != nil {
			return 0, fmt.Errorf("remote: decode get frame: %w", err)
		}
		if f.Error != nil {
			r.done = true
			return 0, errorFromProto(f.Error)
		}
		r.buf = f.Data
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *blobReader) Close() error { return r.stream.Close() }

func objectInfoFromProto(i *fabriqpb.BlobObjectInfo) blob.ObjectInfo {
	if i == nil {
		return blob.ObjectInfo{}
	}
	oi := blob.ObjectInfo{Key: i.Key, Size: i.Size, Checksum: i.Checksum, ContentType: i.ContentType}
	if i.ModifiedAtUnixNano != 0 {
		oi.ModifiedAt = time.Unix(0, i.ModifiedAtUnixNano).UTC()
	}
	return oi
}

func objectInfoToProto(i blob.ObjectInfo) *fabriqpb.BlobObjectInfo {
	pi := &fabriqpb.BlobObjectInfo{Key: i.Key, Size: i.Size, Checksum: i.Checksum, ContentType: i.ContentType}
	if !i.ModifiedAt.IsZero() {
		pi.ModifiedAtUnixNano = i.ModifiedAt.UnixNano()
	}
	return pi
}
