package agent

import (
	"context"
	"testing"
)

type embedderStub struct{ dims int }

func (e embedderStub) Dims() int { return e.dims }
func (e embedderStub) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	return out, nil
}

func TestEmbedder_SatisfiedByStub(t *testing.T) {
	var _ Embedder = embedderStub{dims: 768}
}
