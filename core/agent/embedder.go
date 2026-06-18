package agent

import "context"

// Embedder turns text into vectors. The host supplies the implementation
// (Anthropic, OpenAI, a local model); fabriq stays model-agnostic. Embed
// returns one vector per input string, in order. Dims reports the embedding
// dimensionality, validated against the vector port at NewToolkit.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Dims() int
}
