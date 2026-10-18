package agent

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/telemetry"
)

// embedTexts is the one path through which the agent package calls an
// Embedder. The call runs under a "call embedder" span and starts from a
// context with inbound baggage dropped: an Embedder usually wraps a model
// provider's API, and a client's W3C baggage has no business reaching it.
func embedTexts(ctx context.Context, emb Embedder, texts []string) ([][]float32, error) {
	ctx, span := telemetry.Tracer().Start(telemetry.WithoutBaggage(ctx), "call embedder",
		trace.WithAttributes(telemetry.AttrModelKind.String("embed"), telemetry.AttrInputs.Int(len(texts))))
	defer span.End()
	vecs, err := emb.Embed(ctx, texts)
	if err != nil {
		telemetry.Fail(span, "embed failed", err)
	}
	return vecs, err
}

// summarizeText is the one path through which the agent package calls a
// Summarizer, with the same span and baggage rules as embedTexts.
func summarizeText(ctx context.Context, sum Summarizer, in SummaryInput) (string, error) {
	ctx, span := telemetry.Tracer().Start(telemetry.WithoutBaggage(ctx), "call summarizer",
		trace.WithAttributes(telemetry.AttrModelKind.String("summarize")))
	defer span.End()
	out, err := sum.Summarize(ctx, in)
	if err != nil {
		telemetry.Fail(span, "summarize failed", err)
	}
	return out, err
}
