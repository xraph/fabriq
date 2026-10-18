package forgeext

import (
	"context"
	"time"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/internal/metrics"
)

// meteredEmbedder records every call the embed and distill workers make to
// emb as fabriq_model_call_duration_seconds{kind="embed"}. core/agent already
// traces the call and drops inbound baggage before it gets here. A nil m (or
// a nil emb) passes emb through unchanged.
func meteredEmbedder(emb agent.Embedder, m *metrics.Metrics) agent.Embedder {
	if emb == nil || m == nil {
		return emb
	}
	return embedderMeter{Embedder: emb, m: m}
}

type embedderMeter struct {
	agent.Embedder
	m *metrics.Metrics
}

func (e embedderMeter) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	start := time.Now()
	vecs, err := e.Embedder.Embed(ctx, texts)
	e.m.ObserveModelCall("embed", modelResult(err), time.Since(start))
	return vecs, err
}

// meteredSummarizer is meteredEmbedder for the distill worker's Summarizer
// (kind="summarize").
func meteredSummarizer(sum agent.Summarizer, m *metrics.Metrics) agent.Summarizer {
	if sum == nil || m == nil {
		return sum
	}
	return summarizerMeter{Summarizer: sum, m: m}
}

type summarizerMeter struct {
	agent.Summarizer
	m *metrics.Metrics
}

func (s summarizerMeter) Summarize(ctx context.Context, in agent.SummaryInput) (string, error) {
	start := time.Now()
	out, err := s.Summarizer.Summarize(ctx, in)
	s.m.ObserveModelCall("summarize", modelResult(err), time.Since(start))
	return out, err
}

func modelResult(err error) string {
	if err != nil {
		return telemetry.ResultError
	}
	return telemetry.ResultOK
}
