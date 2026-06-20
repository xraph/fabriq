// core/agent/toolkit.go
package agent

import (
	"fmt"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// WritePolicy is the opt-in allowlist for agent writes. An entity/op absent
// from Allow is denied (deny-by-default).
type WritePolicy struct {
	Allow map[string][]command.Op
}

func (p WritePolicy) allows(entity string, op command.Op) bool {
	for _, o := range p.Allow[entity] {
		if o == op {
			return true
		}
	}
	return false
}

const (
	defaultK          = 24
	defaultHops       = 1
	defaultVectorDims = 768
	defaultGraphSeeds = 8
)

// Config tunes the toolkit. Zero values get sensible defaults via withDefaults.
type Config struct {
	K              int                // candidates per channel (default 24)
	Hops           int                // graph expansion depth (default 1)
	VectorDims     int                // expected embedding dims (default 768)
	ChannelWeights map[string]float64 // per-channel RRF weight (default 1.0 each)
	Tokenizer      func([]byte) int   // token estimator (default bytes/4)
	Strict         bool               // fail on any channel error (default false: lenient)
	GraphSeeds     int                // top seeds (vector+search) to expand in the graph channel (default 8)
	GraphReverse   bool               // expand incoming (reverse) edges too; default false
	Write          WritePolicy        // agent write allowlist; empty = no writes
	Altitude       Altitude           // distillation layer to surface (default AltAuto: budget decides)
	CAS            blob.CAS           // content-addressed store for digest summaries (optional; nil = no CAS-backed ops)
}

func defaultTokenizer(b []byte) int { return (len(b) + 3) / 4 }

func (c *Config) withDefaults() {
	if c.K <= 0 {
		c.K = defaultK
	}
	if c.Hops <= 0 {
		c.Hops = defaultHops
	}
	if c.VectorDims <= 0 {
		c.VectorDims = defaultVectorDims
	}
	if c.Tokenizer == nil {
		c.Tokenizer = defaultTokenizer
	}
	if c.GraphSeeds <= 0 {
		c.GraphSeeds = defaultGraphSeeds
	}
}

// Toolkit is the transport-agnostic agent surface over the fabriq facade.
type Toolkit struct {
	fab      query.Fabric
	reg      *registry.Registry
	emb      Embedder
	cfg      Config
	cas      blob.CAS
	revEdges map[string][]reverseEdge // cached result of reverseEdgeIndex(reg); built once in NewToolkit
}

// NewToolkit builds a Toolkit. emb may be nil (semantic recall is then skipped).
func NewToolkit(fab query.Fabric, reg *registry.Registry, emb Embedder, cfg Config) (*Toolkit, error) {
	if fab == nil {
		return nil, fmt.Errorf("agent: nil Fabric")
	}
	if reg == nil {
		return nil, fmt.Errorf("agent: nil Registry")
	}
	cfg.withDefaults()
	if emb != nil && emb.Dims() != cfg.VectorDims {
		return nil, fmt.Errorf("agent: embedder dims %d != configured vector dims %d", emb.Dims(), cfg.VectorDims)
	}
	return &Toolkit{fab: fab, reg: reg, emb: emb, cfg: cfg, cas: cfg.CAS, revEdges: reverseEdgeIndex(reg)}, nil
}
