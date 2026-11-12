package forgeext_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/forgeext"
)

func TestLoadEngineTrust(t *testing.T) {
	for _, key := range []string{"FABRIQ_FALKORDB_TLS_MODE", "FABRIQ_FALKORDB_CA_CERT", "FABRIQ_ELASTICSEARCH_CA_CERT"} {
		t.Setenv(key, "")
	}
	const fixture = `falkordb:
  addr: graph:6380
  tlsMode: verify-full
  caCert: |-
    graph file line one
    graph file line two
elasticsearch:
  addrs: [https://search:9200]
  caCert: |-
    search file line one
    search file line two
extensions:
  fabriq:
    falkordb:
      addr: host-graph:6380
      tlsMode: verify-full
      caCert: |-
        host graph line one
        host graph line two
    elasticsearch:
      addrs: [https://host-search:9200]
      caCert: |-
        host search line one
        host search line two
`
	cm := newEnvConfigApp(t, fixture)
	checkEngineTrust(t, forgeext.LoadConfig(cm, ""), "verify-full", "graph file line one\ngraph file line two", "search file line one\nsearch file line two")

	const envGraph = "graph env line one\r\ngraph env line two\r\n"
	const envSearch = "search env line one\nsearch env line two\n"
	t.Setenv("FABRIQ_FALKORDB_TLS_MODE", "disable")
	t.Setenv("FABRIQ_FALKORDB_CA_CERT", envGraph)
	t.Setenv("FABRIQ_ELASTICSEARCH_CA_CERT", envSearch)
	checkEngineTrust(t, forgeext.LoadConfig(cm, ""), "disable", envGraph, envSearch)
	checkEngineTrust(t, forgeext.LoadConfig(cm, "extensions.fabriq."), "verify-full", "host graph line one\nhost graph line two", "host search line one\nhost search line two")

	t.Setenv("FABRIQ_FALKORDB_TLS_MODE", "")
	t.Setenv("FABRIQ_FALKORDB_CA_CERT", "")
	t.Setenv("FABRIQ_ELASTICSEARCH_CA_CERT", "")
	checkEngineTrust(t, forgeext.LoadConfig(cm, ""), "verify-full", "graph file line one\ngraph file line two", "search file line one\nsearch file line two")
	checkEngineTrust(t, forgeext.LoadConfig(newEnvConfigApp(t, ""), ""), "", "", "")
}

func TestLoadEngineTrustFromServeEnvironment(t *testing.T) {
	t.Setenv("FABRIQ_FALKORDB_ADDR", "graph:6380")
	t.Setenv("FABRIQ_FALKORDB_TLS_MODE", "verify-full")
	t.Setenv("FABRIQ_FALKORDB_CA_CERT", "graph\nCA\n")
	t.Setenv("FABRIQ_ELASTICSEARCH_ADDRS", "https://search:9200")
	t.Setenv("FABRIQ_ELASTICSEARCH_CA_CERT", "search\nCA\n")
	cfg := forgeext.LoadConfig(newEnvConfigApp(t, ""), "")
	checkEngineTrust(t, cfg, "verify-full", "graph\nCA\n", "search\nCA\n")
	if cfg.FalkorDB.Addr != "graph:6380" || strings.Join(cfg.Elasticsearch.Addrs, ",") != "https://search:9200" {
		t.Fatal("serve environment did not provide the engine endpoints")
	}
}

func checkEngineTrust(t *testing.T, cfg fabriq.Config, mode, graphCA, searchCA string) {
	t.Helper()
	// Check the serialized configuration contract on the baseline and candidate.
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var settings struct {
		FalkorDB struct {
			TLSMode string `json:"tlsMode"`
			CACert  string `json:"caCert"`
		} `json:"falkordb"`
		Elasticsearch struct {
			CACert string `json:"caCert"`
		} `json:"elasticsearch"`
	}
	if err := json.Unmarshal(raw, &settings); err != nil {
		t.Fatal(err)
	}
	if settings.FalkorDB.TLSMode != mode || settings.FalkorDB.CACert != graphCA || settings.Elasticsearch.CACert != searchCA {
		t.Fatal("engine trust settings did not bind with the expected scope, precedence and PEM bytes")
	}
}

// Redis trust binds like FalkorDB's: from the file under either prefix, and
// from FABRIQ_REDIS_TLS_MODE and FABRIQ_REDIS_CA_CERT for the standalone
// keys only, multiline PEM intact.
func TestLoadRedisTrust(t *testing.T) {
	t.Setenv("FABRIQ_REDIS_TLS_MODE", "")
	t.Setenv("FABRIQ_REDIS_CA_CERT", "")
	const fixture = `redis:
  addr: stream:6380
  tlsMode: verify-full
  caCert: |-
    stream file line one
    stream file line two
extensions:
  fabriq:
    redis:
      addr: host-stream:6380
      tlsMode: verify-full
      caCert: |-
        host stream line one
        host stream line two
`
	cm := newEnvConfigApp(t, fixture)
	checkRedisTrust(t, forgeext.LoadConfig(cm, ""), "verify-full", "stream file line one\nstream file line two")
	checkRedisTrust(t, forgeext.LoadConfig(cm, "extensions.fabriq."), "verify-full", "host stream line one\nhost stream line two")

	const envCA = "stream env line one\r\nstream env line two\r\n"
	t.Setenv("FABRIQ_REDIS_TLS_MODE", "disable")
	t.Setenv("FABRIQ_REDIS_CA_CERT", envCA)
	checkRedisTrust(t, forgeext.LoadConfig(cm, ""), "disable", envCA)
	checkRedisTrust(t, forgeext.LoadConfig(cm, "extensions.fabriq."), "verify-full", "host stream line one\nhost stream line two")
}

func checkRedisTrust(t *testing.T, cfg fabriq.Config, mode, ca string) {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var settings struct {
		Redis struct {
			TLSMode string `json:"tlsMode"`
			CACert  string `json:"caCert"`
		} `json:"redis"`
	}
	if err := json.Unmarshal(raw, &settings); err != nil {
		t.Fatal(err)
	}
	if settings.Redis.TLSMode != mode || settings.Redis.CACert != ca {
		t.Fatalf("redis trust = %q, %q; want %q, %q", settings.Redis.TLSMode, settings.Redis.CACert, mode, ca)
	}
}
