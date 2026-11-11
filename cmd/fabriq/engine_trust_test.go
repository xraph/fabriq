package main

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xraph/forge/cli"
)

func TestOperatorEngineTrust(t *testing.T) {
	const graphCA = "-----BEGIN CERTIFICATE-----\r\ngraph fixture\r\n-----END CERTIFICATE-----\r\n"
	const searchCA = "-----BEGIN CERTIFICATE-----\nsearch fixture\n-----END CERTIFICATE-----\n"
	for key, val := range map[string]string{
		"FABRIQ_FALKORDB_ADDR":         "env-graph:6380",
		"FABRIQ_FALKORDB_TLS_MODE":     "verify-full",
		"FABRIQ_FALKORDB_CA_CERT":      graphCA,
		"FABRIQ_ELASTICSEARCH_ADDRS":   "https://env-search:9200",
		"FABRIQ_ELASTICSEARCH_CA_CERT": searchCA,
	} {
		t.Setenv(key, val)
	}
	cfg := operatorStoreConfig(engineAddressFlags{values: map[string]string{
		"falkordb":      "flag-graph:6380",
		"elasticsearch": " https://one:9200, ,https://two:9200 ",
	}}, "postgres://fixture/source")
	if cfg.FalkorDB.Addr != "flag-graph:6380" || !reflect.DeepEqual(cfg.Elasticsearch.Addrs, []string{"https://one:9200", "https://two:9200"}) {
		t.Fatal("operator address flags did not override the environment")
	}
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
	if settings.FalkorDB.TLSMode != "verify-full" || settings.FalkorDB.CACert != graphCA || settings.Elasticsearch.CACert != searchCA {
		t.Fatal("operator lost the TLS mode or changed multiline CA bytes")
	}
}

type engineAddressFlags struct {
	cli.CommandContext
	values map[string]string
}

func (c engineAddressFlags) String(name string) string { return c.values[name] }
