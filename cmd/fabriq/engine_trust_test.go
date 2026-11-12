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

func TestOperatorRedisTrust(t *testing.T) {
	const ca = "-----BEGIN CERTIFICATE-----\r\nstream fixture\r\n-----END CERTIFICATE-----\r\n"
	cfg := storeConfigFromEnv(func(k string) string {
		return map[string]string{
			"FABRIQ_REDIS_ADDR":     "stream:25061",
			"FABRIQ_REDIS_TLS_MODE": "verify-full",
			"FABRIQ_REDIS_CA_CERT":  ca,
		}[k]
	})
	raw, err := json.Marshal(cfg.Redis)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Addr    string `json:"addr"`
		TLSMode string `json:"tlsMode"`
		CACert  string `json:"caCert"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Addr != "stream:25061" || got.TLSMode != "verify-full" || got.CACert != ca {
		t.Fatalf("operator redis = %+v", got)
	}
}
