package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
)

// envBool reads key through getenv as a bool. An empty value returns def; a
// value strconv.ParseBool rejects is an error that names the key.
func envBool(getenv func(string) string, key string, def bool) (bool, error) {
	v := getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %q is not a boolean (want true or false)", key, v)
	}
	return b, nil
}

// envDuration reads key through getenv as a time.Duration. An empty value
// returns def; an unparseable value is an error that names the key.
func envDuration(getenv func(string) string, key string, def time.Duration) (time.Duration, error) {
	v := getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration (want something like 30s)", key, v)
	}
	return d, nil
}

// errUnknownDomainPack marks a FABRIQ_DOMAIN_PACK value registerDomainPack
// does not know, so callers can tell a bad setting from a failed registration.
var errUnknownDomainPack = errors.New("unknown FABRIQ_DOMAIN_PACK")

// registerDomainPack registers the entity pack FABRIQ_DOMAIN_PACK selects.
// "" and "example" register the example pack, "none" registers nothing, and
// anything else is an error.
func registerDomainPack(reg *registry.Registry, pack string) error {
	switch pack {
	case "", "example":
		return domain.RegisterAll(reg)
	case "none":
		return nil
	default:
		return fmt.Errorf("%w %q (want example or none)", errUnknownDomainPack, pack)
	}
}

// storeConfigFromEnv reads every store setting but the Postgres DSN through
// getenv, for the operator commands that build their config by hand rather
// than through forgeext.LoadConfig: FABRIQ_REDIS_ADDR, FABRIQ_REDIS_USERNAME,
// FABRIQ_REDIS_PASSWORD, FABRIQ_FALKORDB_ADDR, FABRIQ_FALKORDB_USERNAME,
// FABRIQ_FALKORDB_PASSWORD, FABRIQ_ELASTICSEARCH_ADDRS (comma-separated),
// FABRIQ_ELASTICSEARCH_USERNAME and FABRIQ_ELASTICSEARCH_PASSWORD. Unset
// variables leave their fields zero. The commands' address flags still win.
func storeConfigFromEnv(getenv func(string) string) fabriq.Config {
	return fabriq.Config{
		Redis: fabriq.RedisConfig{
			Addr:     getenv("FABRIQ_REDIS_ADDR"),
			Username: getenv("FABRIQ_REDIS_USERNAME"),
			Password: getenv("FABRIQ_REDIS_PASSWORD"),
		},
		FalkorDB: fabriq.FalkorDBConfig{
			Addr:     getenv("FABRIQ_FALKORDB_ADDR"),
			Username: getenv("FABRIQ_FALKORDB_USERNAME"),
			Password: getenv("FABRIQ_FALKORDB_PASSWORD"),
		},
		Elasticsearch: fabriq.ElasticsearchConfig{
			Addrs:    splitAddrs(getenv("FABRIQ_ELASTICSEARCH_ADDRS")),
			Username: getenv("FABRIQ_ELASTICSEARCH_USERNAME"),
			Password: getenv("FABRIQ_ELASTICSEARCH_PASSWORD"),
		},
	}
}

// splitAddrs splits a comma-separated address list, trimming spaces and
// dropping empty entries. An empty list is nil.
func splitAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}
