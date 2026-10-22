package main

import (
	"fmt"
	"strconv"
	"time"

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
		return fmt.Errorf("unknown FABRIQ_DOMAIN_PACK %q (want example or none)", pack)
	}
}
