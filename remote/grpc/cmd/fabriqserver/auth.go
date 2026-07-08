package main

import (
	"context"
	"errors"

	remotegrpc "github.com/xraph/fabriq/remote/grpc"
)

// devAuthenticator is a DEVELOPMENT-ONLY Authenticator: it trusts the bearer
// token AS the tenant id, with no verification. It exists so this reference
// server runs against a plain fabriq+grpc://<tenant>@host DSN out of the box.
//
// Do NOT use it in production. A real deployment verifies the credential — a
// signed bearer/JWT or the mTLS client certificate — and maps the *verified*
// identity to a tenant (remotegrpc.BearerToken / remotegrpc.ClientCertificate),
// never trusting an unauthenticated value as the tenant.
func devAuthenticator() remotegrpc.Authenticator {
	return func(ctx context.Context) (context.Context, error) {
		tok, ok := remotegrpc.BearerToken(ctx)
		if !ok || tok == "" {
			return nil, errors.New("fabriqserver: missing bearer credential")
		}
		return remotegrpc.WithTenant(ctx, tok)
	}
}
