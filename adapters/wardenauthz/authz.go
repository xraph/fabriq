// Package wardenauthz bridges fabriq's adminapi.Authorizer port to a
// github.com/xraph/warden PBAC engine. It is an opt-in, host-wired module:
// fabriq imports no warden, and this module imports no fabriq (except a
// test-only compile assertion). A type with Authorize(ctx, capability)
// (bool, error) satisfies adminapi.Authorizer structurally.
package wardenauthz

import (
	"context"
	"strings"

	"github.com/xraph/forge"
	"github.com/xraph/warden"
)

// Mapper maps a fabriq capability string to a warden (action, resourceType, resourceID).
type Mapper func(ctx context.Context, capability string) (action, resourceType, resourceID string)

// SubjectFunc resolves the calling subject from the request context.
type SubjectFunc func(ctx context.Context) warden.Subject

// checker is the subset of *warden.Engine the adapter uses; it exists so tests
// can inject a failing engine. *warden.Engine satisfies it.
type checker interface {
	Check(ctx context.Context, req *warden.CheckRequest, opts ...warden.CallOption) (*warden.CheckResult, error)
}

// Authorizer implements fabriq's adminapi.Authorizer over a warden engine.
type Authorizer struct {
	eng     checker
	mapCap  Mapper
	subject SubjectFunc
}

// Option configures an Authorizer.
type Option func(*Authorizer)

// WithMapper overrides the default capability→(action,resource) mapping.
func WithMapper(m Mapper) Option {
	return func(a *Authorizer) {
		if m != nil {
			a.mapCap = m
		}
	}
}

// WithSubjectFunc overrides the default subject resolution.
func WithSubjectFunc(f SubjectFunc) Option {
	return func(a *Authorizer) {
		if f != nil {
			a.subject = f
		}
	}
}

// New builds an Authorizer over a warden engine. eng must be non-nil.
func New(eng *warden.Engine, opts ...Option) *Authorizer {
	if eng == nil {
		panic("wardenauthz: New requires a non-nil *warden.Engine")
	}
	a := &Authorizer{eng: eng, mapCap: DefaultMapper, subject: DefaultSubject}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Authorize satisfies adminapi.Authorizer: it asks the warden engine whether the
// caller (resolved from ctx) may perform the capability's action on its resource.
// A warden error yields (false, err) so the caller fails closed.
func (a *Authorizer) Authorize(ctx context.Context, capability string) (bool, error) {
	action, resType, resID := a.mapCap(ctx, capability)
	res, err := a.eng.Check(ctx, &warden.CheckRequest{
		Subject:  a.subject(ctx),
		Action:   warden.Action{Name: action},
		Resource: warden.Resource{Type: resType, ID: resID},
	})
	if err != nil {
		return false, err
	}
	return res.Allowed, nil
}

// DefaultMapper splits the capability on the LAST dot: "analytics.admin" →
// action "admin", resourceType "analytics"; a dot-less capability →
// action = the whole string, resourceType "". resourceID is always empty (a
// host wanting tenant-scoped resources supplies its own Mapper).
func DefaultMapper(_ context.Context, capability string) (action, resourceType, resourceID string) {
	if i := strings.LastIndex(capability, "."); i >= 0 {
		return capability[i+1:], capability[:i], ""
	}
	return capability, "", ""
}

// DefaultSubject reads the Authsome user id forge stamped on ctx; anonymous when absent.
func DefaultSubject(ctx context.Context) warden.Subject {
	if uid := forge.UserIDFromContext(ctx); uid != "" {
		return warden.Subject{Kind: warden.SubjectUser, ID: uid}
	}
	return warden.Subject{Kind: "unknown", ID: "anonymous"}
}
