package wardenauthz_test

import (
	"context"
	"testing"

	"github.com/xraph/forge"
	"github.com/xraph/warden"
	"github.com/xraph/warden/assignment"
	"github.com/xraph/warden/id"
	"github.com/xraph/warden/permission"
	"github.com/xraph/warden/role"
	"github.com/xraph/warden/store/memory"

	"github.com/xraph/fabriq/adapters/wardenauthz"
)

// seedAnalyticsAdmin builds an engine where "alice" has the admin action on the
// analytics resource (matching DefaultMapper("analytics.admin")).
func seedAnalyticsAdmin(t *testing.T) (*warden.Engine, context.Context) {
	t.Helper()
	ctx := warden.WithTenant(context.Background(), "app1", "t1")
	s := memory.New()
	eng, err := warden.NewEngine(warden.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	roleID := id.NewRoleID()
	permID := id.NewPermissionID()
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	must(s.CreateRole(ctx, &role.Role{ID: roleID, TenantID: "t1", Name: "analytics-admin", Slug: "analytics-admin"}))
	must(s.CreatePermission(ctx, &permission.Permission{ID: permID, TenantID: "t1", Name: "analytics:admin", Resource: "analytics", Action: "admin"}))
	must(s.AttachPermission(ctx, roleID, permission.Ref{Name: "analytics:admin"}))
	must(s.CreateAssignment(ctx, &assignment.Assignment{TenantID: "t1", RoleID: roleID, SubjectKind: "user", SubjectID: "alice"}))
	return eng, ctx
}

func TestAuthorize_AllowAndDeny(t *testing.T) {
	eng, ctx := seedAnalyticsAdmin(t)
	sub := func(id string) wardenauthz.Option {
		return wardenauthz.WithSubjectFunc(func(context.Context) warden.Subject {
			return warden.Subject{Kind: warden.SubjectUser, ID: id}
		})
	}
	if ok, err := wardenauthz.New(eng, sub("alice")).Authorize(ctx, "analytics.admin"); err != nil || !ok {
		t.Fatalf("alice analytics.admin = (%v,%v), want (true,nil)", ok, err)
	}
	if ok, err := wardenauthz.New(eng, sub("bob")).Authorize(ctx, "analytics.admin"); err != nil || ok {
		t.Fatalf("bob analytics.admin = (%v,%v), want (false,nil)", ok, err)
	}
	// alice lacks analytics.read (only admin granted) — warden's RBAC match is an
	// exact "resource:action" comparison (matchPermission in warden's engine.go),
	// with only trailing-"*" glob support; "analytics:admin" does not match a
	// required "analytics:read", so a different action on the same resource is
	// denied even though alice holds "admin" on "analytics".
	if ok, err := wardenauthz.New(eng, sub("alice")).Authorize(ctx, "analytics.read"); err != nil || ok {
		t.Fatalf("alice analytics.read = (%v,%v), want (false,nil) — only admin was granted", ok, err)
	}
}

// TestAuthorize_DefaultSubject exercises the DefaultSubject path: a forge user id
// stamped on ctx becomes the warden subject id.
func TestAuthorize_DefaultSubject(t *testing.T) {
	eng, ctx := seedAnalyticsAdmin(t)
	ctx = forge.WithUserID(ctx, "alice") // default subject reads this
	if ok, err := wardenauthz.New(eng).Authorize(ctx, "analytics.admin"); err != nil || !ok {
		t.Fatalf("default-subject alice analytics.admin = (%v,%v), want (true,nil)", ok, err)
	}
}
