package relguard

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// releaseTagPattern matches the root module's release tag shape: vX.Y.Z, with
// an optional pre-release suffix. Nested module tags (core/vX.Y.Z and the
// like) do not match, since this guard is specifically about the root.
var releaseTagPattern = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$`)

// requireVersion returns the version a go.mod's require directive names for
// importPath, or "" if importPath is not required at all. It matches plain
// text fields rather than parsing go.mod as a module file, since the only
// thing this guard needs is the version string that follows the import path,
// wherever it appears (single-line require or inside a require (...) block).
func requireVersion(gomod, importPath string) string {
	for _, line := range strings.Split(gomod, "\n") {
		fields := strings.Fields(line)
		for i, f := range fields {
			if f == importPath && i+1 < len(fields) {
				return fields[i+1]
			}
		}
	}
	return ""
}

// TestRootCoreRequireMatchesReleaseTag is the release gate. fabriq's root
// module requires github.com/xraph/fabriq/core itself, with a local replace
// directive for development. Consumers ignore replace directives, so once
// HEAD is checked out at a root release tag, the root go.mod must require
// fabriq/core at that exact tag. If it still names the v0.0.0 placeholder (or
// anything other than the tag), a consumer running
// `go get github.com/xraph/fabriq@<tag>` fails with "unknown revision v0.0.0"
// because the resolved require never points at a version that exists.
//
// Ordinary development is not a release: HEAD is only ever exactly at a tag
// once the release process itself puts it there, so this test skips on every
// other checkout.
func TestRootCoreRequireMatchesReleaseTag(t *testing.T) {
	cmd := exec.Command("git", "describe", "--tags", "--exact-match", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		t.Skip("HEAD is not checked out exactly at a tag; this guard only applies to release checkouts")
	}
	tag := strings.TrimSpace(string(out))

	if !releaseTagPattern.MatchString(tag) {
		t.Skipf("HEAD is at tag %q, which is not a root release tag (vX.Y.Z); skipping", tag)
	}

	gomod, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatalf("reading root go.mod: %v", err)
	}

	got := requireVersion(string(gomod), "github.com/xraph/fabriq/core")
	if got == "" {
		t.Fatalf("root go.mod does not require github.com/xraph/fabriq/core at all")
	}
	if got != tag {
		t.Fatalf(
			"HEAD is tagged %s but root go.mod requires github.com/xraph/fabriq/core at %s.\n"+
				"A consumer running 'go get github.com/xraph/fabriq@%s' ignores this repo's\n"+
				"local replace directive for fabriq/core and fails with \"unknown revision %s\"\n"+
				"because the require line never names a version that was actually tagged.\n"+
				"Fix: run scripts/release-modules.sh, which tags and pushes core first, then\n"+
				"bumps this require to that tag, then tags root, in that order.",
			tag, got, tag, got,
		)
	}
}
