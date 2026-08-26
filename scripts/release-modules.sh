#!/bin/bash
# Read-only preflight for fabriq's five-module release.
#
# This script NEVER tags, pushes, edits go.mod, or commits anything. It only
# validates preconditions and prints the exact ordered plan that
# .github/workflows/release-dispatch.yml performs for real. Releases are cut
# through that workflow, not by running this script with any flag:
#
#   gh workflow run release-dispatch.yml -f tag=vX.Y.Z -f dry_run=false
#
# Usage: ./scripts/release-modules.sh <version>
#
# fabriq's root module requires github.com/xraph/fabriq/core itself (with a
# local "replace" for development). Consumers ignore replace directives, so a
# released root that still names core at the v0.0.0 placeholder breaks every
# `go get github.com/xraph/fabriq@vX.Y.Z` that does not also separately
# require fabriq/core at a real version. The fix is ordering: tag and push
# core first, bump the root's require to that real tag, verify it resolves
# with the replace dropped, then tag the root. That ordering now lives in
# release-dispatch.yml, including the real-proxy verification step. This
# script is only the local sanity check you run before triggering it.
#
# scripts/check-module-versions.sh is the general read-only health check for
# the same class of problem (placeholders, go-version drift), safe to run at
# any time, not just before a release.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}i${NC} $1"
}

print_success() {
    echo -e "${GREEN}v${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}!${NC} $1"
}

print_error() {
    echo -e "${RED}x${NC} $1"
}

print_step() {
    echo ""
    echo -e "${BLUE}=== $1 ===${NC}"
}

usage() {
    echo "Usage: $0 <version>"
    echo ""
    echo "Read-only preflight, no flags. It validates the version, checks the"
    echo "working tree and module consistency, then prints the ordered plan"
    echo "release-dispatch.yml will run. It never tags, pushes, edits go.mod,"
    echo "or commits, so there is no --execute or --dry-run flag here: every"
    echo "run of this script behaves like a dry run."
    echo ""
    echo "Example:"
    echo "  $0 1.7.0"
    echo ""
    echo "To actually cut the release, trigger the workflow:"
    echo "  gh workflow run release-dispatch.yml -f tag=v1.7.0 -f dry_run=false"
    echo ""
    echo "To preview the same plan from the workflow side without pushing"
    echo "anything, leave dry_run at its default:"
    echo "  gh workflow run release-dispatch.yml -f tag=v1.7.0"
}

# --- Argument parsing --------------------------------------------------

VERSION="${1:-}"

if [ -z "$VERSION" ] || [ "$#" -ne 1 ]; then
    usage
    exit 1
fi

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
    print_error "Invalid semantic version: $VERSION"
    echo "Expected format: 1.2.3 or 1.2.3-beta.1"
    exit 1
fi

print_info "Read-only preflight for v$VERSION. This script makes no changes: no tag, no push, no go.mod edit, no commit."

# --- Preconditions ------------------------------------------------------

if [ ! -f "go.mod" ] || [ "$(head -1 go.mod)" != "module github.com/xraph/fabriq" ]; then
    print_error "Must be run from the fabriq repository root"
    exit 1
fi

if [ "$(git rev-parse --show-toplevel)" != "$(pwd)" ]; then
    print_error "Must be run from the repository root (git top level)"
    exit 1
fi

if ! git diff-index --quiet HEAD -- || [ -n "$(git status --porcelain --untracked-files=all)" ]; then
    print_error "Working directory is not clean. Commit or stash changes first."
    git status --short
    exit 1
fi

print_success "Working directory is clean"

TAG_ROOT="v$VERSION"
TAG_CORE="core/v$VERSION"
TAG_GRPC="remote/grpc/v$VERSION"
TAG_WARDEN="adapters/wardenauthz/v$VERSION"
TAG_FABRIQSERVER="remote/grpc/cmd/fabriqserver/v$VERSION"

for tag in "$TAG_ROOT" "$TAG_CORE" "$TAG_GRPC" "$TAG_WARDEN" "$TAG_FABRIQSERVER"; do
    if git rev-parse "$tag" >/dev/null 2>&1; then
        print_error "Tag $tag already exists locally!"
        exit 1
    fi
done

print_success "None of the five release tags exist locally yet"

# --- Module go-directive consistency -------------------------------------

print_step "Checking go directives agree across all five modules"

ROOT_GO=$(grep '^go ' go.mod | awk '{print $2}')
MISMATCH=0
for gomod in core/go.mod remote/grpc/go.mod adapters/wardenauthz/go.mod remote/grpc/cmd/fabriqserver/go.mod; do
    if [ ! -f "$gomod" ]; then
        print_error "Expected module not found: $gomod"
        MISMATCH=1
        continue
    fi
    MOD_GO=$(grep '^go ' "$gomod" | awk '{print $2}')
    if [ "$MOD_GO" != "$ROOT_GO" ]; then
        print_error "$gomod uses go $MOD_GO, root uses go $ROOT_GO"
        MISMATCH=1
    else
        print_success "$gomod: go $MOD_GO"
    fi
done

if [ "$MISMATCH" -ne 0 ]; then
    print_error "Fix the go-directive mismatch(es) above before releasing (go mod edit -go=$ROOT_GO in the affected module)."
    exit 1
fi

print_success "All five modules agree on go $ROOT_GO"

CORE_IMPORT="github.com/xraph/fabriq/core"

# --- Print the ordered plan (this script performs none of it) -----------

print_step "Ordered release plan (release-dispatch.yml performs this; this script does not)"

cat <<PLAN
  1. tag + push $TAG_CORE
  2. go mod edit -require=${CORE_IMPORT}@v${VERSION}
  3. verify ${CORE_IMPORT}@v${VERSION} resolves on the real module proxy, in
     a scratch module outside this repo, with the replace directive not in
     effect (the release aborts here, before root is tagged, if this fails)
  4. commit the require bump
  5. push that commit
  6. tag + push $TAG_ROOT   (triggers release.yml)
  7. tag + push $TAG_GRPC
  8. tag + push $TAG_WARDEN
  9. tag + push $TAG_FABRIQSERVER
PLAN

echo ""
print_success "Preflight passed for v$VERSION. Nothing was tagged, pushed, edited, or committed."
print_info "Run the release for real:"
echo "  gh workflow run release-dispatch.yml -f tag=$TAG_ROOT -f dry_run=false"
print_info "Or trigger the workflow's own dry run first, to see this same plan echoed there:"
echo "  gh workflow run release-dispatch.yml -f tag=$TAG_ROOT"
