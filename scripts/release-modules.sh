#!/bin/bash
# Release fabriq's five Go modules at the same version.
# Usage: ./scripts/release-modules.sh <version> [--execute]
#
# Dry run is the default and the safe choice: with no --execute flag this
# script prints every command it would run and pushes nothing. Pass --execute
# to actually tag and push.
#
# fabriq's root module requires github.com/xraph/fabriq/core itself (with a
# local "replace" for development). Consumers ignore replace directives, so a
# released root that still names core at the v0.0.0 placeholder breaks every
# `go get github.com/xraph/fabriq@vX.Y.Z` that does not also separately
# require fabriq/core at a real version. The fix is ordering: tag and push
# core FIRST, bump the root's require to that real tag, verify it resolves
# with the replace dropped, THEN tag the root. That order is what this script
# performs; scripts/check-module-versions.sh is the read-only check for the
# same problem.

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

# --- Argument parsing --------------------------------------------------

VERSION="${1:-}"
EXECUTE=false

for arg in "${@:2}"; do
    case "$arg" in
        --execute) EXECUTE=true ;;
        --dry-run) EXECUTE=false ;;
        *)
            print_error "Unknown argument: $arg"
            exit 1
            ;;
    esac
done

if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version> [--execute]"
    echo ""
    echo "Examples:"
    echo "  $0 1.7.0             # Dry run: print the ordered release plan, push nothing"
    echo "  $0 1.7.0 --execute   # Actually tag and push all five modules"
    echo ""
    echo "Dry run is the default. Nothing is tagged or pushed without --execute."
    exit 1
fi

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
    print_error "Invalid semantic version: $VERSION"
    echo "Expected format: 1.2.3 or 1.2.3-beta.1"
    exit 1
fi

if $EXECUTE; then
    print_warning "Running in EXECUTE mode. This will tag and push to origin."
else
    print_info "Running in DRY RUN mode (default). No tags will be created, nothing will be pushed."
    print_info "Pass --execute to actually perform the release."
fi

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
        print_error "Tag $tag already exists!"
        exit 1
    fi
done

print_success "None of the five release tags exist yet"

CORE_IMPORT="github.com/xraph/fabriq/core"

# run CMD... - in dry run, just print what would be run. In execute mode,
# print it too, then actually run it.
run() {
    echo "  + $*"
    if $EXECUTE; then
        "$@"
    fi
}

# --- Step 1: tag and push core FIRST ------------------------------------

print_step "Step 1: tag and push core ($TAG_CORE)"
print_info "core has to exist at a real tag before the root's require can point at it."
run git tag -a "$TAG_CORE" -m "Release fabriq/core v$VERSION"
run git push origin "$TAG_CORE"
print_success "core tagged and pushed: $TAG_CORE"

# --- Step 2: bump the root's require for fabriq/core --------------------

print_step "Step 2: bump root's require for $CORE_IMPORT to $TAG_CORE"
run go mod edit "-require=${CORE_IMPORT}@${TAG_CORE}"
print_success "go.mod now requires ${CORE_IMPORT}@${TAG_CORE}"

# --- Step 3: verify the require resolves without the local replace ------

print_step "Step 3: verify $CORE_IMPORT@$TAG_CORE resolves standalone"
print_info "Temporarily dropping the local replace directive, downloading the module,"
print_info "then restoring the replace whether or not the download succeeded."

if $EXECUTE; then
    RESTORE_REPLACE="go mod edit -replace=${CORE_IMPORT}=./core"
    # A trap guarantees the replace directive comes back even if this shell
    # is interrupted mid-verification, so go.mod is never left mangled.
    trap '$RESTORE_REPLACE' EXIT INT TERM

    echo "  + go mod edit -dropreplace=${CORE_IMPORT}"
    go mod edit "-dropreplace=${CORE_IMPORT}"

    echo "  + go mod download ${CORE_IMPORT}"
    if go mod download "${CORE_IMPORT}"; then
        DOWNLOAD_OK=true
    else
        DOWNLOAD_OK=false
    fi

    echo "  + ${RESTORE_REPLACE}"
    eval "$RESTORE_REPLACE"
    trap - EXIT INT TERM

    if ! $DOWNLOAD_OK; then
        print_error "${CORE_IMPORT}@${TAG_CORE} did not resolve without the replace directive."
        print_error "The replace directive has been restored. Nothing further was tagged."
        print_error "Investigate before retrying: was $TAG_CORE actually pushed and visible to the Go proxy?"
        exit 1
    fi
    print_success "${CORE_IMPORT}@${TAG_CORE} resolves cleanly on its own"
else
    echo "  + go mod edit -dropreplace=${CORE_IMPORT}"
    echo "  + go mod download ${CORE_IMPORT}"
    echo "  + go mod edit -replace=${CORE_IMPORT}=./core   (restored via trap, always runs)"
    print_info "(dry run: verification not actually performed)"
fi

# --- Step 4: commit the require bump ------------------------------------

print_step "Step 4: commit the require bump"
run git add go.mod go.sum
run git commit -m "chore: bump fabriq/core requirement to ${TAG_CORE}"
print_success "Committed the require bump"

# --- Step 5: tag and push the root --------------------------------------

print_step "Step 5: tag and push root ($TAG_ROOT)"
run git tag -a "$TAG_ROOT" -m "Release fabriq v$VERSION"
run git push origin "$TAG_ROOT"
print_success "root tagged and pushed: $TAG_ROOT"

# --- Step 6: tag and push the remaining nested modules -------------------

print_step "Step 6: tag and push the remaining nested modules"
for pair in "$TAG_GRPC:remote/grpc" "$TAG_WARDEN:adapters/wardenauthz" "$TAG_FABRIQSERVER:remote/grpc/cmd/fabriqserver"; do
    tag="${pair%%:*}"
    mod="${pair##*:}"
    run git tag -a "$tag" -m "Release ${mod} v${VERSION}"
    run git push origin "$tag"
    print_success "${mod} tagged and pushed: ${tag}"
done

# --- Also push the commit itself -----------------------------------------

print_step "Step 7: push the require-bump commit"
run git push origin HEAD
print_success "Pushed the require-bump commit"

echo ""
if $EXECUTE; then
    print_success "Release v$VERSION complete. All five modules tagged and pushed:"
else
    print_success "Dry run complete. Nothing was tagged or pushed. Re-run with --execute to perform it for real:"
fi
echo "  $TAG_ROOT"
echo "  $TAG_CORE"
echo "  $TAG_GRPC"
echo "  $TAG_WARDEN"
echo "  $TAG_FABRIQSERVER"
