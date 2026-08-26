#!/bin/bash
# Check module versions and dependencies across fabriq's five Go modules.
# Usage: ./scripts/check-module-versions.sh
#
# Safe to run at any time: it only reads go.mod files and git tags, never
# writes anything. Exits 0 when everything lines up, non-zero when it finds
# an issue.
#
# fabriq's root module is NOT a leaf like a typical multi-module repo: it
# requires github.com/xraph/fabriq/core itself (with a local replace for
# development). A released root that still carries a placeholder v0.0.0 for
# that require breaks every consumer, because `go get` ignores replace
# directives. That single check is why this script exists.

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

if [ ! -f "go.mod" ]; then
    print_error "Must be run from repository root"
    exit 1
fi

echo -e "${BLUE}=== fabriq Multi-Module Version Check ===${NC}"
echo ""

# Module table: path, import path, tag glob (bash 3 compatible, no assoc arrays).
MODULE_PATHS=(
    "."
    "core"
    "remote/grpc"
    "adapters/wardenauthz"
    "remote/grpc/cmd/fabriqserver"
)
MODULE_IMPORTS=(
    "github.com/xraph/fabriq"
    "github.com/xraph/fabriq/core"
    "github.com/xraph/fabriq/remote/grpc"
    "github.com/xraph/fabriq/adapters/wardenauthz"
    "github.com/xraph/fabriq/remote/grpc/cmd/fabriqserver"
)
MODULE_TAG_GLOBS=(
    "v[0-9]*"
    "core/v*"
    "remote/grpc/v*"
    "adapters/wardenauthz/v*"
    "remote/grpc/cmd/fabriqserver/v*"
)
MODULE_NAMES=(
    "root"
    "core"
    "remote/grpc"
    "adapters/wardenauthz"
    "remote/grpc/cmd/fabriqserver"
)

ISSUES_FOUND=0

# is_placeholder VERSION - true if it is the zero pseudo-version go mod edit
# writes for a module it cannot resolve (v0.0.0 or a v0.0.0- pseudo-version).
is_placeholder() {
    case "$1" in
        v0.0.0|v0.0.0-*) return 0 ;;
        *) return 1 ;;
    esac
}

# require_version GOMOD IMPORT - the version IMPORT is required at in GOMOD,
# or empty if it is not required there at all. The trailing space in the
# match keeps "fabriq" from matching "fabriq/core".
require_version() {
    local gomod="$1" import="$2"
    grep -E "^\s*${import} v" "$gomod" 2>/dev/null | awk '{print $2}' | head -1 || true
}

# bare_version TAG - strip any "<subdir>/" prefix and the leading "v".
bare_version() {
    local t="${1##*/}"
    echo "${t#v}"
}

MAIN_GOMOD="go.mod"
MAIN_GO_VERSION=$(grep "^go " "$MAIN_GOMOD" | awk '{print $2}')
MAIN_TAG=$(git tag -l "${MODULE_TAG_GLOBS[0]}" --sort=-version:refname | head -1)

echo -e "${BLUE}Root module (fabriq)${NC}"
echo "  Location: ."
echo "  Go Version: $MAIN_GO_VERSION"
if [ -n "$MAIN_TAG" ]; then
    echo "  Latest Tag: $MAIN_TAG"
else
    echo -e "  Latest Tag: ${YELLOW}(none)${NC}"
fi

CORE_DEP=$(require_version "$MAIN_GOMOD" "github.com/xraph/fabriq/core")
echo -n "  Requires fabriq/core: ${CORE_DEP:-(not found)}"
if [ -n "$CORE_DEP" ] && is_placeholder "$CORE_DEP"; then
    echo -e " ${RED}x PLACEHOLDER${NC}"
    print_error "root's go.mod requires github.com/xraph/fabriq/core at $CORE_DEP"
    echo "    A released root carrying that placeholder breaks every standalone"
    echo "    consumer: 'go get github.com/xraph/fabriq@vX.Y.Z' ignores the local"
    echo "    replace directive, so go tries to fetch fabriq/core@v0.0.0 and fails"
    echo "    with \"unknown revision v0.0.0\". Tag and push core first, then run"
    echo "    'go mod edit -require=github.com/xraph/fabriq/core@<tag>' before"
    echo "    tagging root. scripts/release-modules.sh does this in order."
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
else
    echo -e " ${GREEN}v${NC}"
fi

if grep -q "^replace github.com/xraph/fabriq/core" "$MAIN_GOMOD"; then
    echo -e "  Replace Directive: ${GREEN}v Present${NC} (expected, for local development)"
else
    echo -e "  Replace Directive: ${YELLOW}! MISSING${NC}"
fi
echo ""

# Walk the remaining four modules.
for i in 1 2 3 4; do
    path="${MODULE_PATHS[$i]}"
    import="${MODULE_IMPORTS[$i]}"
    glob="${MODULE_TAG_GLOBS[$i]}"
    name="${MODULE_NAMES[$i]}"
    gomod="$path/go.mod"

    if [ ! -f "$gomod" ]; then
        print_error "Expected module not found: $path ($gomod missing)"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
        continue
    fi

    echo -e "${BLUE}Module: $name${NC}"
    echo "  Location: $path"
    echo "  Import: $import"

    GO_VERSION=$(grep "^go " "$gomod" | awk '{print $2}')
    echo -n "  Go Version: $GO_VERSION"
    if [ "$GO_VERSION" != "$MAIN_GO_VERSION" ]; then
        echo -e " ${RED}x MISMATCH${NC} (root uses $MAIN_GO_VERSION)"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    else
        echo -e " ${GREEN}v${NC}"
    fi

    # Report what this module requires for every other in-repo fabriq module
    # it depends on, flagging placeholder versions the same way as root/core.
    for j in 0 1 2 3 4; do
        [ "$j" -eq "$i" ] && continue
        dep_import="${MODULE_IMPORTS[$j]}"
        dep_version=$(require_version "$gomod" "$dep_import")
        [ -z "$dep_version" ] && continue

        echo -n "  Requires ${MODULE_NAMES[$j]} ($dep_import): $dep_version"
        if is_placeholder "$dep_version"; then
            echo -e " ${RED}x PLACEHOLDER${NC}"
            print_error "$path/go.mod requires $dep_import at $dep_version"
            echo "    Same class of bug as root/core: a standalone consumer of this"
            echo "    module ignores its replace directives and fails to resolve"
            echo "    $dep_import at that placeholder. Fix it the same way: tag the"
            echo "    dependency first, then bump this require to the real tag."
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        else
            echo -e " ${GREEN}v${NC}"
        fi
    done

    EXT_TAG=$(git tag -l "$glob" --sort=-version:refname | head -1)
    if [ -n "$EXT_TAG" ]; then
        echo "  Latest Tag: $EXT_TAG"
        if [ -n "$MAIN_TAG" ] && [ "$(bare_version "$EXT_TAG")" != "$(bare_version "$MAIN_TAG")" ]; then
            print_warning "$name is at $(bare_version "$EXT_TAG"), root is at $(bare_version "$MAIN_TAG")"
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        fi
    else
        echo -e "  Latest Tag: ${YELLOW}(none)${NC}"
        if [ -n "$MAIN_TAG" ]; then
            print_info "$name has never been tagged (root is at $MAIN_TAG). Not an error until the next coordinated release."
        fi
    fi

    echo ""
done

echo -e "${BLUE}=== Summary ===${NC}"
if [ "$ISSUES_FOUND" -eq 0 ]; then
    print_success "All modules are properly configured"
    exit 0
else
    print_error "Found $ISSUES_FOUND issue(s)"
    echo ""
    echo "Common fixes:"
    echo ""
    echo "1. Align Go versions:"
    echo "   cd <module>"
    echo "   go mod edit -go=$MAIN_GO_VERSION"
    echo ""
    echo "2. Replace a placeholder require with the real tag (after that module"
    echo "   has been tagged and pushed):"
    echo "   go mod edit -require=<import>@<tag>"
    echo ""
    echo "3. Release all five modules together, in the safe order:"
    echo "   ./scripts/release-modules.sh <version>"
    echo ""
    exit 1
fi
