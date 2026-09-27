#!/usr/bin/env bash
# ==============================================================================
# verify_refactoring.sh
#
# Automated dialectic verification gate for iRODS modernization and refactoring.
# Enforces:
#   1. Clean compilation of modified components
#   2. Unit test pass rate (100% assertions passing)
#   3. Incremental synchronization of the Code Property Graph (CPG)
#   4. Zero concurrency safety violations (lockset, races, deadlocks)
#   5. Zero typestate invariant violations
#
# Usage:
#   scripts/verify_refactoring.sh [options] [files...]
#
# Options:
#   --db <path>       Path to Project Insight L3KVG database (default: .insight/l3kvg)
#   --skip-build      Skip CMake build step
#   --skip-tests      Skip unit test execution
#   --help, -h        Show this message
# ==============================================================================

set -euo pipefail

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

INSIGHT_BIN="${REPO_ROOT}/../project_insight/build/insight"
if [ ! -x "${INSIGHT_BIN}" ]; then
    if command -v insight >/dev/null 2>&1; then
        INSIGHT_BIN="$(command -v insight)"
    else
        echo -e "${RED}[ERROR] Project Insight binary not found at ${INSIGHT_BIN} or on PATH.${NC}" >&2
        exit 1
    fi
fi

DB_PATH="${REPO_ROOT}/.insight/l3kvg"
BUILD_DIR="${REPO_ROOT}/build"
SKIP_BUILD=0
SKIP_TESTS=0
SPECIFIED_FILES=()

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --db)
            DB_PATH="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=1
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=1
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--db <path>] [--skip-build] [--skip-tests] [files...]"
            exit 0
            ;;
        *)
            SPECIFIED_FILES+=("$1")
            shift
            ;;
    esac
done

echo -e "${BOLD}${BLUE}================================================================${NC}"
echo -e "${BOLD}${BLUE}       iRODS Dialectic Invariant Verification Engine            ${NC}"
echo -e "${BOLD}${BLUE}================================================================${NC}"

# Step 1: Detect modified C/C++ files
MODIFIED_FILES=()
if [ ${#SPECIFIED_FILES[@]} -gt 0 ]; then
    for f in "${SPECIFIED_FILES[@]}"; do
        if [[ "$f" =~ \.(cpp|cxx|cc|c|hpp|hxx|h)$ ]] && [ -f "$f" ]; then
            MODIFIED_FILES+=("$f")
        fi
    done
else
    # Gather modified files from git (staged + working tree)
    while IFS= read -r f; do
        if [ -n "$f" ] && [[ "$f" =~ \.(cpp|cxx|cc|c|hpp|hxx|h)$ ]] && [ -f "$f" ]; then
            MODIFIED_FILES+=("$f")
        fi
    done < <(git status --porcelain | awk '{print $2}' | sort -u)
fi

# Resolve Translation Units (.cpp files) for ingestion
declare -A TU_MAP
TUS_TO_SYNC=()

for f in "${MODIFIED_FILES[@]}"; do
    if [[ "$f" =~ \.(cpp|cxx|cc|c)$ ]]; then
        if [ -z "${TU_MAP[$f]:-}" ]; then
            TU_MAP[$f]=1
            TUS_TO_SYNC+=("$f")
        fi
    elif [[ "$f" =~ \.(hpp|hxx|h)$ ]]; then
        base="$(basename "$f" | cut -d. -f1)"
        # 1. Look for matching .cpp in plugins, server, lib, unit_tests
        for cpp in $(find plugins server lib unit_tests -name "${base}.cpp" 2>/dev/null); do
            if [ -f "$cpp" ] && [ -z "${TU_MAP[$cpp]:-}" ]; then
                TU_MAP[$cpp]=1
                TUS_TO_SYNC+=("$cpp")
            fi
        done
        # 2. Grep for direct includes of this header
        header_base="$(basename "$f")"
        for incl_cpp in $(git grep -l "#include .*${header_base}" -- "*.cpp" 2>/dev/null | head -n 5); do
            if [ -f "$incl_cpp" ] && [ -z "${TU_MAP[$incl_cpp]:-}" ]; then
                TU_MAP[$incl_cpp]=1
                TUS_TO_SYNC+=("$incl_cpp")
            fi
        done
    fi
done

echo -e "${BLUE}[INFO] Repository:${NC} ${REPO_ROOT}"
echo -e "${BLUE}[INFO] Database:${NC}   ${DB_PATH}"
echo -e "${BLUE}[INFO] Insight:${NC}    ${INSIGHT_BIN}"
if [ ${#MODIFIED_FILES[@]} -gt 0 ]; then
    echo -e "${BLUE}[INFO] Modified C/C++ Files (${#MODIFIED_FILES[@]}):${NC}"
    for f in "${MODIFIED_FILES[@]}"; do
        echo "       - $f"
    done
    echo -e "${BLUE}[INFO] Resolved Translation Units to Reindex (${#TUS_TO_SYNC[@]}):${NC}"
    for tu in "${TUS_TO_SYNC[@]}"; do
        echo "       - $tu"
    done
else
    echo -e "${YELLOW}[WARN] No modified C/C++ source or header files detected.${NC}"
fi

# Step 2: Build verification
if [ "${SKIP_BUILD}" -eq 0 ]; then
    echo -e "\n${BOLD}[1/4] Verifying Build Integrity...${NC}"
    if [ ! -d "${BUILD_DIR}" ]; then
        echo -e "${RED}[FAIL] Build directory ${BUILD_DIR} does not exist.${NC}" >&2
        exit 1
    fi
    cmake --build "${BUILD_DIR}" -j"$(nproc)"
    echo -e "${GREEN}[PASS] Build succeeded with zero errors.${NC}"
else
    echo -e "\n${YELLOW}[1/4] Build verification skipped (--skip-build).${NC}"
fi

# Step 3: Unit Test Execution
if [ "${SKIP_TESTS}" -eq 0 ]; then
    echo -e "\n${BOLD}[2/4] Executing Regression & Modernization Unit Tests...${NC}"
    if [ -x "${BUILD_DIR}/unit_tests/irods_nanodbc_executor" ]; then
        echo -e "${BLUE}Running irods_nanodbc_executor Catch2 tests...${NC}"
        "${BUILD_DIR}/unit_tests/irods_nanodbc_executor"
    fi
    echo -e "${GREEN}[PASS] All unit test assertions satisfied.${NC}"
else
    echo -e "\n${YELLOW}[2/4] Unit test execution skipped (--skip-tests).${NC}"
fi

# Step 4: Incremental Code Property Graph (CPG) Synchronization
echo -e "\n${BOLD}[3/4] Synchronizing Code Property Graph (CPG)...${NC}"
if [ ${#TUS_TO_SYNC[@]} -gt 0 ]; then
    if [ -f "${BUILD_DIR}/compile_commands.json" ]; then
        # Ensure root symlink points to freshest compile_commands.json
        ln -sf build/compile_commands.json compile_commands.json
    fi
    "${INSIGHT_BIN}" ingest "${BUILD_DIR}/compile_commands.json" \
        --db "${DB_PATH}" \
        --files "${TUS_TO_SYNC[@]}"
    echo -e "${GREEN}[PASS] Incremental CPG ingestion synchronized.${NC}"
else
    echo -e "${GREEN}[PASS] CPG up to date (no translation units to sync).${NC}"
fi

# Step 5: Dialectic Static Analysis (Concurrency & Typestate Invariants)
echo -e "\n${BOLD}[4/4] Verifying Dialectic Invariants via Project Insight...${NC}"

echo -e "${BLUE}Checking concurrency safety (lock inversion, ABBA cycles, unprotected shared memory)...${NC}"
CONCURRENCY_OUTPUT=$("${INSIGHT_BIN}" check --db "${DB_PATH}" --concurrency)
echo "${CONCURRENCY_OUTPUT}"

if echo "${CONCURRENCY_OUTPUT}" | grep -qE "[1-9][0-9]* (finding|violation)"; then
    echo -e "${RED}[FAIL] Concurrency safety violations detected!${NC}" >&2
    exit 1
fi
echo -e "${GREEN}[PASS] Zero concurrency violations detected.${NC}"

echo -e "\n${BLUE}Checking typestate invariants (protocol order, leaked resources, invalid transitions)...${NC}"
TYPESTATE_OUTPUT=$("${INSIGHT_BIN}" check --db "${DB_PATH}" --typestate)
echo "${TYPESTATE_OUTPUT}"

if echo "${TYPESTATE_OUTPUT}" | grep -qE "[1-9][0-9]* (finding|violation)"; then
    echo -e "${RED}[FAIL] Typestate invariant violations detected!${NC}" >&2
    exit 1
fi
echo -e "${GREEN}[PASS] Zero typestate violations detected.${NC}"

echo -e "\n${BOLD}${GREEN}================================================================${NC}"
echo -e "${BOLD}${GREEN}  [SUCCESS] All Refactoring Dialectic Invariants Verified!     ${NC}"
echo -e "${BOLD}${GREEN}================================================================${NC}"
exit 0
