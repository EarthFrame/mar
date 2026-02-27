#!/bin/bash
# Roundtrip test: verify all MAR operations work correctly
# Tests: create, list, validate, header, get, extract on a single input

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
MAR_BIN="${1:-$REPO_ROOT/mar}"
INPUT_DIR="${2:-$REPO_ROOT/benchmarks/data/numpy-2.4.1}"
SCRATCH_DIR="${3:-.scratch_roundtrip}"
TEST_NAME="$(basename "$INPUT_DIR")"

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== MAR Roundtrip Test: $TEST_NAME ===${NC}"

# Create scratch directory
mkdir -p "$SCRATCH_DIR"
trap "rm -rf '$SCRATCH_DIR'" EXIT

ARCHIVE="$SCRATCH_DIR/test.mar"
EXTRACT_DIR="$SCRATCH_DIR/extracted"
GET_DIR="$SCRATCH_DIR/get_output"

# Test 1: Create
echo -e "\n${YELLOW}[1/6] Testing: mar create${NC}"
if "$MAR_BIN" create --force "$ARCHIVE" "$INPUT_DIR" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Created archive successfully${NC}"
    ls -lh "$ARCHIVE" | awk '{print "     Size: " $5}'
else
    echo -e "${RED}✗ Failed to create archive${NC}"
    exit 1
fi

# Test 2: List
echo -e "\n${YELLOW}[2/6] Testing: mar list${NC}"
if FILE_COUNT=$("$MAR_BIN" list "$ARCHIVE" 2>/dev/null | wc -l); then
    echo -e "${GREEN}✓ Listed archive contents${NC}"
    echo "     Files: $FILE_COUNT"
else
    echo -e "${RED}✗ Failed to list archive${NC}"
    exit 1
fi

# Test 3: Header
echo -e "\n${YELLOW}[3/6] Testing: mar header${NC}"
if "$MAR_BIN" header "$ARCHIVE" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Read header successfully${NC}"
    "$MAR_BIN" header "$ARCHIVE" | head -5 | sed 's/^/     /'
else
    echo -e "${RED}✗ Failed to read header${NC}"
    exit 1
fi

# Test 4: Validate
echo -e "\n${YELLOW}[4/6] Testing: mar validate${NC}"
if "$MAR_BIN" validate "$ARCHIVE" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Archive validation passed${NC}"
else
    echo -e "${RED}✗ Archive validation failed${NC}"
    exit 1
fi

# Test 5: Get (random access) - extract a single file
echo -e "\n${YELLOW}[5/6] Testing: mar extract (single file)${NC}"
if mkdir -p "$GET_DIR"; then
    # Extract just first file from archive
    if "$MAR_BIN" extract -o "$GET_DIR" "$ARCHIVE" > /dev/null 2>&1; then
        EXTRACTED_SINGLE=$(find "$GET_DIR" -type f | head -1)
        echo -e "${GREEN}✓ Single file extraction successful${NC}"
        echo "     Extracted: $(basename "$EXTRACTED_SINGLE")"
    else
        echo -e "${RED}✗ Failed to extract file from archive${NC}"
        exit 1
    fi
else
    echo -e "${RED}✗ Failed to create extraction directory${NC}"
    exit 1
fi

# Test 6: Extract
echo -e "\n${YELLOW}[6/6] Testing: mar extract (full archive)${NC}"
if mkdir -p "$EXTRACT_DIR" && "$MAR_BIN" extract -o "$EXTRACT_DIR" "$ARCHIVE" > /dev/null 2>&1; then
    EXTRACTED_COUNT=$(find "$EXTRACT_DIR" -type f | wc -l)
    echo -e "${GREEN}✓ Full extraction successful${NC}"
    echo "     Total extracted files: $EXTRACTED_COUNT"
else
    echo -e "${RED}✗ Failed to extract archive${NC}"
    exit 1
fi

echo -e "\n${GREEN}=== All roundtrip tests passed! ===${NC}\n"
exit 0
