#!/bin/bash

# Script to fetch PDFs from IPFS gateway based on CID list
# Usage: ./fetch_cids.sh [gateway_url]

# Don't exit on error - we want to continue processing even if one fails
set +e

# Configuration
CID_LIST="$(dirname "$0")/cid-list.txt"
GATEWAY="${1:-https://ipfs.io/ipfs/}"
OUTPUT_DIR="$(dirname "$0")/../mardi-pdfs"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# Check if CID list exists
if [ ! -f "$CID_LIST" ]; then
    echo -e "${RED}Error: CID list file '$CID_LIST' not found${NC}"
    exit 1
fi

# Count total CIDs
TOTAL=$(grep -v '^[[:space:]]*$' "$CID_LIST" | wc -l)
echo -e "${GREEN}Found $TOTAL CIDs to fetch${NC}"
echo -e "${YELLOW}Using gateway: $GATEWAY${NC}"
echo ""

# Counters
SUCCESS=0
SKIPPED=0
FAILED=0

# Process each CID
while IFS= read -r cid || [ -n "$cid" ]; do
    # Skip empty lines
    if [ -z "$cid" ]; then
        continue
    fi
    
    # Trim whitespace
    cid=$(echo "$cid" | xargs)
    
    # Skip if empty after trim
    if [ -z "$cid" ]; then
        continue
    fi
    
    # Construct output filename
    OUTPUT_FILE="${OUTPUT_DIR}/${cid}.pdf"
    
    # Skip if already exists
    if [ -f "$OUTPUT_FILE" ]; then
        CURRENT=$((SUCCESS + SKIPPED + FAILED + 1))
        echo -e "${YELLOW}[$CURRENT/$TOTAL] Skipping $cid (already exists)${NC}"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi
    
    # Construct gateway URL
    URL="${GATEWAY}${cid}"
    
    # Fetch the file
    CURRENT=$((SUCCESS + SKIPPED + FAILED + 1))
    echo -n "[$CURRENT/$TOTAL] Fetching $cid... "
    
    if curl -s -f -L --max-time 60 --retry 2 -o "$OUTPUT_FILE" "$URL" 2>/dev/null; then
        # Check if file was downloaded and is not empty
        if [ -s "$OUTPUT_FILE" ]; then
            # Verify it's a PDF (check magic bytes)
            if file "$OUTPUT_FILE" 2>/dev/null | grep -q "PDF"; then
                echo -e "${GREEN}✓ Success${NC}"
                SUCCESS=$((SUCCESS + 1))
            else
                echo -e "${YELLOW}⚠ Downloaded but not a PDF (keeping file)${NC}"
                SUCCESS=$((SUCCESS + 1))
            fi
        else
            echo -e "${RED}✗ Empty file${NC}"
            rm -f "$OUTPUT_FILE"
            FAILED=$((FAILED + 1))
        fi
    else
        echo -e "${RED}✗ Failed${NC}"
        rm -f "$OUTPUT_FILE" 2>/dev/null
        FAILED=$((FAILED + 1))
    fi
    
    # Small delay to avoid overwhelming the gateway
    sleep 0.5
    
done < "$CID_LIST"

# Summary
echo ""
echo -e "${GREEN}=== Summary ===${NC}"
echo -e "Total:    $TOTAL"
echo -e "${GREEN}Success:  $SUCCESS${NC}"
echo -e "${YELLOW}Skipped:  $SKIPPED${NC}"
echo -e "${RED}Failed:   $FAILED${NC}"

if [ $FAILED -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}Note: Some downloads failed. You can re-run this script to retry failed downloads.${NC}"
    exit 1
fi
