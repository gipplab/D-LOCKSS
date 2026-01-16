#!/bin/bash

# Quick test script for D-LOCKSS
# Tests basic functionality and new features

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== D-LOCKSS Quick Test ===${NC}"

# Test 1: Build
echo -e "\n${YELLOW}Test 1: Building application...${NC}"
go build -o dlockss-node ./main.go
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Build successful${NC}"
else
    echo -e "${RED}✗ Build failed${NC}"
    exit 1
fi

# Test 2: Configuration
echo -e "\n${YELLOW}Test 2: Testing configuration...${NC}"
DLOCKSS_MIN_REPLICATION=7 \
DLOCKSS_DATA_DIR=./test_data \
timeout 3 ./dlockss-node > test_config.log 2>&1 || true

if grep -q "\[Config\] Replication: 7-10" test_config.log; then
    echo -e "${GREEN}✓ Configuration test passed${NC}"
else
    echo -e "${RED}✗ Configuration test failed${NC}"
    cat test_config.log
    exit 1
fi

# Test 3: File ingestion
echo -e "\n${YELLOW}Test 3: Testing file ingestion...${NC}"
mkdir -p test_data
echo "test content" > test_data/test.txt

DLOCKSS_DATA_DIR=./test_data \
DLOCKSS_CHECK_INTERVAL=1s \
timeout 5 ./dlockss-node > test_ingestion.log 2>&1 &
NODE_PID=$!
sleep 3
kill $NODE_PID 2>/dev/null || true
wait $NODE_PID 2>/dev/null || true

if grep -q "Pinned file" test_ingestion.log; then
    echo -e "${GREEN}✓ File ingestion test passed${NC}"
else
    echo -e "${RED}✗ File ingestion test failed${NC}"
    cat test_ingestion.log | tail -20
    exit 1
fi

# Test 4: Error handling
echo -e "\n${YELLOW}Test 4: Testing error handling...${NC}"
DLOCKSS_CHECK_INTERVAL=invalid_value \
timeout 2 ./dlockss-node > test_error.log 2>&1 || true

if grep -q "Invalid duration value" test_error.log; then
    echo -e "${GREEN}✓ Error handling test passed${NC}"
else
    echo -e "${RED}✗ Error handling test failed${NC}"
    cat test_error.log
    exit 1
fi

# Cleanup
echo -e "\n${YELLOW}Cleaning up...${NC}"
rm -rf test_data test_*.log
rm -f dlockss-node

echo -e "\n${GREEN}=== All tests passed! ===${NC}"
