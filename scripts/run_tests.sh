#!/bin/bash

# go-kv Test Runner
# This script runs all tests and generates a coverage report

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
COVERAGE_FILE="coverage.txt"
COVERAGE_HTML="coverage.html"
TEST_RESULTS_DIR="test_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
SUMMARY_FILE="${TEST_RESULTS_DIR}/test_summary_${TIMESTAMP}.txt"

# Create results directory
mkdir -p "${TEST_RESULTS_DIR}"

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}  go-kv Test Runner${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""

# Step 1: Clean previous test cache
echo -e "${YELLOW}Step 1: Cleaning test cache...${NC}"
go clean -testcache
echo -e "${GREEN}Done${NC}"
echo ""

# Step 2: Run unit tests with coverage
echo -e "${YELLOW}Step 2: Running unit tests with coverage...${NC}"
echo -e "${BLUE}--------------------------------------${NC}"

# Get all packages with tests
PACKAGES=$(go list -f '{{if .TestGoFiles}}{{.ImportPath}}{{end}}' ./...)

UNIT_TEST_RESULTS="${TEST_RESULTS_DIR}/unit_test_${TIMESTAMP}.txt"

go test -race -timeout=20m -v -cover -coverprofile="${COVERAGE_FILE}" -coverpkg=./... \
    ${PACKAGES} 2>&1 | tee "${UNIT_TEST_RESULTS}"

echo ""
echo -e "${GREEN}Unit tests completed${NC}"
echo ""

# Step 3: Run integration tests
echo -e "${YELLOW}Step 3: Running integration tests...${NC}"
echo -e "${BLUE}--------------------------------------${NC}"

INTEGRATION_TEST_RESULTS="${TEST_RESULTS_DIR}/integration_test_${TIMESTAMP}.txt"

go test -race -v ./tests/... 2>&1 | tee "${INTEGRATION_TEST_RESULTS}"

echo ""
echo -e "${GREEN}Integration tests completed${NC}"
echo ""

# Step 4: Generate coverage report
echo -e "${YELLOW}Step 4: Generating coverage report...${NC}"
echo -e "${BLUE}--------------------------------------${NC}"

# Generate text coverage summary
COVERAGE_SUMMARY="${TEST_RESULTS_DIR}/coverage_summary_${TIMESTAMP}.txt"
go tool cover -func="${COVERAGE_FILE}" | tail -n 1 > "${COVERAGE_SUMMARY}"

TOTAL_COVERAGE=$(cat "${COVERAGE_SUMMARY}" | awk '{print $3}')

echo -e "${GREEN}Total coverage: ${TOTAL_COVERAGE}${NC}"
echo ""

# Generate HTML coverage report
go tool cover -html="${COVERAGE_FILE}" -o "${COVERAGE_HTML}"

echo -e "${GREEN}Coverage report generated: ${COVERAGE_HTML}${NC}"
echo ""

# Step 5: Generate test summary
echo -e "${YELLOW}Step 5: Generating test summary...${NC}"
echo -e "${BLUE}--------------------------------------${NC}"

cat > "${SUMMARY_FILE}" << EOF
========================================
  go-kv Test Summary
========================================

Generated: $(date)
Test Environment: $(uname -s) $(uname -m)

========================================
  Unit Tests
========================================

EOF

# Extract unit test statistics
UNIT_PASS=$(grep -c "^PASS" "${UNIT_TEST_RESULTS}" || echo "0")
UNIT_FAIL=$(grep -c "^FAIL" "${UNIT_TEST_RESULTS}" || echo "0")

echo "Packages tested: $(echo ${PACKAGES} | wc -w)" >> "${SUMMARY_FILE}"
echo "Passed: ${UNIT_PASS}" >> "${SUMMARY_FILE}"
echo "Failed: ${UNIT_FAIL}" >> "${SUMMARY_FILE}"
echo "" >> "${SUMMARY_FILE}"

echo "========================================
  Integration Tests
========================================

EOF

# Extract integration test statistics
INT_PASS=$(grep -c "^PASS" "${INTEGRATION_TEST_RESULTS}" || echo "0")
INT_FAIL=$(grep -c "^FAIL" "${INTEGRATION_TEST_RESULTS}" || echo "0")

echo "Test suites: $(grep -c "^--- PASS" "${INTEGRATION_TEST_RESULTS}")" >> "${SUMMARY_FILE}"
echo "Passed: ${INT_PASS}" >> "${SUMMARY_FILE}"
echo "Failed: ${INT_FAIL}" >> "${SUMMARY_FILE}"
echo "" >> "${SUMMARY_FILE}"

echo "========================================
  Coverage
========================================

EOF

cat "${COVERAGE_SUMMARY}" >> "${SUMMARY_FILE}"
echo "" >> "${SUMMARY_FILE}"

echo "========================================
  Files Generated
========================================

EOF

echo "Unit test results: ${UNIT_TEST_RESULTS}" >> "${SUMMARY_FILE}"
echo "Integration test results: ${INTEGRATION_TEST_RESULTS}" >> "${SUMMARY_FILE}"
echo "Coverage profile: ${COVERAGE_FILE}" >> "${SUMMARY_FILE}"
echo "Coverage HTML: ${COVERAGE_HTML}" >> "${SUMMARY_FILE}"

cat "${SUMMARY_FILE}"

echo ""
echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Test Summary${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""

# Check for failures
if [ "${UNIT_FAIL}" -gt 0 ] || [ "${INT_FAIL}" -gt 0 ]; then
    echo -e "${RED}Some tests failed!${NC}"
    echo -e "  Unit test failures: ${UNIT_FAIL}"
    echo -e "  Integration test failures: ${INT_FAIL}"
    exit 1
else
    echo -e "${GREEN}All tests passed successfully!${NC}"
    echo ""
    echo -e "${BLUE}Files generated:${NC}"
    echo -e "  - ${SUMMARY_FILE}"
    echo -e "  - ${UNIT_TEST_RESULTS}"
    echo -e "  - ${INTEGRATION_TEST_RESULTS}"
    echo -e "  - ${COVERAGE_FILE}"
    echo -e "  - ${COVERAGE_HTML}"
fi

echo ""
echo -e "${BLUE}======================================${NC}"
