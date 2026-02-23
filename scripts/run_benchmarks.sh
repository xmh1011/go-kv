#!/bin/bash

# go-kv Benchmark Runner
# This script runs all benchmarks and collects results into a formatted report

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
RESULTS_DIR="benchmark_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
REPORT_FILE="${RESULTS_DIR}/benchmark_report_${TIMESTAMP}.txt"
SUMMARY_FILE="${RESULTS_DIR}/benchmark_summary_${TIMESTAMP}.txt"

# Create results directory
mkdir -p "${RESULTS_DIR}"

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}  go-kv Benchmark Runner${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""

# Function to run a benchmark and save results
run_benchmark() {
    local pkg="$1"
    local name="$2"
    local extra_args="$3"

    echo -e "${YELLOW}Running benchmark: ${name}${NC}"
    echo -e "${YELLOW}Package: ${pkg}${NC}"
    echo ""

    # Run benchmark
    local output="${RESULTS_DIR}/${name}.txt"
    go test -bench="${extra_args}" -benchmem -benchtime=3s -timeout=30m \
        "${pkg}" 2>&1 | tee "${output}"

    echo ""
}

# Function to extract benchmark results
extract_results() {
    local file="$1"
    local output_file="$2"

    if [ -f "${file}" ]; then
        echo "=== $(basename ${file} .txt) ===" >> "${output_file}"
        grep -E "^(Benchmark|PASS|FAIL)" "${file}" >> "${output_file}" || true
        echo "" >> "${output_file}"
    fi
}

# Function to generate summary
generate_summary() {
    local summary_file="$1"

    echo -e "${BLUE}======================================${NC}"
    echo -e "${BLUE}  Benchmark Summary${NC}"
    echo -e "${BLUE}======================================${NC}"
    echo "" > "${summary_file}"

    echo "Benchmark Results - $(date)" >> "${summary_file}"
    echo "==========================================" >> "${summary_file}"
    echo "" >> "${summary_file}"

    # Find all benchmark result files
    for file in "${RESULTS_DIR}"/*.txt; do
        if [ "$(basename ${file})" != "$(basename ${summary_file})" ]; then
            extract_results "${file}" "${summary_file}"
        fi
    done

    cat "${summary_file}"
    echo ""
}

# Main execution
echo -e "${BLUE}Step 1: Running LSM Database Benchmarks${NC}"
echo -e "${BLUE}--------------------------------------${NC}"
run_benchmark "./engine/lsm/database/" "LSM_Database" "."

echo -e "${BLUE}Step 2: Running Raft Core Benchmarks${NC}"
echo -e "${BLUE}--------------------------------------${NC}"
run_benchmark "./raft/" "Raft_Core" "."

echo -e "${BLUE}Step 3: Running Storage Layer Benchmarks${NC}"
echo -e "${BLUE}--------------------------------------${NC}"
run_benchmark "./pkg/storage/" "Storage_Layer" "."

echo -e "${BLUE}Step 4: Running Transport Layer Benchmarks${NC}"
echo -e "${BLUE}--------------------------------------${NC}"
run_benchmark "./pkg/transport/" "Transport_Layer" "."

echo -e "${BLUE}Step 5: Running Integration Benchmarks${NC}"
echo -e "${BLUE}--------------------------------------${NC}"
run_benchmark "./tests/" "Integration" "."

# Generate summary report
echo -e "${BLUE}Step 6: Generating Summary Report${NC}"
echo -e "${BLUE}--------------------------------------${NC}"
generate_summary "${SUMMARY_FILE}"

# Generate markdown report
echo -e "${BLUE}Step 7: Generating Markdown Report${NC}"
echo -e "${BLUE}--------------------------------------${NC}"

MARKDOWN_FILE="${RESULTS_DIR}/benchmark_report_${TIMESTAMP}.md"
cat > "${MARKDOWN_FILE}" << EOF
# go-kv Benchmark Results

**Generated:** $(date)
**Test Environment:** $(uname -s) $(uname -m)

## Summary

All benchmarks were run with:
- Benchtime: 3s
- Memory profiling: Enabled
- CPU profiling: Disabled

## Results

EOF

# Parse benchmark results and format as markdown
for file in "${RESULTS_DIR}"/*.txt; do
    if [ "$(basename ${file})" != "$(basename ${summary_file})" ] && \
       [ "$(basename ${file})" != "$(basename ${MARKDOWN_FILE})" ] && \
       [ "$(basename ${file})" != "LSM_Database.txt" ] && \
       [ "$(basename ${file})" != "Raft_Core.txt" ] && \
       [ "$(basename ${file})" != "Storage_Layer.txt" ] && \
       [ "$(basename ${file})" != "Transport_Layer.txt" ] && \
       [ "$(basename ${file})" != "Integration.txt" ]; then
        continue
    fi

    if [ -f "${file}" ]; then
        echo "" >> "${MARKDOWN_FILE}"
        echo "### $(basename ${file} .txt | sed 's/_/ /g')" >> "${MARKDOWN_FILE}"
        echo "" >> "${MARKDOWN_FILE}"
        echo "\`\`\`" >> "${MARKDOWN_FILE}"
        grep "^Benchmark" "${file}" | sed 's/  */ | /g' | sed 's/ / | /2' | sed 's/ ns\/op//g' | sed 's/ B\/op//g' | sed 's/ allocs\/op//g' >> "${MARKDOWN_FILE}" || true
        echo "\`\`\`" >> "${MARKDOWN_FILE}"
    fi
done

cat >> "${MARKDOWN_FILE}" << EOF

## Performance Analysis

### Key Findings

1. **Throughput**: The system handles [X] requests/second for small key-value pairs
2. **Latency**: Average request latency is [X] ms
3. **Scalability**: Performance scales [linearly/sublinearly] with cluster size
4. **Resource Usage**: Memory allocation per operation is [X] bytes

### Recommendations

- Use InMemory transport for testing and development
- Use TCP or gRPC for production based on your requirements
- LSM storage provides good performance for write-heavy workloads

## System Information

\`\`\`
$(go version)
\`\`\`

\`\`\`
$(uname -a)
\`\`\`
EOF

echo -e "${GREEN}Benchmark results saved to:${NC}"
echo -e "  - ${REPORT_FILE}"
echo -e "  - ${SUMMARY_FILE}"
echo -e "  - ${MARKDOWN_FILE}"
echo ""

echo -e "${BLUE}======================================${NC}"
echo -e "${GREEN}All benchmarks completed successfully!${NC}"
echo -e "${BLUE}======================================${NC}"
