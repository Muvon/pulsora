#!/bin/bash

# Pulsora Benchmarking Script
# Runs comprehensive performance tests and generates reports

set -e

echo "🚀 Pulsora Performance Benchmarking Suite"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create benchmark results directory
RESULTS_DIR="benchmark_results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo -e "${BLUE}📊 Results will be saved to: $RESULTS_DIR${NC}"

# System information
echo -e "\n${YELLOW}📋 System Information${NC}"
echo "===================="
echo "OS: $(uname -s) $(uname -r)"
echo "CPU: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
echo "Memory: $(sysctl -n hw.memsize 2>/dev/null | awk '{print $1/1024/1024/1024 " GB"}' || grep MemTotal /proc/meminfo | awk '{print $2/1024/1024 " GB"}')"
echo "Rust Version: $(rustc --version)"
echo "Cargo Version: $(cargo --version)"

# Build in release mode
echo -e "\n${YELLOW}🔨 Building in release mode...${NC}"
cargo build --release

# Run individual benchmark suites
echo -e "\n${YELLOW}🏃 Running Ingestion Benchmarks...${NC}"
cargo bench --bench ingestion -- --output-format html > "$RESULTS_DIR/ingestion_output.txt" 2>&1 || true

echo -e "\n${YELLOW}🔍 Running Query Benchmarks...${NC}"
cargo bench --bench query -- --output-format html > "$RESULTS_DIR/query_output.txt" 2>&1 || true

echo -e "\n${YELLOW}🌐 Running End-to-End Benchmarks...${NC}"
cargo bench --bench end_to_end -- --output-format html > "$RESULTS_DIR/end_to_end_output.txt" 2>&1 || true

# Run all benchmarks and save detailed results
echo -e "\n${YELLOW}📈 Running Complete Benchmark Suite...${NC}"
cargo bench > "$RESULTS_DIR/complete_results.txt" 2>&1 || true

# Generate summary report
echo -e "\n${YELLOW}📝 Generating Summary Report...${NC}"
cat > "$RESULTS_DIR/benchmark_summary.md" << EOF
# Pulsora Benchmark Results

**Date:** $(date)
**System:** $(uname -s) $(uname -r)
**Rust Version:** $(rustc --version)

## System Specifications

- **CPU:** $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)
- **Memory:** $(sysctl -n hw.memsize 2>/dev/null | awk '{print $1/1024/1024/1024 " GB"}' || grep MemTotal /proc/meminfo | awk '{print $2/1024/1024 " GB"}')
- **Storage:** $(df -h . | tail -1 | awk '{print $2 " total, " $4 " available"}')

## Benchmark Categories

### 1. Ingestion Performance
- CSV parsing speed
- Batch insertion throughput
- Full ingestion pipeline
- Batch size optimization

### 2. Query Performance  
- Time-range queries
- Pagination efficiency
- Result set scaling
- Concurrent read performance

### 3. End-to-End Performance
- HTTP API throughput
- Mixed workload simulation
- Multi-table scaling
- Data type handling

## Key Metrics

The benchmarks measure:
- **Throughput:** Operations per second
- **Latency:** Time per operation
- **Memory Usage:** Peak memory consumption
- **Scalability:** Performance across different data sizes

## Files Generated

- \`complete_results.txt\` - Full benchmark output
- \`ingestion_output.txt\` - Ingestion benchmark details
- \`query_output.txt\` - Query benchmark details  
- \`end_to_end_output.txt\` - End-to-end benchmark details
- HTML reports in \`target/criterion/\` directory

## Usage

To run benchmarks yourself:

\`\`\`bash
# Run all benchmarks
cargo bench

# Run specific benchmark suite
cargo bench --bench ingestion
cargo bench --bench query
cargo bench --bench end_to_end

# Run with custom parameters
cargo bench -- --sample-size 50
\`\`\`

## Performance Targets

Based on these benchmarks, Pulsora aims for:
- **Ingestion:** 100K+ rows/second for typical market data
- **Queries:** Sub-millisecond response for time-range queries
- **Memory:** Predictable usage under configured limits
- **Concurrency:** Linear scaling up to CPU core count
EOF

# Copy Criterion HTML reports if they exist
if [ -d "target/criterion" ]; then
    echo -e "\n${YELLOW}📊 Copying HTML reports...${NC}"
    cp -r target/criterion "$RESULTS_DIR/" 2>/dev/null || true
fi

# Performance comparison (if previous results exist)
PREV_RESULTS=$(find benchmark_results -name "complete_results.txt" -not -path "$RESULTS_DIR/*" | head -1)
if [ -n "$PREV_RESULTS" ]; then
    echo -e "\n${YELLOW}📊 Comparing with previous results...${NC}"
    echo "Previous results: $PREV_RESULTS"
    echo "Current results: $RESULTS_DIR/complete_results.txt"
    # Add comparison logic here if needed
fi

echo -e "\n${GREEN}✅ Benchmarking Complete!${NC}"
echo -e "${GREEN}📁 Results saved to: $RESULTS_DIR${NC}"
echo -e "${GREEN}📊 View HTML reports: open $RESULTS_DIR/criterion/report/index.html${NC}"

# Quick performance summary
echo -e "\n${BLUE}🎯 Quick Performance Summary${NC}"
echo "=============================="
if [ -f "$RESULTS_DIR/complete_results.txt" ]; then
    echo "Extracting key metrics from benchmark results..."
    grep -E "(time:|throughput:)" "$RESULTS_DIR/complete_results.txt" | head -10 || echo "No timing data found in results"
else
    echo "No benchmark results file found"
fi

echo -e "\n${YELLOW}💡 Next Steps:${NC}"
echo "- Review detailed results in $RESULTS_DIR/"
echo "- Open HTML reports for interactive charts"
echo "- Compare with previous benchmark runs"
echo "- Tune configuration based on results"
echo "- Run production load tests"