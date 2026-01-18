#!/bin/bash
###############################################################################
# Run All Memory Profiling Scenarios
#
# This script runs all predefined scenarios sequentially and generates a
# comparison report. Useful for regression testing memory optimizations.
#
# Usage:
#   ./run-all-scenarios.sh [MIN_HEAP] [MAX_HEAP]
#
# Example:
#   ./run-all-scenarios.sh 512m 2g
###############################################################################

set -euo pipefail

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROFILING_DIR="$(dirname "$SCRIPT_DIR")"
MIN_HEAP="${1:-512m}"
MAX_HEAP="${2:-2g}"
RESULTS_DIR="$PROFILING_DIR/results"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
COMPARISON_REPORT="$RESULTS_DIR/$TIMESTAMP-comparison-report.txt"

# Scenarios to run (in order of increasing load)
SCENARIOS=(
    "baseline-http.yaml"
    "bounded-buffer-test.yaml"
    "high-throughput-http.yaml"
    "large-batch-http.yaml"
    "sustained-load-http.yaml"
)

# Optional: Add stress test if heap is large enough
if [[ "$MAX_HEAP" =~ ^([0-9]+)g$ ]] && [[ ${BASH_REMATCH[1]} -ge 2 ]]; then
    SCENARIOS+=("stress-test-http.yaml")
fi

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "================================================================================"
echo "  Memory Profiling - Run All Scenarios"
echo "================================================================================"
echo ""
echo "Configuration:"
echo "  Min Heap:   $MIN_HEAP"
echo "  Max Heap:   $MAX_HEAP"
echo "  Scenarios:  ${#SCENARIOS[@]}"
echo "  Results:    $RESULTS_DIR"
echo ""

# Initialize comparison report
cat > "$COMPARISON_REPORT" << EOF
================================================================================
Memory Profiling Comparison Report
================================================================================

Generated: $(date '+%Y-%m-%d %H:%M:%S')
Configuration:
  Min Heap: $MIN_HEAP
  Max Heap: $MAX_HEAP

Scenarios Run:
EOF

for scenario in "${SCENARIOS[@]}"; do
    echo "  - $scenario" >> "$COMPARISON_REPORT"
done

cat >> "$COMPARISON_REPORT" << EOF

================================================================================
Results Summary
================================================================================

EOF

# Track results
TOTAL_SCENARIOS=${#SCENARIOS[@]}
SUCCESSFUL=0
FAILED=0

# Run each scenario
for i in "${!SCENARIOS[@]}"; do
    scenario="${SCENARIOS[$i]}"
    scenario_num=$((i + 1))

    echo ""
    echo "--------------------------------------------------------------------------------"
    echo "Scenario $scenario_num/$TOTAL_SCENARIOS: $scenario"
    echo "--------------------------------------------------------------------------------"
    echo ""

    scenario_path="$PROFILING_DIR/scenarios/$scenario"

    if [[ ! -f "$scenario_path" ]]; then
        log_warning "Scenario file not found: $scenario_path"
        FAILED=$((FAILED + 1))
        continue
    fi

    # Run the scenario
    if "$SCRIPT_DIR/run-memory-profile.sh" "$scenario_path" "$MIN_HEAP" "$MAX_HEAP" --no-gc-logging; then
        log_success "Scenario completed: $scenario"
        SUCCESSFUL=$((SUCCESSFUL + 1))

        # Extract key metrics from the latest report
        latest_report=$(ls -t "$RESULTS_DIR"/*-memory-report.txt 2>/dev/null | head -1)
        if [[ -f "$latest_report" ]]; then
            # Parse and append to comparison report
            cat >> "$COMPARISON_REPORT" << EOF

$scenario
$(echo "$scenario" | sed 's/./=/g')
EOF
            # Extract key lines from the report
            grep -E "(Total Requests|Throughput|Total Data|Avg Latency|Errors|Duration)" "$latest_report" >> "$COMPARISON_REPORT" 2>/dev/null || true
        fi
    else
        log_warning "Scenario failed: $scenario"
        FAILED=$((FAILED + 1))

        cat >> "$COMPARISON_REPORT" << EOF

$scenario
$(echo "$scenario" | sed 's/./=/g')
Status: FAILED

EOF
    fi

    # Brief pause between scenarios
    if [[ $scenario_num -lt $TOTAL_SCENARIOS ]]; then
        sleep 2
    fi
done

# Finalize comparison report
cat >> "$COMPARISON_REPORT" << EOF

================================================================================
Summary
================================================================================

Total Scenarios:     $TOTAL_SCENARIOS
Successful:          $SUCCESSFUL
Failed:              $FAILED

Results Directory:   $RESULTS_DIR

================================================================================
EOF

# Print final summary
echo ""
echo "================================================================================"
echo "  All Scenarios Complete"
echo "================================================================================"
echo ""
cat "$COMPARISON_REPORT"

if [[ $FAILED -eq 0 ]]; then
    log_success "All scenarios passed successfully!"
    exit 0
else
    log_warning "$FAILED scenario(s) failed"
    exit 1
fi
