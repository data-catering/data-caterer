#!/bin/bash

###############################################################################
# Memory Profiling Script for Data Caterer
#
# This script orchestrates memory profiling for Data Caterer HTTP streaming:
# 1. Builds Data Caterer JAR
# 2. Starts HTTP test server
# 3. Configures JVM profiling
# 4. Runs Data Caterer with specified scenario
# 5. Collects and reports memory metrics
#
# Usage:
#   ./run-memory-profile.sh [YAML_FILE] [MIN_HEAP] [MAX_HEAP] [OPTIONS]
#
# Examples:
#   ./run-memory-profile.sh
#   ./run-memory-profile.sh scenarios/high-throughput-http.yaml
#   ./run-memory-profile.sh scenarios/stress-test.yaml 1g 2g --oom-dump
###############################################################################

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROFILING_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(cd "$PROFILING_DIR/../.." && pwd)"

# Default configuration
YAML_FILE="${1:-$PROFILING_DIR/scenarios/baseline-http.yaml}"
# Convert to absolute path if relative
if [[ ! "$YAML_FILE" = /* ]]; then
    YAML_FILE="$(cd "$(dirname "$YAML_FILE")" && pwd)/$(basename "$YAML_FILE")"
fi
MIN_HEAP="${2:-512m}"
MAX_HEAP="${3:-2g}"
HTTP_PORT="${HTTP_PORT:-8080}"
RESULTS_DIR="$PROFILING_DIR/results"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"

# Profiling options
ENABLE_OOM_DUMP=false
ENABLE_FLIGHT_RECORDER=false
ENABLE_GC_LOGGING=true
PROFILER_TOOL=""
KEEP_SERVER=false
REBUILD=false

# Parse additional arguments
shift 3 2>/dev/null || true
while [[ $# -gt 0 ]]; do
    case $1 in
        --oom-dump)
            ENABLE_OOM_DUMP=true
            shift
            ;;
        --flight-recorder)
            ENABLE_FLIGHT_RECORDER=true
            shift
            ;;
        --no-gc-logging)
            ENABLE_GC_LOGGING=false
            shift
            ;;
        --profiler)
            PROFILER_TOOL="$2"
            shift 2
            ;;
        --port)
            HTTP_PORT="$2"
            shift 2
            ;;
        --rebuild)
            REBUILD=true
            shift
            ;;
        --keep-server)
            KEEP_SERVER=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Output files
REPORT_FILE="$RESULTS_DIR/$TIMESTAMP-memory-report.txt"
GC_LOG_FILE="$RESULTS_DIR/$TIMESTAMP-gc.log"
HEAP_DUMP_FILE="$RESULTS_DIR/$TIMESTAMP-heap-dump.hprof"
JFR_FILE="$RESULTS_DIR/$TIMESTAMP-recording.jfr"
HTTP_LOG_FILE="$RESULTS_DIR/$TIMESTAMP-http-server.log"

# Create results directory
mkdir -p "$RESULTS_DIR"

###############################################################################
# Helper Functions
###############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" >&2
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

cleanup() {
    log_info "Cleaning up..."

    # Stop memory sampler
    if [[ -f "$PROFILING_DIR/.memory-sampler.pid" ]]; then
        local pid=$(cat "$PROFILING_DIR/.memory-sampler.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "Stopping memory sampler (PID: $pid)"
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
        rm -f "$PROFILING_DIR/.memory-sampler.pid"
    fi

    # Stop HTTP server
    if [[ -f "$PROFILING_DIR/.http-server.pid" ]]; then
        local pid=$(cat "$PROFILING_DIR/.http-server.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "Stopping HTTP server (PID: $pid)"
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
        rm -f "$PROFILING_DIR/.http-server.pid"
    fi

    # Stop Data Caterer if still running
    if [[ -f "$PROFILING_DIR/.data-caterer.pid" ]]; then
        local pid=$(cat "$PROFILING_DIR/.data-caterer.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "Stopping Data Caterer (PID: $pid)"
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
        rm -f "$PROFILING_DIR/.data-caterer.pid"
    fi
}

trap cleanup EXIT INT TERM

check_dependencies() {
    log_info "Checking dependencies..."

    # Check Java
    if ! command -v java &> /dev/null; then
        log_error "Java not found. Please install Java 11 or higher."
        exit 1
    fi

    local java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
    if [[ "$java_version" -lt 11 ]]; then
        log_error "Java 11 or higher required. Found: $java_version"
        exit 1
    fi

    log_success "Java version: $(java -version 2>&1 | head -n 1)"

    # Check Python3
    if ! command -v python3 &> /dev/null; then
        log_error "Python3 not found. Please install Python 3.6 or higher."
        exit 1
    fi

    log_success "Python3 version: $(python3 --version)"

    # Check psutil for HTTP server
    if ! python3 -c "import psutil" 2>/dev/null; then
        log_warning "Python psutil not found. Installing..."
        pip3 install psutil
    fi

    # Check Gradle
    if [[ ! -x "$PROJECT_ROOT/gradlew" ]]; then
        log_error "Gradle wrapper not found at $PROJECT_ROOT/gradlew"
        exit 1
    fi

    log_success "All dependencies satisfied"
}

build_data_caterer() {
    if [[ "$REBUILD" != "true" ]]; then
        log_info "Checking if Data Caterer jar already exists..."
        
        if [[ -f "$PROJECT_ROOT/app/build/libs/data-caterer.jar" ]]; then
            log_success "Data Caterer jar already exists"
            echo "$PROJECT_ROOT/app/build/libs/data-caterer.jar"
            return
        fi
    fi

    log_info "Building Data Caterer..."

    cd "$PROJECT_ROOT"

    if ./gradlew :app:shadowJar -x test --no-daemon >> "$RESULTS_DIR/$TIMESTAMP-build.log" 2>&1; then
        log_success "Shadow JAR created"
    else
        log_error "Shadow JAR creation failed. Check $RESULTS_DIR/$TIMESTAMP-build.log"
        exit 1
    fi

    # Find the JAR (look for data-caterer.jar or *-all.jar)
    local jar_file=$(find "$PROJECT_ROOT/app/build/libs" -name "data-caterer.jar" -o -name "*-all.jar" | head -n 1)

    if [[ -z "$jar_file" || ! -f "$jar_file" ]]; then
        log_error "Could not find shadow JAR in $PROJECT_ROOT/app/build/libs"
        log_error "Expected: data-caterer.jar or *-all.jar"
        ls -lh "$PROJECT_ROOT/app/build/libs/" || true
        exit 1
    fi

    log_success "Data Caterer built: $jar_file"
    echo "$jar_file"
}

start_http_server() {
    log_info "Starting HTTP test server on port $HTTP_PORT..."

    local server_script="$PROFILING_DIR/http-server/simple-http-server.py"

    if [[ ! -f "$server_script" ]]; then
        log_error "HTTP server script not found: $server_script"
        exit 1
    fi

    # Make sure port is free
    if lsof -Pi :$HTTP_PORT -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        log_error "Port $HTTP_PORT is already in use"
        exit 1
    fi

    # Start server in background
    python3 "$server_script" --port "$HTTP_PORT" --log-level info > "$HTTP_LOG_FILE" 2>&1 &
    local pid=$!
    echo "$pid" > "$PROFILING_DIR/.http-server.pid"

    # Wait for server to start
    sleep 2

    # Check if server is running
    if ! ps -p "$pid" > /dev/null 2>&1; then
        log_error "HTTP server failed to start. Check $HTTP_LOG_FILE"
        exit 1
    fi

    # Test health endpoint
    if curl -s "http://localhost:$HTTP_PORT/health" > /dev/null; then
        log_success "HTTP server started (PID: $pid)"
    else
        log_error "HTTP server not responding on port $HTTP_PORT"
        exit 1
    fi
}

build_jvm_options() {
    local options=""

    # Heap settings
    options="$options -Xms$MIN_HEAP -Xmx$MAX_HEAP"

    # Java 11+ module access for Spark
    options="$options --add-opens=java.base/java.lang=ALL-UNNAMED"
    options="$options --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
    options="$options --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
    options="$options --add-opens=java.base/java.io=ALL-UNNAMED"
    options="$options --add-opens=java.base/java.net=ALL-UNNAMED"
    options="$options --add-opens=java.base/java.nio=ALL-UNNAMED"
    options="$options --add-opens=java.base/java.util=ALL-UNNAMED"
    options="$options --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
    options="$options --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
    options="$options --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    options="$options --add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
    options="$options --add-opens=java.base/sun.security.action=ALL-UNNAMED"
    options="$options --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    options="$options --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

    # GC settings (G1GC for better performance)
    options="$options -XX:+UseG1GC"

    # GC logging
    if [[ "$ENABLE_GC_LOGGING" == "true" ]]; then
        options="$options -Xlog:gc*:file=$GC_LOG_FILE:time,uptime,level,tags"
    fi

    # Heap dump on OOM
    if [[ "$ENABLE_OOM_DUMP" == "true" ]]; then
        options="$options -XX:+HeapDumpOnOutOfMemoryError"
        options="$options -XX:HeapDumpPath=$HEAP_DUMP_FILE"
    fi

    # Flight Recorder
    if [[ "$ENABLE_FLIGHT_RECORDER" == "true" ]]; then
        options="$options -XX:StartFlightRecording=filename=$JFR_FILE,dumponexit=true,settings=profile"
    fi

    # JMX for monitoring
    options="$options -Dcom.sun.management.jmxremote"
    options="$options -Dcom.sun.management.jmxremote.port=9010"
    options="$options -Dcom.sun.management.jmxremote.authenticate=false"
    options="$options -Dcom.sun.management.jmxremote.ssl=false"

    # Print options for debugging
    options="$options -XX:+PrintFlagsFinal"

    echo "$options"
}

run_data_caterer() {
    local jar_file="$1"
    local jvm_options="$2"

    log_info "Running Data Caterer with scenario: $YAML_FILE"
    log_info "JVM Options: $jvm_options"

    # Update YAML to use correct HTTP port if needed
    local yaml_content=$(cat "$YAML_FILE")
    if [[ "$HTTP_PORT" != "8080" ]]; then
        yaml_content=$(echo "$yaml_content" | sed "s/:8080/:$HTTP_PORT/g")
        local temp_yaml="$RESULTS_DIR/$TIMESTAMP-scenario.yaml"
        echo "$yaml_content" > "$temp_yaml"
        YAML_FILE="$temp_yaml"
        log_info "Updated YAML with port $HTTP_PORT: $temp_yaml"
    fi

    # Run Data Caterer
    cd "$PROJECT_ROOT"

    local start_time=$(date +%s)

    # Run with environment variables explicitly set
    # shellcheck disable=SC2086
    PLAN_FILE_PATH="$YAML_FILE" GENERATED_REPORTS_FOLDER_PATH="$RESULTS_DIR/$TIMESTAMP-reports" \
        java $jvm_options -jar "$jar_file" > "$RESULTS_DIR/$TIMESTAMP-datacaterer.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$PROFILING_DIR/.data-caterer.pid"

    log_info "Data Caterer started (PID: $pid)"
    log_info "Monitoring progress..."

    # Start memory sampling in background
    local metrics_file="$RESULTS_DIR/$TIMESTAMP-metrics.json"
    python3 "$SCRIPT_DIR/sample-memory.py" "$pid" \
        --interval 1.0 \
        --output "$metrics_file" \
        --http-stats "http://localhost:$HTTP_PORT/stats" \
        > "$RESULTS_DIR/$TIMESTAMP-memory-sampling.log" 2>&1 &
    local sampler_pid=$!
    echo "$sampler_pid" > "$PROFILING_DIR/.memory-sampler.pid"
    log_info "Memory sampling started (PID: $sampler_pid)"

    # Monitor process
    while ps -p "$pid" > /dev/null 2>&1; do
        sleep 5

        # Get HTTP server stats
        local stats=$(curl -s "http://localhost:$HTTP_PORT/stats" || echo "{}")
        local requests=$(echo "$stats" | python3 -c "import sys, json; print(json.load(sys.stdin).get('total_requests', 0))" 2>/dev/null || echo "0")
        local throughput=$(echo "$stats" | python3 -c "import sys, json; print(f\"{json.load(sys.stdin).get('throughput_per_sec', 0):.1f}\")" 2>/dev/null || echo "0.0")

        log_info "Progress: $requests requests | Throughput: $throughput/sec"
    done

    # Stop memory sampling
    if [[ -f "$PROFILING_DIR/.memory-sampler.pid" ]]; then
        local sampler_pid=$(cat "$PROFILING_DIR/.memory-sampler.pid")
        kill "$sampler_pid" 2>/dev/null || true
        rm -f "$PROFILING_DIR/.memory-sampler.pid"
        log_info "Memory sampling stopped"
    fi

    wait "$pid" || true
    local exit_code=$?

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    if [[ $exit_code -eq 0 ]]; then
        log_success "Data Caterer completed successfully in ${duration}s"
    else
        log_error "Data Caterer exited with code $exit_code"
        log_error "Check logs: $RESULTS_DIR/$TIMESTAMP-datacaterer.log"
    fi

    rm -f "$PROFILING_DIR/.data-caterer.pid"

    return $exit_code
}

generate_report() {
    local start_time="$1"
    local end_time="$2"
    local duration=$((end_time - start_time))

    log_info "Generating memory profiling report..."

    # Get HTTP server final stats
    local stats=$(curl -s "http://localhost:$HTTP_PORT/stats" || echo "{}")

    # Create report
    cat > "$REPORT_FILE" << EOF
================================================================================
Memory Profiling Report - Data Caterer
================================================================================

Scenario: $(basename "$YAML_FILE")
Timestamp: $TIMESTAMP
Duration: ${duration}s ($(date -u -r $duration +%H:%M:%S 2>/dev/null || echo "${duration}s"))

JVM Configuration:
  Min Heap:     $MIN_HEAP
  Max Heap:     $MAX_HEAP
  GC Algorithm: G1GC

Profiling Options:
  OOM Heap Dump:      $ENABLE_OOM_DUMP
  Flight Recorder:    $ENABLE_FLIGHT_RECORDER
  GC Logging:         $ENABLE_GC_LOGGING
  External Profiler:  ${PROFILER_TOOL:-None}

HTTP Server Statistics:
EOF

    # Parse and add HTTP stats
    echo "$stats" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(f\"  Total Requests:     {data.get('total_requests', 0):,}\")
    print(f\"  Total Data:         {data.get('total_bytes', 0) / (1024*1024):.2f} MB\")
    print(f\"  Throughput:         {data.get('throughput_per_sec', 0):.2f} requests/sec\")
    print(f\"  Avg Latency:        {data.get('avg_latency_ms', 0):.3f} ms\")
    print(f\"  Errors:             {data.get('errors', 0)}\")
    print(f\"  Server Memory:      {data.get('memory_mb', 0):.2f} MB\")
except:
    print('  (Statistics unavailable)')
" >> "$REPORT_FILE"

    cat >> "$REPORT_FILE" << EOF

Output Files:
  Report:             $REPORT_FILE
  Data Caterer Log:   $RESULTS_DIR/$TIMESTAMP-datacaterer.log
  HTTP Server Log:    $HTTP_LOG_FILE
  Build Log:          $RESULTS_DIR/$TIMESTAMP-build.log
EOF

    if [[ "$ENABLE_GC_LOGGING" == "true" ]]; then
        echo "  GC Log:             $GC_LOG_FILE" >> "$REPORT_FILE"
    fi

    if [[ "$ENABLE_OOM_DUMP" == "true" && -f "$HEAP_DUMP_FILE" ]]; then
        echo "  Heap Dump:          $HEAP_DUMP_FILE" >> "$REPORT_FILE"
    fi

    if [[ "$ENABLE_FLIGHT_RECORDER" == "true" && -f "$JFR_FILE" ]]; then
        echo "  Flight Recorder:    $JFR_FILE" >> "$REPORT_FILE"
    fi

    cat >> "$REPORT_FILE" << EOF

Analysis Commands:
  View GC log:        java -jar /path/to/gceasy.jar $GC_LOG_FILE
  Analyze heap dump:  jhat -J-Xmx4g $HEAP_DUMP_FILE
  View JFR:           jmc $JFR_FILE

================================================================================
EOF

    # Print report to console
    cat "$REPORT_FILE"

    log_success "Report saved to: $REPORT_FILE"
}

###############################################################################
# Main Execution
###############################################################################

main() {
    echo "" >&2
    echo "================================================================================" >&2
    echo "  Data Caterer Memory Profiling" >&2
    echo "================================================================================" >&2
    echo "" >&2

    # Validate YAML file exists
    if [[ ! -f "$YAML_FILE" ]]; then
        log_error "YAML file not found: $YAML_FILE"
        exit 1
    fi

    log_info "Configuration:"
    log_info "  Scenario:    $YAML_FILE"
    log_info "  Min Heap:    $MIN_HEAP"
    log_info "  Max Heap:    $MAX_HEAP"
    log_info "  HTTP Port:   $HTTP_PORT"
    log_info "  Results:     $RESULTS_DIR"
    echo "" >&2

    # Check dependencies
    check_dependencies
    echo "" >&2

    # Build Data Caterer
    local jar_file=$(build_data_caterer)
    echo "" >&2

    # Start HTTP server
    start_http_server
    echo "" >&2

    # Build JVM options
    local jvm_options=$(build_jvm_options)

    # Run Data Caterer with profiling
    local start_time=$(date +%s)
    run_data_caterer "$jar_file" "$jvm_options"
    local exit_code=$?
    local end_time=$(date +%s)

    echo ""

    # Generate report
    generate_report "$start_time" "$end_time"

    echo ""

    if [[ $exit_code -eq 0 ]]; then
        log_success "Memory profiling completed successfully!"
    else
        log_error "Memory profiling completed with errors (exit code: $exit_code)"
    fi

    # Stop HTTP server unless --keep-server
    if [[ "$KEEP_SERVER" == "true" ]]; then
        log_info "HTTP server left running (use 'kill $(cat $PROFILING_DIR/.http-server.pid)' to stop)"
    fi

    exit $exit_code
}

main "$@"
