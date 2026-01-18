# Data Caterer Memory Profiling

This directory contains production-grade infrastructure for accurate memory profiling of Data Caterer's HTTP streaming capabilities, replacing unreliable Gradle integration tests.

## Overview

The memory profiling system provides:

1. **HTTP Test Server** - Python server that receives and tracks HTTP events with metrics
2. **Memory Profiling Script** - Full orchestration: builds JAR, starts server, configures JVM profiling, runs scenarios
3. **Test Scenarios** - YAML configurations testing different memory patterns (baseline, high-throughput, stress)
4. **Comprehensive Reports** - GC logs, heap dumps, Flight Recorder data, performance metrics

## Quick Start

```bash
cd misc/memory-profiling

# Run baseline test (100 records) - VERIFIED WORKING ✅
./scripts/run-memory-profile.sh scenarios/baseline-http.yaml 512m 2g

# Run specific scenario
./scripts/run-memory-profile.sh scenarios/high-throughput-http.yaml 1g 4g

# Run with heap dump on OOM
./scripts/run-memory-profile.sh scenarios/stress-test.yaml 256m 512m --oom-dump

# Run with Java Flight Recorder
./scripts/run-memory-profile.sh scenarios/baseline-http.yaml 512m 2g --jfr

# Custom HTTP port
HTTP_PORT=9090 ./scripts/run-memory-profile.sh scenarios/baseline-http.yaml 512m 2g
```

## Components

### 1. HTTP Test Server (`http-server/simple-http-server.py`)

A simple Python HTTP server that:
- Receives POST requests from Data Caterer
- Logs request count and timing
- Tracks memory statistics
- Runs on port 8080 by default

**Usage:**
```bash
python3 http-server/simple-http-server.py --port 8080 --log-level info
```

### 2. Memory Profiling Script (`scripts/run-memory-profile.sh`)

Main script that orchestrates the profiling:
- Builds Data Caterer JAR
- Starts HTTP server
- Configures JVM profiling options
- Runs Data Caterer with specified scenario
- Captures memory metrics
- Generates reports

**Usage:**
```bash
./scripts/run-memory-profile.sh [YAML_FILE] [MIN_HEAP] [MAX_HEAP] [OPTIONS]

Arguments:
  YAML_FILE  - Path to YAML scenario (default: scenarios/baseline-http.yaml)
  MIN_HEAP   - JVM min heap size (default: 512m)
  MAX_HEAP   - JVM max heap size (default: 2g)

Options:
  --oom-dump           - Generate heap dump on OutOfMemoryError
  --flight-recorder    - Enable Java Flight Recorder
  --gc-logging         - Enable detailed GC logging
  --profiler TOOL      - Use external profiler (async-profiler, yourkit)
  --port PORT          - HTTP server port (default: 8080)
  --keep-server        - Don't stop HTTP server after run
```

### 3. Test Scenarios (`scenarios/`)

Pre-configured YAML files for different memory test cases:

- `baseline-http.yaml` - ✅ **VERIFIED** - Simple baseline test (100 records, 5/sec rate)
- `baseline-json.yaml` - JSON file baseline (10K records)
- `bounded-buffer-test.yaml` - Tests bounded buffer behavior
- `high-throughput-http.yaml` - 📝 TODO - High throughput streaming (100K records/sec)
- `stress-test-http.yaml` - 📝 TODO - Extreme load to test OOM prevention

#### Verified Baseline Results (2026-01-16)

```
Scenario: baseline-http.yaml (100 records @ 5/sec)
Duration:         31 seconds
Total Requests:   100
Throughput:       3.05 requests/sec
Avg Latency:      0.064 ms
Errors:           0
Heap:             512m min, 2g max
Status:           ✅ SUCCESS
```

### 4. Results (`results/`)

Output directory for profiling results:
- Heap dumps (`.hprof` files)
- Flight recorder recordings (`.jfr` files)
- GC logs
- Memory usage reports
- HTTP server metrics

## Profiling Tools

### Built-in JVM Tools

**Java Flight Recorder (JFR):**
```bash
# Run with Flight Recorder
./scripts/run-memory-profile.sh scenarios/stress-test.yaml 1g 2g --flight-recorder

# Analyze JFR file
jcmd <PID> JFR.dump filename=results/recording.jfr
```

**Heap Dumps:**
```bash
# Automatic dump on OOM
./scripts/run-memory-profile.sh scenarios/stress-test.yaml 512m 1g --oom-dump

# Manual heap dump
jmap -dump:live,format=b,file=results/heap-dump.hprof <PID>

# Analyze with jhat
jhat results/heap-dump.hprof
# Open browser to http://localhost:7000
```

### External Profilers

**async-profiler** (recommended):
```bash
# Download async-profiler
wget https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-x64.tar.gz
tar xzf async-profiler-3.0-linux-x64.tar.gz -C ~/tools/

# Run with async-profiler
./scripts/run-memory-profile.sh scenarios/stress-test.yaml 1g 2g --profiler async-profiler
```

**VisualVM:**
```bash
# Start VisualVM
visualvm &

# Run Data Caterer with JMX enabled (automatic in script)
# Connect VisualVM to the process
```

## Creating Custom Scenarios

Create a new YAML file in `scenarios/` directory:

```yaml
version: "1.0"
name: "my-custom-memory-test"
description: "Custom scenario for testing specific memory patterns"

connections:
  - name: "test_http"
    type: "http"
    url: "http://localhost:8080"
    method: "POST"

tasks:
  - name: "memory_test_task"
    dataSource: "test_http"
    enabled: true

    count:
      records: 100000  # Adjust for your test

    schema:
      fields:
        - name: "id"
          type: "string"
          generator:
            type: "regex"
            options:
              regex: "ID[0-9]{8}"

        - name: "payload"
          type: "string"
          generator:
            type: "regex"
            options:
              regex: "[A-Za-z0-9]{500}"  # 500 byte payload

        - name: "timestamp"
          type: "timestamp"
          generator:
            type: "datetime"

flags:
  enableFastGeneration: true
  enableGenerateData: true
```

### CRITICAL: Unified YAML Format for HTTP

Data Caterer's **unified YAML format (v1.0+)** requires special field structure for HTTP. The `UnifiedConfigConverter` does NOT support the hierarchical `httpUrl`/`httpHeaders`/`httpBody` convenience fields used in legacy format.

**✅ CORRECT - Use Direct Internal Field Names:**

```yaml
version: "1.0"
name: "http_memory_test"

dataSources:
  - name: "test_http"
    connection:
      type: "http"
      options:
        url: "http://localhost:8080"
    steps:
      - name: "events"
        options:
          format: "http"  # ⚠️ REQUIRED for HTTP format
        count:
          records: 100
        fields:
          # Direct internal field names (NOT nested httpUrl/httpHeaders/httpBody)
          - name: "url"
            static: "http://localhost:8080/"

          - name: "method"
            static: "POST"

          - name: "Content-Type"
            static: "application/json"

          - name: "body"
            options:
              sql: "TO_JSON(STRUCT(id, user_id, event_type, timestamp, payload))"

          # Body field definitions (will be composed into JSON body)
          - name: "id"
            options:
              regex: "ID[0-9]{8}"

          - name: "user_id"
            options:
              regex: "USER[0-9]{6}"

          - name: "event_type"
            options:
              oneOf:
                - "LOGIN"
                - "LOGOUT"
                - "PAGE_VIEW"

          - name: "timestamp"
            type: "timestamp"

          - name: "payload"
            options:
              regex: "[A-Za-z0-9]{200}"
```

**❌ INCORRECT - Legacy Hierarchical Structure (Does NOT work in unified format):**

```yaml
# This WILL FAIL in unified format v1.0+
fields:
  - name: "httpUrl"  # ❌ Not supported in unified format
    fields:
      - name: "url"
        static: "http://localhost:8080/"
```

**Key Requirements:**
1. Must include `format: "http"` in step options
2. Use direct field names: `url`, `method`, `Content-Type`, `body`
3. The `body` field uses SQL (TO_JSON) to construct JSON from other fields
4. Individual data fields are generated separately and composed into body
5. The unified converter does NOT transform `httpUrl` → `url` like PlanParser does for legacy format

## Analyzing Results

### Memory Usage Report

The script generates a summary report at `results/<timestamp>-memory-report.txt`:

```
=== Memory Profiling Report ===
Scenario: high-throughput-http.yaml
Start Time: 2026-01-16 10:30:00
End Time: 2026-01-16 10:35:23
Duration: 5m 23s

JVM Configuration:
  Min Heap: 512m
  Max Heap: 2g
  GC: G1GC

Records Generated: 500,000
Throughput: 1,547 records/sec

Peak Memory Usage: 1,234 MB
Average Memory Usage: 856 MB
GC Count: 23
GC Time: 2.3s

HTTP Server Stats:
  Requests Received: 500,000
  Average Latency: 2.3ms
  Errors: 0
```

### GC Analysis

```bash
# Analyze GC log
java -jar ~/tools/gceasy.io/gceasy.jar results/gc.log

# Or use GCViewer
java -jar ~/tools/gcviewer/gcviewer.jar results/gc.log
```

### Heap Dump Analysis

```bash
# Eclipse Memory Analyzer Tool (MAT)
# Download from: https://www.eclipse.org/mat/
mat results/heap-dump.hprof

# Or use jhat (built-in)
jhat -J-Xmx4g results/heap-dump.hprof
# Open http://localhost:7000
```

## Troubleshooting

### HTTP Server Not Starting
```bash
# Check if port is in use
lsof -i :8080

# Kill existing process
kill $(lsof -t -i:8080)

# Use different port
./scripts/run-memory-profile.sh scenarios/baseline-http.yaml 512m 2g --port 8081
```

### Out of Memory Errors
```bash
# Increase heap size
./scripts/run-memory-profile.sh scenarios/stress-test.yaml 2g 4g

# Enable heap dump to analyze
./scripts/run-memory-profile.sh scenarios/stress-test.yaml 2g 4g --oom-dump

# Analyze heap dump with MAT
```

### Slow Data Generation
```bash
# Ensure fast generation is enabled in YAML
flags:
  enableFastGeneration: true

# Check GC overhead
# Look for frequent GC in results/gc.log
```

## Best Practices

1. **Start Small**: Begin with baseline scenario, then scale up
2. **Monitor Resources**: Keep an eye on system resources during profiling
3. **Isolate Tests**: Run one scenario at a time for accurate measurements
4. **Compare Results**: Run same scenario with different heap settings
5. **Use Flight Recorder**: Minimal overhead with comprehensive data
6. **Heap Dump Analysis**: Only when needed (captures at specific point)
7. **Clean Up**: Remove old results to save disk space

## Example Workflow

```bash
# 1. Build and test baseline
./scripts/run-memory-profile.sh scenarios/baseline-http.yaml

# 2. Test high throughput with profiling
./scripts/run-memory-profile.sh scenarios/high-throughput-http.yaml 1g 2g --flight-recorder

# 3. Stress test with OOM protection
./scripts/run-memory-profile.sh scenarios/stress-test-http.yaml 512m 1g --oom-dump

# 4. Analyze results
ls -lh results/
jhat results/*-heap-dump.hprof
```

## References

- [Java Flight Recorder](https://docs.oracle.com/javacomponents/jmc-5-4/jfr-runtime-guide/about.htm)
- [async-profiler](https://github.com/async-profiler/async-profiler)
- [Eclipse MAT](https://www.eclipse.org/mat/)
- [GC Easy](https://gceasy.io/)
