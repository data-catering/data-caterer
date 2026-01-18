#!/usr/bin/env python3
"""
Memory sampling script for Data Caterer profiling.
Samples JVM memory usage using jcmd (more reliable than jstat) and outputs JSON metrics.
"""

import subprocess
import json
import time
import argparse
import sys
import re
from datetime import datetime
from pathlib import Path


def find_jcmd():
    """Find jcmd command."""
    # Try common locations
    jcmd_paths = [
        'jcmd',  # In PATH
        '/usr/bin/jcmd',
        '/usr/local/bin/jcmd',
    ]

    # Check JAVA_HOME
    try:
        java_home = subprocess.run(['java', '-XshowSettings:properties', '-version'],
                                  capture_output=True, text=True, timeout=2).stderr
        for line in java_home.split('\n'):
            if 'java.home' in line:
                jdk_home = line.split('=')[1].strip()
                jcmd_paths.insert(0, f"{jdk_home}/bin/jcmd")
                break
    except:
        pass

    for path in jcmd_paths:
        try:
            result = subprocess.run([path, '-h'], capture_output=True, timeout=1)
            if result.returncode == 0 or 'Usage' in result.stdout.decode():
                return path
        except:
            continue

    return None


def get_jvm_memory_jcmd(pid, jcmd_cmd='jcmd'):
    """Get JVM memory stats using jcmd (more reliable than jstat)."""
    try:
        # Use jcmd to get GC heap info
        result = subprocess.run(
            [jcmd_cmd, str(pid), 'GC.heap_info'],
            capture_output=True,
            text=True,
            timeout=2
        )

        if result.returncode != 0:
            return None

        output = result.stdout

        # Parse jcmd output
        # Example output format:
        # garbage-first heap   total 524288K, used 157696K
        # Eden space: capacity = X, used = Y
        # Survivor space: capacity = X, used = Y
        # Old generation: capacity = X, used = Y

        stats = {}

        # Extract total and used heap
        heap_match = re.search(r'total\s+(\d+)K,\s+used\s+(\d+)K', output)
        if heap_match:
            stats['committedHeapKB'] = float(heap_match.group(1))
            stats['usedHeapKB'] = float(heap_match.group(2))

        # Extract Eden space
        eden_match = re.search(r'Eden.*?capacity\s*=\s*(\d+).*?used\s*=\s*(\d+)', output, re.DOTALL)
        if eden_match:
            stats['edenUsedKB'] = float(eden_match.group(2)) / 1024  # Convert bytes to KB
        else:
            stats['edenUsedKB'] = 0

        # Extract Old generation
        old_match = re.search(r'(?:Old|Tenured).*?capacity\s*=\s*(\d+).*?used\s*=\s*(\d+)', output, re.DOTALL)
        if old_match:
            stats['oldGenUsedKB'] = float(old_match.group(2)) / 1024
        else:
            stats['oldGenUsedKB'] = 0

        # Get GC stats from GC.class_histogram or VM.info
        gc_result = subprocess.run(
            [jcmd_cmd, str(pid), 'VM.info'],
            capture_output=True,
            text=True,
            timeout=2
        )

        if gc_result.returncode == 0:
            gc_output = gc_result.stdout
            # Try to extract GC counts (format varies by GC)
            young_gc_match = re.search(r'(?:young|minor).*?collections?\s*[:=]\s*(\d+)', gc_output, re.IGNORECASE)
            full_gc_match = re.search(r'(?:full|major|old).*?collections?\s*[:=]\s*(\d+)', gc_output, re.IGNORECASE)

            stats['youngGCCount'] = int(young_gc_match.group(1)) if young_gc_match else 0
            stats['fullGCCount'] = int(full_gc_match.group(1)) if full_gc_match else 0
            stats['youngGCTime'] = 0.0  # jcmd doesn't easily provide GC times
            stats['fullGCTime'] = 0.0
        else:
            stats['youngGCCount'] = 0
            stats['fullGCCount'] = 0
            stats['youngGCTime'] = 0.0
            stats['fullGCTime'] = 0.0

        return stats

    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, ValueError) as e:
        print(f"Warning: Failed to get JVM stats via jcmd: {e}", file=sys.stderr)
        return None


def get_jvm_memory(pid, jstat_cmd='jstat'):
    """Get JVM memory stats using jstat (fallback method)."""
    try:
        # Run jstat to get heap memory usage
        result = subprocess.run(
            [jstat_cmd, '-gc', str(pid)],
            capture_output=True,
            text=True,
            timeout=2
        )

        if result.returncode != 0:
            return None

        lines = result.stdout.strip().split('\n')
        if len(lines) < 2:
            return None

        headers = lines[0].split()
        values = lines[1].split()

        if len(headers) != len(values):
            return None

        stats = dict(zip(headers, values))

        # Calculate heap usage in KB
        # S0C, S1C, EC, OC, MC = capacities
        # S0U, S1U, EU, OU, MU = used
        eden_used = float(stats.get('EU', 0))
        survivor0_used = float(stats.get('S0U', 0))
        survivor1_used = float(stats.get('S1U', 0))
        old_used = float(stats.get('OU', 0))

        eden_capacity = float(stats.get('EC', 0))
        survivor0_capacity = float(stats.get('S0C', 0))
        survivor1_capacity = float(stats.get('S1C', 0))
        old_capacity = float(stats.get('OC', 0))

        used_heap_kb = eden_used + survivor0_used + survivor1_used + old_used
        committed_heap_kb = eden_capacity + survivor0_capacity + survivor1_capacity + old_capacity

        # GC stats
        young_gc_count = int(float(stats.get('YGC', 0)))
        young_gc_time = float(stats.get('YGCT', 0))
        full_gc_count = int(float(stats.get('FGC', 0)))
        full_gc_time = float(stats.get('FGCT', 0))

        return {
            'usedHeapKB': used_heap_kb,
            'committedHeapKB': committed_heap_kb,
            'youngGCCount': young_gc_count,
            'youngGCTime': young_gc_time,
            'fullGCCount': full_gc_count,
            'fullGCTime': full_gc_time,
            'edenUsedKB': eden_used,
            'oldGenUsedKB': old_used
        }

    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, ValueError, KeyError) as e:
        print(f"Warning: Failed to get JVM stats: {e}", file=sys.stderr)
        return None


def sample_memory(pid, interval, output_file, http_stats_url=None, jcmd_cmd='jcmd', jstat_cmd=None):
    """Sample memory usage at regular intervals."""
    samples = []
    start_time = time.time()
    last_gc_count = 0
    cumulative_records = 0

    print(f"Starting memory sampling for PID {pid}, interval={interval}s", file=sys.stderr)
    print(f"Using jcmd: {jcmd_cmd} (with jstat fallback: {jstat_cmd})", file=sys.stderr)

    try:
        while True:
            # Check if process is still running
            try:
                subprocess.run(['ps', '-p', str(pid)], capture_output=True, check=True)
            except subprocess.CalledProcessError:
                print(f"Process {pid} has terminated", file=sys.stderr)
                break

            # Get current timestamp
            elapsed = time.time() - start_time

            # Sample JVM memory (try jcmd first, fall back to jstat)
            mem_stats = get_jvm_memory_jcmd(pid, jcmd_cmd)
            if not mem_stats and jstat_cmd:
                mem_stats = get_jvm_memory(pid, jstat_cmd)

            if not mem_stats:
                time.sleep(interval)
                continue

            # Calculate GC pause time (approximation)
            current_gc_count = mem_stats['youngGCCount'] + mem_stats['fullGCCount']
            gc_pause_ms = 0
            if current_gc_count > last_gc_count:
                # Estimate pause time from GC time increase
                total_gc_time = mem_stats['youngGCTime'] + mem_stats['fullGCTime']
                if samples:
                    prev_gc_time = samples[-1].get('totalGCTime', 0)
                    gc_pause_ms = (total_gc_time - prev_gc_time) * 1000
                last_gc_count = current_gc_count

            # Get HTTP stats if available
            records_per_sec = 0
            http_requests_per_sec = 0
            if http_stats_url:
                try:
                    import urllib.request
                    with urllib.request.urlopen(http_stats_url, timeout=1) as response:
                        http_stats = json.loads(response.read())
                        cumulative_records = http_stats.get('total_requests', 0)
                        http_requests_per_sec = http_stats.get('throughput_per_sec', 0)
                        records_per_sec = http_requests_per_sec  # Same for HTTP sink
                except Exception as e:
                    pass  # Ignore HTTP stats errors

            # Create sample
            sample = {
                'timestamp': round(elapsed, 2),
                'usedHeapMB': round(mem_stats['usedHeapKB'] / 1024, 2),
                'committedHeapMB': round(mem_stats['committedHeapKB'] / 1024, 2),
                'edenUsedMB': round(mem_stats['edenUsedKB'] / 1024, 2),
                'oldGenUsedMB': round(mem_stats['oldGenUsedKB'] / 1024, 2),
                'youngGCCount': mem_stats['youngGCCount'],
                'fullGCCount': mem_stats['fullGCCount'],
                'totalGCTime': round(mem_stats['youngGCTime'] + mem_stats['fullGCTime'], 3),
                'gcPauseMs': round(gc_pause_ms, 2),
                'recordsPerSec': round(records_per_sec, 2),
                'httpRequestsPerSec': round(http_requests_per_sec, 2),
                'cumulativeRecords': cumulative_records
            }

            samples.append(sample)

            # Write to output file periodically
            if len(samples) % 5 == 0 or not samples:
                write_metrics(output_file, samples, start_time)

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopping memory sampling", file=sys.stderr)

    # Final write
    write_metrics(output_file, samples, start_time)
    print(f"Collected {len(samples)} samples, written to {output_file}", file=sys.stderr)

    return samples


def write_metrics(output_file, samples, start_time):
    """Write metrics to JSON file."""
    if not samples:
        return

    last_sample = samples[-1]
    used_heaps = [s['usedHeapMB'] for s in samples]

    metrics = {
        'summary': {
            'duration': round(time.time() - start_time, 1),
            'peakMemoryMB': round(max(used_heaps), 2),
            'avgMemoryMB': round(sum(used_heaps) / len(used_heaps), 2),
            'minMemoryMB': round(min(used_heaps), 2),
            'gcCount': last_sample['youngGCCount'] + last_sample['fullGCCount'],
            'totalGCTime': last_sample['totalGCTime'],
            'totalRecords': last_sample['cumulativeRecords'],
            'sampleCount': len(samples)
        },
        'samples': samples
    }

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump(metrics, f, indent=2)

    # Also write to latest-metrics.json for auto-refresh
    latest_path = output_path.parent / 'latest-metrics.json'
    with open(latest_path, 'w') as f:
        json.dump(metrics, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Sample JVM memory usage using jcmd (or jstat fallback)')
    parser.add_argument('pid', type=int, help='Process ID to monitor')
    parser.add_argument('--interval', type=float, default=1.0, help='Sampling interval in seconds (default: 1.0)')
    parser.add_argument('--output', required=True, help='Output JSON file')
    parser.add_argument('--http-stats', help='HTTP stats URL (e.g., http://localhost:8080/stats)')

    args = parser.parse_args()

    # Find jcmd command (preferred)
    jcmd_cmd = find_jcmd()
    if not jcmd_cmd:
        print("Error: jcmd not found. Make sure JDK is installed and JAVA_HOME is set.", file=sys.stderr)
        sys.exit(1)

    print(f"Found jcmd at: {jcmd_cmd}", file=sys.stderr)

    # Try to find jstat as fallback
    jstat_cmd = None
    try:
        java_home = subprocess.run(['java', '-XshowSettings:properties', '-version'],
                                  capture_output=True, text=True, timeout=2).stderr
        for line in java_home.split('\n'):
            if 'java.home' in line:
                jdk_home = line.split('=')[1].strip()
                jstat_path = f"{jdk_home}/bin/jstat"
                if subprocess.run([jstat_path, '-version'], capture_output=True, timeout=1).returncode == 0:
                    jstat_cmd = jstat_path
                    print(f"Found jstat fallback at: {jstat_cmd}", file=sys.stderr)
                break
    except:
        pass

    # Verify process exists
    try:
        subprocess.run(['ps', '-p', str(args.pid)], capture_output=True, check=True)
    except subprocess.CalledProcessError:
        print(f"Error: Process {args.pid} not found", file=sys.stderr)
        sys.exit(1)

    sample_memory(args.pid, args.interval, args.output, args.http_stats, jcmd_cmd, jstat_cmd)


if __name__ == '__main__':
    main()
