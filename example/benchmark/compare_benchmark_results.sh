#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$(cd "$SCRIPT_DIR/results" && pwd)"
LATEST_VERSION=${1:-0.11.5}
RESULT_FILE_REGEX="benchmark_results_([0-9\.]+)\.txt"

if [[ -z $2 ]]; then
  echo "No second version to compare against passed into arguments, defaulting to previous version in results"
  PREVIOUS_VERSION_RESULT_FILE_NAME=$(ls -1 "$RESULTS_DIR" | sort --version-sort | tail -2 | head -1)
  if [[ $PREVIOUS_VERSION_RESULT_FILE_NAME =~ $RESULT_FILE_REGEX ]]; then
    PREVIOUS_VERSION="${BASH_REMATCH[1]}"
  else
    echo "Previous version file name does not match regex: $RESULT_FILE_REGEX, previous version file: $PREVIOUS_VERSION_RESULT_FILE_NAME"
    exit 1
  fi
else
  PREVIOUS_VERSION=${2}
fi
echo "Latest version: $LATEST_VERSION"
echo "Previous version: $PREVIOUS_VERSION"
echo

# Automatically discover all unique benchmark configurations from the latest results
# Format: "ClassName:engine,recordCount,runNumber,time"
# We want unique combinations of: ClassName:engine,recordCount
echo "Discovering benchmark configurations from latest results..."
latest_file="$RESULTS_DIR/benchmark_results_${LATEST_VERSION}.txt"
previous_file="$RESULTS_DIR/benchmark_results_${PREVIOUS_VERSION}.txt"

if [[ ! -f "$latest_file" ]]; then
  echo "ERROR: Latest results file not found: $latest_file"
  exit 1
fi

if [[ ! -f "$previous_file" ]]; then
  echo "WARNING: Previous results file not found: $previous_file"
  echo "Skipping comparison, only showing latest results"
  PREVIOUS_VERSION=""
fi

# Extract unique benchmark patterns (class:engine,recordCount)
# Skipping header lines that don't match the benchmark result pattern
plans=$(grep -E "^io\.github\.datacatering\.plan\.benchmark\." "$latest_file" | \
  awk -F "," '{print $1","$2}' | \
  sort -u)

echo "Found $(echo "$plans" | wc -l) unique benchmark configurations"
echo

for plan in $plans; do
  plan_name=$(echo "$plan" | sed 's/io.github.datacatering.plan.benchmark.//')
  echo "Comparing performance for: $plan_name"

  # Get all runs for this configuration from latest version
  latest_version_results=$(grep "^${plan}," "$latest_file" || true)

  if [[ -z "$latest_version_results" ]]; then
    echo "  No results found in latest version"
    echo
    continue
  fi

  # Count number of runs
  num_runs=$(echo "$latest_version_results" | wc -l)
  latest_version_average_time=$(echo "$latest_version_results" | awk -F "," -v n="$num_runs" '{s+=$4} END {if (n>0) print s/n; else print 0}')

  if [[ -n "$PREVIOUS_VERSION" ]]; then
    # Get all runs for this configuration from previous version
    previous_version_results=$(grep "^${plan}," "$previous_file" || true)

    if [[ -z "$previous_version_results" ]]; then
      echo "  Version: $LATEST_VERSION, Average time (s): $latest_version_average_time (NEW BENCHMARK - no comparison available)"
    else
      prev_num_runs=$(echo "$previous_version_results" | wc -l)
      previous_version_average_time=$(echo "$previous_version_results" | awk -F "," -v n="$prev_num_runs" '{s+=$4} END {if (n>0) print s/n; else print 0}')

      difference=$(awk -v t1="$previous_version_average_time" -v t2="$latest_version_average_time" 'BEGIN{printf "%.3f", t2-t1}')
      percent_difference=$(awk -v t1="$previous_version_average_time" -v t2="$latest_version_average_time" 'BEGIN{if (t1>0) printf "%.3f", (t2-t1)/t1 * 100; else print "N/A"}')

      echo "  Version: $PREVIOUS_VERSION, Average time (s): $previous_version_average_time"
      echo "  Version: $LATEST_VERSION, Average time (s): $latest_version_average_time, Difference (s): $difference, Percent: $percent_difference%"
    fi
  else
    echo "  Version: $LATEST_VERSION, Average time (s): $latest_version_average_time"
  fi
  echo
done

# Summary statistics
echo "================================"
echo "Summary Statistics"
echo "================================"
if [[ -n "$PREVIOUS_VERSION" ]]; then
  echo "Comparing $LATEST_VERSION vs $PREVIOUS_VERSION"

  # Calculate overall performance change
  all_comparisons=$(for plan in $plans; do
    latest_results=$(grep "^${plan}," "$latest_file" || true)
    previous_results=$(grep "^${plan}," "$previous_file" || true)

    if [[ -n "$latest_results" && -n "$previous_results" ]]; then
      latest_avg=$(echo "$latest_results" | awk -F "," '{s+=$4; n++} END {if (n>0) print s/n}')
      previous_avg=$(echo "$previous_results" | awk -F "," '{s+=$4; n++} END {if (n>0) print s/n}')

      if [[ -n "$latest_avg" && -n "$previous_avg" ]]; then
        awk -v t1="$previous_avg" -v t2="$latest_avg" 'BEGIN{if (t1>0) printf "%.3f\n", (t2-t1)/t1 * 100}'
      fi
    fi
  done)

  if [[ -n "$all_comparisons" ]]; then
    avg_percent_change=$(echo "$all_comparisons" | awk '{s+=$1; n++} END {if (n>0) printf "%.3f", s/n}')
    echo "Average performance change: ${avg_percent_change}%"

    improvements=$(echo "$all_comparisons" | awk '$1<0 {n++} END {print n+0}')
    regressions=$(echo "$all_comparisons" | awk '$1>0 {n++} END {print n+0}')
    total=$(echo "$all_comparisons" | wc -l)

    echo "Improvements: $improvements / $total"
    echo "Regressions: $regressions / $total"
  fi
else
  echo "Latest version only: $LATEST_VERSION"
  echo "Total benchmarks run: $(echo "$plans" | wc -l)"
fi
