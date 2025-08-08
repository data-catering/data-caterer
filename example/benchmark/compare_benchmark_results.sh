#!/usr/bin/env bash

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
LATEST_VERSION=${1:-0.11.5}
RESULT_FILE_REGEX="benchmark_results_([0-9\.]+)\.txt"

if [[ -z $2 ]]; then
  echo "No second version to compare against passed into arguments, defaulting to previous version in results"
  PREVIOUS_VERSION_RESULT_FILE_NAME=$(ls -1 "$SCRIPT_DIR/results" | sort --version-sort | tail -2 | head -1)
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


plans=(
  "io.github.datacatering.plan.benchmark.BenchmarkParquetPlanRun:,10000,"
  "io.github.datacatering.plan.benchmark.BenchmarkParquetPlanRun:,100000,"
  "io.github.datacatering.plan.benchmark.BenchmarkParquetPlanRun:,1000000,"
  "io.github.datacatering.plan.benchmark.BenchmarkForeignKeyPlanRun:,500000,"
  "io.github.datacatering.plan.benchmark.BenchmarkJsonPlanRun:,100000,"
)

for plan in "${plans[@]}"; do
  plan_name=$(echo "$plan" | sed 's/io.github.datacatering.plan.benchmark.//')
  echo "Comparing performance for plan: $plan_name"

  latest_version_results=$(cat "$SCRIPT_DIR/results/benchmark_results_${LATEST_VERSION}.txt" | grep "${plan}")
  previous_version_results=$(cat "$SCRIPT_DIR/results/benchmark_results_${PREVIOUS_VERSION}.txt" | grep "${plan}")

  latest_version_average_time=$(echo "$latest_version_results" | awk -F "," '{s+=$4} END {print s/3}')
  previous_version_average_time=$(echo "$previous_version_results" | awk -F "," '{s+=$4} END {print s/3}')

  difference=$(awk -v t1="$previous_version_average_time" -v t2="$latest_version_average_time" 'BEGIN{printf "%.3f", t2-t1}')
  percent_difference=$(awk -v t1="$previous_version_average_time" -v t2="$latest_version_average_time" 'BEGIN{printf "%.3f", (t2-t1)/t1 * 100}')

  echo "Version: $PREVIOUS_VERSION, Average time (s): $previous_version_average_time"
  echo "Version: $LATEST_VERSION, Average time (s): $latest_version_average_time, Difference (s): $difference, Percent: $percent_difference%"
  echo
done
