#!/usr/bin/env bash

# Resolve project root relative to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$EXAMPLE_DIR/.." && pwd)"

PROP_FILE_LOCAL="$EXAMPLE_DIR/gradle.properties"
PROP_FILE_ROOT="$REPO_ROOT/gradle.properties"

if [[ -f "$PROP_FILE_ROOT" ]]; then
  PROP_FILE="$PROP_FILE_ROOT"
else
  PROP_FILE="$PROP_FILE_LOCAL"
fi

enable_query_engine_run=${ENABLE_QUERY_ENGINE_RUN:-true}
enable_data_size_run=${ENABLE_DATA_SIZE_RUN:-true}
enable_data_sink_run=${ENABLE_DATA_SINK_RUN:-true}

data_caterer_version=$(grep -E "^dataCatererVersion=" "$PROP_FILE" | cut -d= -f2)
default_job="io.github.datacatering.plan.benchmark.BenchmarkParquetPlanRun"
#default_job="io.github.datacatering.plan.benchmark.BenchmarkValidationPlanRun"
default_record_count="100000"
driver_memory="DRIVER_MEMORY=2g"
executor_memory="EXECUTOR_MEMORY=2g"
benchmark_result_file="benchmark/results/benchmark_results_${data_caterer_version}.txt"
num_runs=3
uname_out="$(uname -s)"
case "${uname_out}" in
    Darwin*)  sed_option="-E";;
    *)        sed_option="-r";;
esac
data_sizes=(10000 100000 1000000)
job_names=("BenchmarkForeignKeyPlanRun" "BenchmarkJsonPlanRun" "BenchmarkParquetPlanRun")

spark_query_execution_engines=("default" "blaze" "comet" "gluten")
gluten_spark_conf="--conf \"spark.plugins=io.glutenproject.GlutenPlugin\" --conf \"spark.memory.offHeap.enabled=true\" --conf \"spark.memory.offHeap.size=1024mb\" --conf \"spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager\""
blaze_spark_conf="--conf \"spark.sql.extensions=org.apache.spark.sql.blaze.BlazeSparkSessionExtension\" --conf \"spark.shuffle.manager=org.apache.spark.sql.execution.blaze.shuffle.BlazeShuffleManager\""
comet_spark_conf="--conf \"spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions\" --conf \"spark.comet.enabled=true\" --conf \"spark.comet.exec.enabled=true\" --conf \"spark.comet.exec.all.enabled=true\" --conf \"spark.comet.explainFallback.enabled=true\""

echo "Benchmark run" > "$benchmark_result_file"
{
  echo "Data Caterer version: $data_caterer_version"
  echo "Date: $(date)"
  echo "System info:"
  docker info | grep -E "OSType|Architecture|CPUs|Total Memory"
  echo "Driver memory: $driver_memory"
  echo "Executor memory: $executor_memory"
  echo "Class name, Num records, Num run, Time taken (s)"
} >> "$benchmark_result_file"

run_docker() {
  for num_run in $(seq 1 $num_runs)
  do
    echo "Run $num_run of $num_runs"
    case "$3" in
      blaze*) additional_conf="$blaze_spark_conf";;
      comet*) additional_conf="$comet_spark_conf";;
      gluten*) additional_conf="$gluten_spark_conf";;
      *) additional_conf="";;
    esac

    time_taken=$({
      time -p docker run -p 4040:4040 \
        -v "${EXAMPLE_DIR}/build/libs/data-caterer-example-0.1.0.jar:/opt/app/job.jar" \
        -v "${EXAMPLE_DIR}/benchmark/jars/blaze.jar:/opt/app/jars/blaze.jar" \
        -v "${EXAMPLE_DIR}/benchmark/jars/comet.jar:/opt/app/jars/comet.jar" \
        -v "${EXAMPLE_DIR}/benchmark/jars/gluten.jar:/opt/app/jars/gluten.jar" \
        -v "/tmp:/opt/app/data" \
        -e "PLAN_CLASS=$1" \
        -e "RECORD_COUNT=$2" \
        -e "DEPLOY_MODE=client" \
        -e "$driver_memory" \
        -e "$executor_memory" \
        -e "DATA_CATERER_VERSION=$data_caterer_version" \
        -e "ADDITIONAL_OPTS=$additional_conf" \
        datacatering/data-caterer:"$data_caterer_version";
    } 2>&1 | grep "real " | sed "$sed_option" "s/^.*real ([0-9\.]+)$/\1/")
    echo "Time taken: $time_taken"
    if [[ $1 == *BenchmarkForeignKeyPlanRun* ]]; then
      final_record_count=$(($2 * 5))
    else
      final_record_count=$2
    fi
    echo "$1:$3,$final_record_count,$num_run,$time_taken" >> "$benchmark_result_file"
  done
}

echo "Building jar with plan run"
./gradlew clean build -q
if [[ $? -ne 0 ]]; then
  echo "Failed to build!"
  exit 1
fi

echo "Pulling image before starting benchmarks"
docker pull datacatering/data-caterer:"$data_caterer_version"

echo "Running benchmarks"
if [[ "$enable_query_engine_run" == true ]]; then
  echo "Running Spark query execution engine benchmarks"
  for spark_qe in "${spark_query_execution_engines[@]}"; do
    echo "Running for Spark query execution engine: $spark_qe"
    run_docker "$default_job" "100000" "$spark_qe"
  done
fi

if [[ "$enable_data_size_run" ==  true ]]; then
  echo "Running data size benchmarks"
  for record_count in "${data_sizes[@]}"; do
    echo "Running for data size: $record_count"
    run_docker "$default_job" "$record_count"
  done
fi

if [[ "$enable_data_sink_run" ==  true ]]; then
  echo "Running data sink benchmarks"
  for job in "${job_names[@]}"; do
    echo "Running for job: $job"
    full_class_name="io.github.datacatering.plan.benchmark.$job"
    run_docker "$full_class_name" "$default_record_count"
  done
fi

echo "Printing logs of last failed docker run"
docker ps -a | grep -v "Exited (0)" | awk -F " " '{print $1}' | tail -1 | xargs docker logs
echo "Printing logs of last docker run"
docker ps -a | awk -F " " '{print $1}' | tail -1 | xargs docker logs

echo "Cleaning docker runs..."
docker ps -a | grep "datacatering/data-caterer" | awk -F " " '{print $1}' | xargs docker rm
echo "Done!"
