#!/usr/bin/env bash

# Usage: ./run.sh [--uber-jar] [class_name_or_yaml_file]
# 
# Options:
#   --uber-jar    Use the uber jar from app module (built with ./gradlew :app:shadowJar)
#                 instead of the example jar. Useful for testing YAML configurations.
#
# Examples:
#   ./run.sh simple-json.yaml              # Use example jar with YAML plan
#   ./run.sh --uber-jar simple-json.yaml   # Use uber jar with YAML plan
#   ./run.sh DocumentationPlanRun          # Use example jar with Scala class

# Resolve project root relative to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROP_FILE_LOCAL="$SCRIPT_DIR/gradle.properties"
PROP_FILE_ROOT="$REPO_ROOT/gradle.properties"

if [[ -f "$PROP_FILE_ROOT" ]]; then
  PROP_FILE="$PROP_FILE_ROOT"
else
  PROP_FILE="$PROP_FILE_LOCAL"
fi

data_caterer_version=$(grep -E "^version=" "$PROP_FILE" | cut -d= -f2)

if [[ -s ".tmp_prev_class_name" ]]; then
  prev_class_name=$(cat .tmp_prev_class_name)
else
  prev_class_name='DocumentationPlanRun'
fi

# Check if --uber-jar flag is provided
use_uber_jar=false
if [[ "$1" == "--uber-jar" ]]; then
  use_uber_jar=true
  shift
fi

if [[ $# -eq 0 ]]; then
  read -p "Class name of plan to run [$prev_class_name]: " class_name
  curr_class_name=${class_name:-$prev_class_name}
else
  curr_class_name=$1
fi

if [[ $curr_class_name == *".yaml"* ]]; then
  full_class_name="PLAN_FILE_PATH=/opt/app/custom/plan/$curr_class_name"
else
  full_class_name="PLAN_CLASS=io.github.datacatering.plan.$curr_class_name"
fi
echo -n "$curr_class_name" > .tmp_prev_class_name

if [[ "$use_uber_jar" == true ]]; then
  echo "Checking if uber jar exists"
  if [[ -f "$REPO_ROOT/app/build/libs/data-caterer.jar" ]]; then
    echo "Uber jar found, skipping build"
    jar_path="$REPO_ROOT/app/build/libs/data-caterer.jar"
  else
    echo "Uber jar not found, building"
    cd "$REPO_ROOT"
    ./gradlew :app:clean :app:shadowJar
    if [[ $? -ne 0 ]]; then
      echo "Failed to build uber jar!"
      exit 1
    fi
  fi
  cd "$SCRIPT_DIR"
  jar_path="$REPO_ROOT/app/build/libs/data-caterer.jar"
else
  echo "Building jar with plan run"
  ./gradlew clean build
  if [[ $? -ne 0 ]]; then
    echo "Failed to build!"
    exit 1
  fi
  jar_path="$(pwd)/build/libs/data-caterer-example.jar"
fi

docker network create --driver bridge docker_default || true

echo "Running Data Caterer via docker, version: $data_caterer_version"
docker network inspect insta-infra_default >/dev/null 2>&1 || docker network create --driver bridge insta-infra_default --label com.docker.compose.network="default"
DOCKER_CMD=(
  docker run -p 4040:4040
  -v "$jar_path:/opt/app/job.jar"
  -v "$(pwd)/docker/sample:/opt/app/data"
  -v "$(pwd)/docker/sample/tracking:/opt/app/record-tracking"
  -v "$(pwd)/docker/mount:/opt/app/mount"
  -v "$(pwd)/docker/data/custom:/opt/app/custom"
  -v "$(pwd)/docker/tmp:/tmp"
  -e "APPLICATION_CONFIG_PATH=/opt/app/custom/application.conf"
  -e "$full_class_name"
  -e "DEPLOY_MODE=client"
  --network "insta-infra_default"
  datacatering/data-caterer:"$data_caterer_version"
)

eval "${DOCKER_CMD[@]}"
if [[ $? != 0 ]]; then
  echo "Failed to run"
  exit 1
fi
echo "Finished!"
