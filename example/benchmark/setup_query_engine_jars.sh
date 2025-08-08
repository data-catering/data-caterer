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

spark_major_version=$(grep -E "^sparkMajorVersion=" "$PROP_FILE" | cut -d= -f2)

mkdir -p benchmark/jars

# Download jars for the appropriate Spark major version (placeholder logic)
echo "Using Spark major version: $spark_major_version"

echo "Downloading query engine jars with Spark version: ${spark_major_version}"
echo "Getting blaze jar..."
curl -L -o benchmark/jars/blaze.jar "https://github.com/kwai/blaze/releases/download/v2.0.9.1/blaze-engine-spark333-release-2.0.9.1-SNAPSHOT.jar"
echo "Finished downloading blaze jar"

echo "Getting gluten jar..."
curl -L -o benchmark/jars/gluten.jar "https://github.com/apache/incubator-gluten/releases/download/v1.1.1/gluten-velox-bundle-spark3.4_2.12-1.1.1.jar"
echo "Finished downloading gluten jar"

echo "Getting datafusion-comet jar..."
curl -L -o benchmark/jars/comet.jar "https://github.com/data-catering/datafusion-comet/releases/download/0.1.0/comet-spark-spark3.4_2.12-0.1.0-SNAPSHOT.jar"
echo "Finished downloading datafusion-comet jar"

#echo "Building datafusion-comet..."
##git clone git@github.com:apache/datafusion-comet.git benchmark/build
#cd benchmark/build/datafusion-comet
#make release PROFILES="-Pspark-${spark_major_version}"
#ls spark/target/comet-spark-*-SNAPSHOT.jar
#cp spark/target/comet-spark-*-SNAPSHOT.jar ../../jars/comet.jar
#cd ../../..
#echo "Finished building datafusion-comet"
