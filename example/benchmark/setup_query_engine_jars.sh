#!/usr/bin/env bash

spark_major_version=$(grep sparkMajorVersion gradle.properties | cut -d= -f2)

mkdir -p benchmark/jars

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
