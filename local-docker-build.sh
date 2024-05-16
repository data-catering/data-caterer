#!/usr/bin/env bash

version=$(grep version gradle.properties | cut -d= -f2)
sparkVersion=$(grep sparkVersion gradle.properties | cut -d= -f2)

echo "Creating API jar"
gradle clean :api:shadowJar

echo "Creating data caterer jar, version=$version"
gradle build shadowJar -x test
build_app=$?
if [[ "$build_app" -ne 0 ]] ; then
  echo "Failed to build app, exiting"
  exit 1
fi

docker build \
  --build-arg "APP_VERSION=$version" \
  --build-arg "SPARK_VERSION=$sparkVersion" \
  -t datacatering/data-caterer-basic:$version .
