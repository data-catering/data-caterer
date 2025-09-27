#!/usr/bin/env bash

version=$(grep version gradle.properties | cut -d= -f2)
platforms="linux/amd64,linux/arm64"

echo "Creating API jars and publishing, version=$version"
./gradlew clean :api:javadocJar :api:sourcesJar :api:shadowJar publishToSonatype closeAndReleaseSonatypeStagingRepository
publish_res=$?
if [[ "$publish_res" -ne 0 ]] ; then
  echo "Publish API jar failed, exiting"
#  exit 1
fi

echo "Creating data caterer fat jars, version=$version"
./gradlew clean :app:shadowJar
build_app=$?
if [[ "$build_app" -ne 0 ]] ; then
  echo "Failed to build app, exiting"
  exit 1
fi

docker run --privileged --rm tonistiigi/binfmt --install all
docker buildx create --use --name builder
docker buildx inspect --bootstrap builder

docker buildx build --platform $platforms \
  -t datacatering/data-caterer:$version --push .
