#!/usr/bin/env bash

# Determine version: use env var if set, otherwise get from gradle.properties with commit hash
if [ -n "$VERSION" ]; then
  version="$VERSION"
else
  base_version=$(grep version gradle.properties | cut -d= -f2)
  commit_hash=$(git rev-parse --short HEAD)

  # Check if on main branch
  if [[ "$GITHUB_REF" == "refs/heads/main" ]]; then
    version="$base_version"
  else
    version="$base_version-$commit_hash"
  fi
fi

platforms="linux/amd64,linux/arm64"

echo "Building version: $version"
echo "Branch/Ref: ${GITHUB_REF:-local}"

# Only publish to Maven on main branch
if [[ "$GITHUB_REF" == "refs/heads/main" ]]; then
  echo "Creating API jars and publishing to Maven, version=$version"
  ./gradlew clean :api:javadocJar :api:sourcesJar :api:shadowJar publishToSonatype closeAndReleaseSonatypeStagingRepository
  publish_res=$?
  if [[ "$publish_res" -ne 0 ]] ; then
    echo "Publish API jar failed, exiting"
  #  exit 1
  fi
else
  echo "Feature branch detected - skipping Maven publish"
  echo "Creating API jars locally, version=$version"
  ./gradlew clean :api:javadocJar :api:sourcesJar :api:shadowJar
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

# Build and tag Docker images
echo "Building and pushing Docker image with tag: $version"
docker buildx build --platform $platforms \
  -t datacatering/data-caterer:$version --push .

# Also tag as 'latest' only for main branch
if [[ "$GITHUB_REF" == "refs/heads/main" ]]; then
  echo "Main branch - also tagging as 'latest'"
  docker buildx build --platform $platforms \
    -t datacatering/data-caterer:latest --push .
else
  echo "Feature branch - skipping 'latest' tag"
fi
