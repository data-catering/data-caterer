#!/usr/bin/env bash

# Test script for multi-stage Docker build
# Usage: ./test-docker-build.sh [image_tag]

set -e

IMAGE_TAG="${1:-data-caterer-example:latest}"

echo "========================================="
echo "Testing Multi-Stage Dockerfile Build"
echo "========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ ERROR: Docker daemon is not running"
    echo "Please start Docker and try again"
    exit 1
fi

echo "✓ Docker daemon is running"
echo ""

# Build the image
echo "Building Docker image: $IMAGE_TAG"
echo "This will:"
echo "  1. Use Gradle to build the JAR in a builder container"
echo "  2. Copy the JAR to the Data Caterer runtime image"
echo ""

docker build -t "$IMAGE_TAG" .

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================="
    echo "✓ Build successful!"
    echo "========================================="
    echo ""
    echo "Image details:"
    docker images "$IMAGE_TAG"
    echo ""
    echo "To run the image:"
    echo "  docker run -d \\"
    echo "    -e PLAN_CLASS=io.github.datacatering.plan.YourPlanClass \\"
    echo "    -e DEPLOY_MODE=client \\"
    echo "    $IMAGE_TAG"
    echo ""
else
    echo ""
    echo "❌ Build failed!"
    exit 1
fi
