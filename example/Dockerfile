# Multi-stage Dockerfile for Data Caterer custom projects
# Stage 1: Build the example JAR using Gradle
FROM gradle:9.1.0-jdk17 AS builder

# Set working directory
WORKDIR /build

# Copy Gradle wrapper and configuration files first (for better caching)
COPY gradle ./gradle
COPY gradlew gradlew.bat build.gradle.kts settings.gradle.kts* ./
COPY buildSrc ./buildSrc

# Download dependencies (cached layer if build files haven't changed)
RUN ./gradlew dependencies --no-daemon || true

# Copy source code
COPY src ./src

# Build the JAR
RUN ./gradlew clean build --no-daemon

# Stage 2: Runtime image based on Data Caterer
ARG DATA_CATERER_VERSION=0.18.0
FROM datacatering/data-caterer:${DATA_CATERER_VERSION}

# Copy the built JAR from the builder stage
COPY --from=builder --chown=app:app /build/build/libs/data-caterer-example.jar /opt/app/job.jar
