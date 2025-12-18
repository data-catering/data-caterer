---
title: "Deployment"
description: "Data Caterer can be deployed/run as a Docker image or Helm chart."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Deployment

Two main ways to deploy and run Data Caterer:

- Docker
- Helm

## Docker

### Building Your Own Docker Image

Data Caterer provides a multi-stage Dockerfile that automatically builds your custom Scala/Java project and packages it with the Data Caterer runtime. You can use this as a template for your own projects.

The [example Dockerfile](https://github.com/data-catering/data-caterer/blob/main/example/Dockerfile) demonstrates a two-stage build:

1. **Build Stage**: Uses Gradle to compile your Scala/Java code and create a JAR
2. **Runtime Stage**: Copies the JAR into the Data Caterer base image

#### Using the Multi-Stage Dockerfile

The multi-stage approach offers several advantages:
- No need to pre-build the JAR locally
- Consistent build environment across different machines
- Optimized Docker layer caching for faster rebuilds
- Smaller final image size (build tools not included)

To build your own image:

```shell
# Build the Docker image (no pre-build needed)
docker build -t <my_image_name>:<my_image_tag> .

# Run your custom image
docker run -d \
  -e PLAN_CLASS=io.github.datacatering.plan.YourPlanClass \
  -e DEPLOY_MODE=client \
  <my_image_name>:<my_image_tag>
```

#### Customizing for Your Project

To adapt this for your own project, create a similar Dockerfile structure:

```dockerfile
# Stage 1: Build
FROM gradle:8.11.1-jdk17 AS builder
WORKDIR /build

# Copy build configuration
COPY gradle ./gradle
COPY gradlew gradlew.bat build.gradle.kts settings.gradle.kts* ./
COPY buildSrc ./buildSrc

# Download dependencies (cached layer)
RUN ./gradlew dependencies --no-daemon || true

# Copy source and build
COPY src ./src
RUN ./gradlew clean build --no-daemon

# Stage 2: Runtime
ARG DATA_CATERER_VERSION=0.17.0
FROM datacatering/data-caterer:${DATA_CATERER_VERSION}

# Copy your JAR (adjust path if needed)
COPY --from=builder --chown=app:app /build/build/libs/your-project.jar /opt/app/job.jar
```

#### Running with Java/Scala Locally

If you prefer to run locally without Docker, you can build and execute the JAR directly:

```shell
# Build the JAR
./gradlew clean build

# Run with Spark submit (requires Spark installation)
spark-submit \
  --class io.github.datacatering.plan.YourPlanClass \
  --master local[*] \
  build/libs/your-project.jar

# Or run via Gradle
./gradlew run --args="YourPlanClass"
```

#### Build Configuration Tips

- **Gradle Version**: The example uses Gradle 8.11.1 with JDK 17. Adjust based on your project needs.
- **Dependencies**: The `buildSrc` directory contains custom Gradle build logic - include it if present in your project.
- **JAR Name**: Update the `COPY` command in Stage 2 to match your JAR name from `build.gradle.kts`.
- **Data Caterer Version**: Change `DATA_CATERER_VERSION` to match your required version.

### Including Custom Transformers

If you have custom transformations (see [Transformations documentation](generator/transformation.md)), you can include them in your deployment:

#### Option 1: Volume Mount Custom JARs

Mount your transformer JAR(s) into the `/opt/app/custom` directory:

```shell
docker run -d \
  -v /path/to/transformers.jar:/opt/app/custom/transformers.jar \
  -v /path/to/plan:/opt/DataCaterer/plan \
  datacatering/data-caterer:0.17.0
```

#### Option 2: Build Image with Transformers

Include transformers in your custom Docker image:

```dockerfile
# Stage 1: Build transformers
FROM gradle:8.11.1-jdk17 AS builder
WORKDIR /build
COPY . .
RUN ./gradlew clean build --no-daemon

# Stage 2: Add to Data Caterer
ARG DATA_CATERER_VERSION=0.17.0
FROM datacatering/data-caterer:${DATA_CATERER_VERSION}

# Copy transformer JAR to custom directory (automatically in classpath)
COPY --from=builder --chown=app:app /build/build/libs/transformers.jar /opt/app/custom/
```

The `/opt/app/custom` directory is automatically included in the classpath, so any JARs placed there will be available to Data Caterer.

For detailed deployment instructions with transformers, see the [Transformations documentation](generator/transformation.md#deployment-with-custom-transformers).

### Docker Pre and Post Processing Scripts

Data Caterer supports running custom scripts before and after the main data generation process when deployed via Docker. This is useful for setup tasks, cleanup operations, notifications, or integrating with external systems.

#### Configuration

Configure pre and post processing scripts using environment variables:

| Environment Variable       | Description                                                | Default   |
| -------------------------- | ---------------------------------------------------------- | --------- |
| `PRE_PROCESSOR_SCRIPT`     | Path to script to run before Data Caterer execution        | (empty)   |
| `POST_PROCESSOR_SCRIPT`    | Path to script to run after Data Caterer execution         | (empty)   |
| `POST_PROCESSOR_CONDITION` | When to run post processor: `success`, `failure`, `always` | `success` |

#### Usage Example

```shell
docker run -d \
  -e PRE_PROCESSOR_SCRIPT="/opt/app/scripts/setup.sh" \
  -e POST_PROCESSOR_SCRIPT="/opt/app/scripts/cleanup.sh" \
  -e POST_PROCESSOR_CONDITION="always" \
  -v /path/to/scripts:/opt/app/scripts \
  datacatering/data-caterer:0.17.0
```

#### Script Execution Behavior

- **Pre-processor**: Runs before Data Caterer starts
  - If the script fails, Data Caterer execution is stopped
  - Script must be executable and return exit code 0 for success
- **Post-processor**: Runs after Data Caterer completes, based on condition:
  - `success`: Only runs if Data Caterer exit code is 0
  - `failure`: Only runs if Data Caterer exit code is non-zero
  - `always`: Runs regardless of Data Caterer exit code
  - If post-processor fails, the original Data Caterer exit code is preserved

#### Error Handling

- Scripts are executed with bash and include comprehensive error logging
- Missing script files generate warnings but don't stop execution
- All script output is logged with clear prefixes (pre/post processor)
- The final exit code is always Data Caterer's original exit code

## Helm

[Link to sample helm on GitHub here](https://github.com/data-catering/data-caterer/tree/main/example/helm/data-caterer)

Update
the [configuration](https://github.com/data-catering/data-caterer/blob/main/example/helm/data-caterer/templates/configuration.yaml)
to your own data connections and configuration or own image created from above.

```shell
git clone git@github.com:data-catering/data-caterer.git
    cd data-caterer/example
helm install data-caterer ./data-caterer/example/helm/data-caterer
```
