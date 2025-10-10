---
title: "Deployment"
description: "Data Caterer can be deployed/run as an application, docker image or helm chart."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Deployment

Three main ways to deploy and run Data Caterer:

- Application
- Docker
- Helm

## Application

Run the OS native application from [downloading the specific OS application here](../get-started/quick-start.md#quick-start).

## Docker

To package up your class along with the Data Caterer base image, you can follow
the [Dockerfile that is created for you here](https://github.com/data-catering/data-caterer/blob/main/example/Dockerfile).

Then you can run the following:

```shell
./gradlew clean build
docker build -t <my_image_name>:<my_image_tag> .
```

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
  datacatering/data-caterer:0.16.11
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
