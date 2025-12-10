---
title: "Quick start"
description: "Quick start for Data Catering data generation and testing tool that can automatically discover, generate and validate for files, databases, HTTP APIs and messaging systems."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Run Data Caterer

## Quick start

<div class="grid cards" markdown>

-   :material-language-java: :simple-scala: __[Java/Scala]__

    ---

    Instructions for using Java/Scala API via Docker

-   :simple-yaml: __[YAML]__

    ---

    Instructions for using YAML via Docker

-   :material-docker: __[UI App - Docker]__

    ---

    Instructions for Docker download

-   :material-apple: __[UI App - Mac]__

    ---

    Instructions for Mac download

-   :material-microsoft-windows: __[UI App - Windows]__

    ---

    Instructions for Windows download

-   :material-linux: __[UI App - Linux]__

    ---

    Instructions for Linux download

</div>

  [Java/Scala]: #javascala-api
  [YAML]: #yaml
  [UI App - Docker]: #docker
  [UI App - Mac]: #mac
  [UI App - Linux]: #linux
  [UI App - Windows]: #windows

### Java/Scala API

```shell
git clone git@github.com:data-catering/data-caterer.git
cd example && ./run.sh
#check results under example/docker/sample/report/index.html folder
#If you want to run any other examples, check the class names under src/scala or src/java
#And then run with ./run.sh <class_name>
#i.e. ./run.sh CsvPlan
```

### YAML

```shell
git clone git@github.com:data-catering/data-caterer.git
cd example && ./run.sh simple-json.yaml
#check results under example/docker/sample/report/index.html folder
#check example YAML files under:
# - docker/data/custom/plan
# - docker/data/custom/task
# - docker/data/custom/validation
#If you want to run any other examples, check the files under docker/data/custom/plan
#And then run with ./run.sh <file_name>
#i.e. ./run.sh parquet.yaml
```

### Docker

1. Docker
   ```shell
   docker run -d -i -p 9898:9898 -e DEPLOY_MODE=standalone --name datacaterer datacatering/data-caterer:0.18.0
   ```
2. [Open localhost:9898](http://localhost:9898)

### Mac

Choose the download for your Mac architecture:

- **Intel (x86_64)**: [Download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-macos-x86_64.zip)
- **Apple Silicon (M1/M2/M3)**: [Download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-macos-aarch64.zip)

1. Download the appropriate version for your Mac
2. Drag Data Caterer to your Applications folder and double-click to run
3. If your browser doesn't open, go to [http://localhost:9898](http://localhost:9898) in your preferred browser

### Windows

1. [Windows x64 download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-windows-x86_64.zip)
2. After downloading, go to 'Downloads' folder and 'Extract All' from data-caterer-windows-x86_64
3. Double-click the installer to install Data Caterer
4. Click on 'More info' then at the bottom, click 'Run anyway'
5. Go to '/Program Files/DataCaterer' folder and run DataCaterer application
6. If your browser doesn't open, go to [http://localhost:9898](http://localhost:9898) in your preferred browser

### Linux

Choose the download for your Linux architecture:

- **amd64 (x86_64)**: [Download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-linux-amd64.zip)
- **arm64 (aarch64)**: [Download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-linux-arm64.zip)

1. Download the appropriate version for your Linux system
2. Extract and install the debian package
3. If your browser doesn't open, go to [http://localhost:9898](http://localhost:9898) in your preferred browser

#### Report

Check the report generated under `example/docker/data/custom/report/index.html`.

[**Sample report can also be seen here**](../sample/report/html/index.html).

## Gradual start

If you prefer a step-by-step approach to learning the capabilities of Data Caterer, there are a number of guides that
take you through the various data sources and approaches that can be taken when using the tool.

- [**Check out the starter guide here**](../docs/guide/scenario/first-data-generation.md) that will take your through
step by step
- You can also check the other guides [**here**](../docs/guide/index.md) to see the other possibilities of
what Data Caterer can achieve for you.
