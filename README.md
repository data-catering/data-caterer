# Data Caterer - Test Data Management Tool

![Data Catering](misc/banner/logo_landscape_banner.svg)

## Overview

A test data management tool with automated data generation, validation and cleanup.

[Generate data](https://data.catering/setup/generator/data-generator/) for databases, files, messaging systems or HTTP 
requests via UI, Scala/Java SDK or YAML input and executed via Spark. Run 
[data validations](https://data.catering/setup/validation/) after generating data to ensure it is consumed correctly. 
[Clean up generated data or consumed data](https://data.catering/setup/delete-data/) in downstream data sources to keep 
your environments tidy. [Define alerts](https://data.catering/setup/report/alert/) to get notified when failures occur 
and deep dive into issues [from the generated report](https://data.catering/sample/report/html/).

[**Full docs can be found here**](https://data.catering/setup/).
  
[**A demo of the UI found here**](https://data.catering/sample/ui/index.html).
  
[**Scala/Java examples found here**](https://github.com/data-catering/data-caterer-example).

## Features

- [Batch and/or event data generation](https://data.catering/setup/connection/)
- [Maintain relationships across any dataset](https://data.catering/setup/foreign-key/)
- [Create custom data generation/validation scenarios](https://data.catering/setup/generator/data-generator/)
- [Data validation](https://data.catering/setup/validation/)
- [Clean up generated and downstream data](https://data.catering/setup/delete-data/)
- [Suggest data validations](https://data.catering/setup/validation/)
- [Metadata discovery](https://data.catering/setup/guide/scenario/auto-generate-connection/)
- [Detailed report of generated data and validation results](https://data.catering/sample/report/html/)
- [Alerts to be notified of results](https://data.catering/setup/report/alert/)

![Basic flow](design/basic_data_caterer_flow_medium.gif)

## Quick start

1. [Mac download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-mac.zip)
2. [Windows download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-windows.zip)
   1. After downloading, go to 'Downloads' folder and 'Extract All' from data-caterer-windows
   2. Double-click 'DataCaterer-1.0.0' to install Data Caterer
   3. Click on 'More info' then at the bottom, click 'Run anyway'
   4. Go to '/Program Files/DataCaterer' folder and run DataCaterer application
   5. If your browser doesn't open, go to [http://localhost:9898](http://localhost:9898) in your preferred browser
3. [Linux download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-linux.zip)
4. Docker
   ```shell
   docker run -d -i -p 9898:9898 -e DEPLOY_MODE=standalone --name datacaterer datacatering/data-caterer:0.10.4
   ```
   [Open localhost:9898](http://localhost:9898).

### Run Scala/Java examples

```shell
git clone git@github.com:data-catering/data-caterer-example.git
cd data-caterer-example && ./run.sh
#check results under docker/sample/report/index.html folder
```

## Integrations

### Supported data sources

Data Caterer supports the below data sources. Additional data sources can be added on a demand basis. [Check here for 
the full roadmap](#roadmap).

| Data Source Type | Data Source         | Support | Free |
|------------------|---------------------|---------|------|
| Cloud Storage    | AWS S3              | ✅       | ✅    |
| Cloud Storage    | GCP Cloud Storage   | ✅       | ✅    |
| Cloud Storage    | Azure Blob Storage  | ✅       | ✅    |
| Database         | Postgres            | ✅       | ✅    |
| Database         | MySQL               | ✅       | ✅    |
| Database         | Cassandra           | ✅       | ✅    |
| Database         | MongoDB             | ❌       | ✅    |
| Database         | Elasticsearch       | ❌       | ✅    |
| File             | CSV                 | ✅       | ✅    |
| File             | JSON                | ✅       | ✅    |
| File             | ORC                 | ✅       | ✅    |
| File             | Parquet             | ✅       | ✅    |
| File             | Hudi                | ❌       | ✅    |
| File             | Iceberg             | ❌       | ✅    |
| File             | Delta Lake          | ❌       | ✅    |
| HTTP             | REST API            | ✅       | ❌    |
| Messaging        | Kafka               | ✅       | ❌    |
| Messaging        | Solace              | ✅       | ❌    |
| Messaging        | Pulsar              | ❌       | ❌    |
| Messaging        | RabbitMQ            | ❌       | ❌    |
| Messaging        | ActiveMQ            | ❌       | ❌    |
| Metadata         | Marquez             | ✅       | ❌    |
| Metadata         | OpenMetadata        | ✅       | ❌    |
| Metadata         | OpenAPI/Swagger     | ✅       | ❌    |
| Metadata         | Great Expectations  | ✅       | ❌    |
| Metadata         | Amundsen            | ❌       | ❌    |
| Metadata         | Datahub             | ❌       | ❌    |
| Metadata         | Solace Event Portal | ❌       | ❌    |


## Supported use cases

1. Insert into single data sink
2. Insert into multiple data sinks
   1. Foreign keys associated between data sources
   2. Number of records per column value
3. Set random seed at column and whole data generation level
4. Generate real-looking data (via DataFaker) and edge cases
   1. Names, addresses, places etc.
   2. Edge cases for each data type (e.g. newline character in string, maximum integer, NaN, 0)
   3. Nullability
5. Send events progressively
6. Automatically insert data into data source
   1. Read metadata from data source and insert for all sub data sources (e.g. tables)
   2. Get statistics from existing data in data source if exists
7. Track and delete generated data
8. Extract data profiling and metadata from given data sources
   1. Calculate the total number of combinations
9. Validate data
   1. Basic column validations (not null, contains, equals, greater than)
   2. Aggregate validations (group by account_id and sum amounts should be less than 100, each account should have at
      least one transaction)
   3. Upstream data source validations (generate data and then check same data is inserted in another data source with
      potential transformations)
   4. Column name validations (check count and ordering of column names)
10. Data migration validations
    1. Ensure row counts are equal
    2. Check both data sources have same values for key columns

## Run Configurations

Different ways to run Data Caterer based on your use case:

![Types of run configurations](design/high_level_flow-run-config.svg)

## Sponsorship

Data Caterer is set up under a sponsorware model where all features are available to sponsors. The core features
are available here in this project for all to use/fork/update/improve etc., as the open core.

Sponsors have access to the following features:

- All data sources (see [here for all data sources](https://data.catering/setup/connection/))
- Batch and Event generation
- [Auto generation from data connections or metadata sources](https://data.catering/setup/guide/scenario/auto-generate-connection/)
- Suggest data validations
- [Clean up generated and consumed data](https://data.catering/setup/guide/scenario/delete-generated-data/)
- Run as many times as you want, not charged by usage
- Metadata discovery
- [Plus more to come](#roadmap)

[Find out more details here to help with sponsorship.](https://data.catering/sponsor)

This is inspired by the [mkdocs-material project](https://github.com/squidfunk/mkdocs-material) which
[follows the same model](https://squidfunk.github.io/mkdocs-material/insiders/).

## Contributing

[View details here about how you can contribute to the project.](CONTRIBUTING.md)

## Additional Details

### Design

[Design motivations and details can be found here.](https://data.catering/setup/design)

### Roadmap

[Can check here for full list.](https://data.catering/use-case/roadmap/)

#### UI

1. Allow the application to run with UI enabled
2. Runs as a long-lived app with UI that interacts with the existing app as a single container
3. Ability to run as UI, Spark job or both
4. Persist data in files or database (Postgres)
5. UI will show the history of data generation/validation runs, delete generated data, create new scenarios, define data connections

#### Distribution

##### Docker

```shell
gradle clean :api:shadowJar :app:shadowJar
docker build --build-arg "APP_VERSION=0.7.0" --build-arg "SPARK_VERSION=3.5.0" --no-cache -t datacatering/data-caterer:0.7.0 .
docker run -d -i -p 9898:9898 -e DEPLOY_MODE=standalone -v data-caterer-data:/opt/data-caterer --name datacaterer datacatering/data-caterer:0.7.0
#open localhost:9898
```

##### Jpackage

```bash
JPACKAGE_BUILD=true gradle clean :api:shadowJar :app:shadowJar
# Mac
jpackage "@misc/jpackage/jpackage.cfg" "@misc/jpackage/jpackage-mac.cfg"
# Windows
jpackage "@misc/jpackage/jpackage.cfg" "@misc/jpackage/jpackage-windows.cfg"
# Linux
jpackage "@misc/jpackage/jpackage.cfg" "@misc/jpackage/jpackage-linux.cfg"
```

##### Java 17 VM Options

```shell
--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
```