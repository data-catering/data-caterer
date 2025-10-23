---
title: "Guides"
description: "Guides to create Data Catering jobs. Generate batch and/or event data, validate data, read metadata or any other scenario."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Guides

Below are a list of guides you can follow to create your first Data Catering job for your use case.

## Data Sources

### Databases

<div class="grid cards" markdown>

- __:material-database: [BigQuery]__ - Generate/validate data for BigQuery
- __:material-database: [Cassandra]__ - Generate/validate data for Cassandra
- __:material-database: [MySQL]__ - Generate/validate data for MySQL
- __:material-database: [Postgres]__ - Generate/validate data for Postgres

</div>
  
  [BigQuery]: data-source/database/bigquery.md
  [Cassandra]: data-source/database/cassandra.md
  [MySQL]: data-source/database/mysql.md
  [Postgres]: data-source/database/postgres.md

### Files

<div class="grid cards" markdown>

- __:material-file: [CSV]__ - Generate/validate data for CSV
- __:material-file: [Delta Lake]__ - Generate/validate data for Delta Lake
- __:material-file: [Iceberg]__ - Generate/validate data for Iceberg tables
- __:material-file: [JSON]__ - Generate/validate data for JSON
- __:material-file: [ORC]__ - Generate/validate data for ORC
- __:material-file: [Parquet]__ - Generate/validate data for Parquet

</div>

  [CSV]: data-source/file/csv.md
  [Delta Lake]: data-source/file/delta-lake.md
  [Iceberg]: data-source/file/iceberg.md
  [JSON]: data-source/file/json.md
  [ORC]: data-source/file/orc.md
  [Parquet]: data-source/file/parquet.md

### HTTP

<div class="grid cards" markdown>

- __:material-api: [REST API]__ - Generate data for REST APIs

</div>

  [REST API]: data-source/http/http.md

### Messaging

<div class="grid cards" markdown>

- __:material-message: [Kafka]__ - Generate data for Kafka topics
- __:material-message: [Rabbitmq]__ - Generate data for Rabbitmq
- __:material-message: [Solace]__ - Generate data for Solace

</div>

  [Kafka]: data-source/messaging/kafka.md
  [Rabbitmq]: data-source/messaging/rabbitmq.md
  [Solace]: data-source/messaging/solace.md

### Metadata

<div class="grid cards" markdown>

- __:material-file: [Data Contract CLI]__ - Generate data based on metadata in data contract files in Data Contract CLI format
- __:material-check-all: [Great Expectations]__ - Use validations from Great Expectations for testing
- __:material-code-json: [JSON Schema]__ - Generate data based on JSON Schema definitions
- __:material-database-plus: [Marquez]__ - Generate data based on metadata in Marquez
- __:material-database-plus: [OpenMetadata]__ - Generate data based on metadata in OpenMetadata
- __:material-file: [Open Data Contract Standard (ODCS)]__ - Generate data based on metadata in data contract files in ODCS format
- __:simple-yaml: [YAML Configuration Files]__ - Reference existing YAML configuration files within Java/Scala API using YamlBuilder

</div>

  [Data Contract CLI]: data-source/metadata/data-contract-cli.md
  [Great Expectations]: data-source/metadata/great-expectations.md
  [JSON Schema]: data-source/metadata/json-schema.md
  [Marquez]: data-source/metadata/marquez.md
  [OpenMetadata]: data-source/metadata/open-metadata.md
  [Open Data Contract Standard (ODCS)]: data-source/metadata/open-data-contract-standard.md
  [YAML Configuration Files]: data-source/metadata/yaml-configurations.md


## Scenarios

<div class="grid cards" markdown>

- __[Auto Generate From Data Connection]__ - Automatically generating data from just defining data sources
- __[Data Generation]__ - Generate production-like data
- __[Data Validations]__ - Run data validations after generating data
- __[Delete Generated Data]__ - Delete the generated data whilst leaving other data
- __[First Data Generation]__ - If you are new, this is the place to start
- __[Foreign Keys Across Data Sources]__ - Generate matching values across generated data sets
- __[Generate Batch and Event Data]__ - Generate matching batch and event data
- __[Multiple Records Per Field Value]__ - How you can generate multiple records per set of fields

</div>

  [Auto Generate From Data Connection]: scenario/auto-generate-connection.md
  [Data Generation]: scenario/data-generation.md
  [Data Validations]: scenario/data-validation.md
  [Delete Generated Data]: scenario/delete-generated-data.md
  [First Data Generation]: scenario/first-data-generation.md
  [Foreign Keys Across Data Sources]: scenario/batch-and-event.md
  [Generate Batch and Event Data]: scenario/batch-and-event.md
  [Multiple Records Per Field Value]: scenario/records-per-field.md

## YAML Files

### Base Concept

The execution of the data generator is based on the concept of plans and tasks. A plan represent the set of tasks that
need to be executed,
along with other information that spans across tasks, such as foreign keys between data sources.  
A task represent the component(s) of a data source and its associated metadata so that it understands what the data
should look like
and how many steps (sub data sources) there are (i.e. tables in a database, topics in Kafka). Tasks can define one or
more steps.

### Plan

#### Foreign Keys

[Define foreign keys across data sources in your plan to ensure generated data can match](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/plan/foreign-key-example.yaml)  
[Link to associated task 1](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/file/json/json-account-task.yaml)  
[Link to associated task 2](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/jdbc/postgres/postgres-account-task.yaml)

### Task

| Data Source Type | Data Source | Sample Task                                                                                                                                  | Notes                                                             |
|------------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| Database         | Postgres    | [Sample](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/jdbc/postgres/postgres-account-task.yaml)   |                                                                   |
| Database         | MySQL       | [Sample](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/jdbc/mysql/mysql-account-task.yaml)         |                                                                   |
| Database         | Cassandra   | [Sample](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/cassandra/cassandra-customer-task.yaml)     |                                                                   |
| File             | CSV         | [Sample](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/file/csv/csv-transaction-task.yaml)         |                                                                   |
| File             | JSON        | [Sample](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/file/json/json-account-task.yaml)           | Contains nested schemas and use of SQL for generated values       |
| File             | Parquet     | [Sample](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/file/parquet/parquet-transaction-task.yaml) | Partition by year field                                           |
| Messaging System | Kafka       | [Sample](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/kafka/kafka-account-task.yaml)              | Specific base schema to be used, define headers, key, value, etc. |
| Messaging System | Solace      | [Sample](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/jms/solace/jms-account-task.yaml)           | JSON formatted message                                            |
| HTTP             | PUT         | [Sample](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/http/http-account-task.yaml)                | JSON formatted PUT body                                           |

### Configuration

[Basic configuration](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/application.conf)

## Docker-compose

To see how it runs against different data sources, you can run using `docker-compose` and set `DATA_SOURCE` like below

```shell
./gradlew build
cd docker
DATA_SOURCE=postgres docker-compose up -d datacaterer
```

Can set it to one of the following:

- postgres
- mysql
- cassandra
- solace
- kafka
- http
