# Data Caterer - Test Data Management Tool

## Overview

A test data management tool with automated data generation, validation and clean up.

![Basic data flow for Data Caterer](misc/design/high_level_flow-run-config-basic-flow.svg)

[Generate data](https://data.catering/latest/docs/generator/data-generator/) for databases, files, messaging systems or HTTP 
requests via UI, Scala/Java SDK or YAML input and executed via Spark. Run 
[data validations](https://data.catering/latest/docs/validation/) after generating data to ensure it is consumed correctly. 
[Clean up generated data or consumed data](https://data.catering/latest/docs/delete-data/) in downstream data sources to keep 
your environments tidy. [Define alerts](https://data.catering/latest/docs/report/alert/) to get notified when failures occur 
and deep dive into issues [from the generated report](https://data.catering/latest/docs/report/html-report/).

[**Full docs can be found here**](https://data.catering/latest/docs/).
  
[**Scala/Java examples found here**](https://github.com/data-catering/data-caterer-example).
  
[**A demo of the UI found here**](https://data.catering/latest/sample/ui/).

## Features

- [Batch and/or event data generation](https://data.catering/latest/docs/connection/)
- [Maintain relationships across any dataset](https://data.catering/latest/docs/generator/foreign-key/)
- [Create custom data generation/validation scenarios](https://data.catering/latest/docs/generator/data-generator/)
- [Data validation](https://data.catering/latest/docs/validation/)
- [Clean up generated and downstream data](https://data.catering/latest/docs/delete-data/)
- [Suggest data validations](https://data.catering/latest/docs/validation/)
- [Metadata discovery](https://data.catering/latest/docs/guide/scenario/auto-generate-connection/)
- [Detailed report of generated data and validation results](https://data.catering/latest/docs/report/html-report/)
- [Alerts to be notified of results](https://data.catering/latest/docs/report/alert/)
- [Run as GitHub Action](https://github.com/data-catering/insta-integration)

![Basic flow](misc/design/basic_data_caterer_flow_medium.gif)

## Quick start

1. Docker
   ```shell
   docker run -d -i -p 9898:9898 -e DEPLOY_MODE=standalone --name datacaterer datacatering/data-caterer:0.16.11
   ```
   [Open localhost:9898](http://localhost:9898).
1. [Run Scala/Java examples](#run-scalajava-examples)
   ```shell
   git clone git@github.com:data-catering/data-caterer-example.git
   cd data-caterer-example && ./run.sh
   #check results under docker/sample/report/index.html folder
   ```
1. [UI App: Mac download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-mac.zip)
1. [UI App: Windows download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-windows.zip)
   1. After downloading, go to 'Downloads' folder and 'Extract All' from data-caterer-windows
   1. Double-click 'DataCaterer-1.0.0' to install Data Caterer
   1. Click on 'More info' then at the bottom, click 'Run anyway'
   1. Go to '/Program Files/DataCaterer' folder and run DataCaterer application
   1. If your browser doesn't open, go to [http://localhost:9898](http://localhost:9898) in your preferred browser
1. [UI App: Linux download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-linux.zip)

[Follow quick start instructions from here if you want more details](https://data.catering/latest/get-started/quick-start/).


## Integrations

### Supported data sources

Data Caterer supports the below data sources. [Check here for the full roadmap](#roadmap).

| Data Source Type | Data Source                        | Support |
|------------------|------------------------------------|---------|
| Cloud Storage    | AWS S3                             | ✅       |
| Cloud Storage    | Azure Blob Storage                 | ✅       |
| Cloud Storage    | GCP Cloud Storage                  | ✅       |
| Database         | BigQuery                           | ✅       |
| Database         | Cassandra                          | ✅       |
| Database         | MySQL                              | ✅       |
| Database         | Postgres                           | ✅       |
| Database         | Elasticsearch                      | ❌       |
| Database         | MongoDB                            | ❌       |
| File             | CSV                                | ✅       |
| File             | Delta Lake                         | ✅       |
| File             | JSON                               | ✅       |
| File             | Iceberg                            | ✅       |
| File             | ORC                                | ✅       |
| File             | Parquet                            | ✅       |
| File             | Hudi                               | ❌       |
| HTTP             | REST API                           | ✅       |
| Messaging        | Kafka                              | ✅       |
| Messaging        | RabbitMQ                           | ✅       |
| Messaging        | Solace                             | ✅       |
| Messaging        | ActiveMQ                           | ❌       |
| Messaging        | Pulsar                             | ❌       |
| Metadata         | Data Contract CLI                  | ✅       |
| Metadata         | Great Expectations                 | ✅       |
| Metadata         | JSON Schema                        | ✅       |
| Metadata         | Marquez                            | ✅       |
| Metadata         | OpenAPI/Swagger                    | ✅       |
| Metadata         | OpenMetadata                       | ✅       |
| Metadata         | Open Data Contract Standard (ODCS) | ✅       |
| Metadata         | Amundsen                           | ❌       |
| Metadata         | Datahub                            | ❌       |
| Metadata         | Solace Event Portal                | ❌       |


## Additional Details

## Run Configurations

Different ways to run Data Caterer based on your use case:

![Types of run configurations](misc/design/high_level_flow-run-config.svg)

### Design

[Design motivations and details can be found here.](https://data.catering/latest/docs/design/)

### Roadmap

[Can check here for full list of roadmap items.](https://data.catering/latest/use-case/roadmap/)

## Pricing

Data Caterer is set up under a usage pricing model for the latest application version. There are different pricing tiers based on how much you use Data Caterer. This also includes support and requesting features. The current open-source version will be kept for those who want to continue using the open-source version.

[Find out more details here.](https://data.catering/latest/pricing/)

### Mildly Quick Start

#### Generate and validate data

##### [I want to generate data in Postgres](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/PostgresPlanRun.scala)

```scala
postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")  //name and url
```

##### [But I want `account_id` to follow a pattern and be unique](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/PostgresPlanRun.scala)

```scala
postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .fields(field.name("account_id").regex("ACC[0-9]{10}").unique(true))
```

##### [I then want to test my job ingests all the data after generating](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/ValidationPlanRun.scala)

```scala
val postgresTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .fields(field.name("account_id").regex("ACC[0-9]{10}").unique(true))

val parquetValidation = parquet("output_parquet", "/data/parquet/customer")
  .validation(validation.count.isEqual(1000))
```

##### [I want to make sure all the `account_id` values in Postgres are in the Parquet file](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/ValidationPlanRun.scala)

```scala
val postgresTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .fields(field.name("account_id").regex("ACC[0-9]{10}").unique(true))

val parquetValidation = parquet("output_parquet", "/data/parquet/customer")
  .validation(
     validation.upstreamData(postgresTask)
       .joinFields("account_id")
       .withValidation(validation.count().isEqual(1000))
  )
```

##### [I want to start validating once the Parquet file is available](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/ValidationPlanRun.scala)

```scala
val postgresTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .fields(field.name("account_id").regex("ACC[0-9]{10}").unique(true))

val parquetValidation = parquet("output_parquet", "/data/parquet/customer")
  .validation(
     validation.upstreamData(postgresTask)
       .joinFields("account_id")
       .withValidation(validation.count().isEqual(1000))
  )
  .validationWait(waitCondition.file("/data/parquet/customer"))
```

#### Generate same data across data sources

##### [I also want to generate events in Kafka](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/AdvancedKafkaPlanRun.scala)

```scala
kafka("my_kafka", "localhost:29092")
  .topic("account-topic")
  .fields(...)
```

##### [But I want the same `account_id` to show in Postgres and Kafka](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/AdvancedBatchEventPlanRun.scala)

```scala
val postgresTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .fields(field.name("account_id").regex("ACC[0-9]{10}"))

val kafkaTask = kafka("my_kafka", "localhost:29092")
  .topic("account-topic")
  .fields(...)

plan.addForeignKeyRelationship(
   postgresTask, List("account_id"),
   List(kafkaTask -> List("account_id"))
)
```

#### Generate data and clean up

##### [I want to generate 5 transactions per `account_id`](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/MultipleRecordsPerColPlan.scala)

```scala
postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .table("account", "transactions")
  .count(count.recordsPerField(5, "account_id"))
```

##### [Randomly generate 1 to 5 transactions per `account_id`](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/MultipleRecordsPerColPlan.scala)

```scala
postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .table("account", "transactions")
  .count(count.recordsPerFieldGenerator(generator.min(1).max(5), "account_id"))
```

##### [I want to delete the generated data](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/AdvancedDeletePlanRun.scala)

```scala
val postgresTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .table("account", "transactions")
  .count(count.recordsPerFieldGenerator(generator.min(0).max(5), "account_id"))

val conf = configuration
  .enableDeleteGeneratedRecords(true)
  .enableGenerateData(false)
```

##### [I also want to delete the data in Cassandra because my job consumed the data in Postgres and pushed to Cassandra](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/AdvancedDeletePlanRun.scala)

```scala
val postgresTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .table("account", "transactions")
  .count(count.recordsPerFieldGenerator(generator.min(0).max(5), "account_id"))

val cassandraTxns = cassandra("ingested_data", "localhost:9042")
  .table("account", "transactions")

val deletePlan = plan.addForeignKeyRelationship(
   postgresTask, List("account_id"),
   List(),
   List(cassandraTxns -> List("account_id"))
)

val conf = configuration
  .enableDeleteGeneratedRecords(true)
  .enableGenerateData(false)
```

##### [But only the `account_number` is saved in Cassandra from the `account_id`](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/AdvancedDeletePlanRun.scala)

```scala
val postgresTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .count(count.recordsPerFieldGenerator(generator.min(0).max(5), "account_id"))

val cassandraTxns = cassandra("ingested_data", "localhost:9042")
  .table("account", "transactions")

val deletePlan = plan.addForeignKeyRelationship(
   postgresTask, List("account_id"),
   List(),
   List(cassandraTxns -> List("SUBSTR(account_id, 3) AS account_number"))
)

val conf = configuration
  .enableDeleteGeneratedRecords(true)
  .enableGenerateData(false)
```

#### Generate data with schema from metadata source

##### [I have a data contract using the Open Data Contract Standard (ODCS) format](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/AdvancedODCSPlanRun.scala)

```scala
parquet("customer_parquet", "/data/parquet/customer")
  .fields(metadataSource.openDataContractStandard("/data/odcs/full-example.odcs.yaml"))
```

##### [I have an OpenAPI/Swagger doc](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/AdvancedHttpPlanRun.scala)

```scala
http("my_http")
  .fields(metadataSource.openApi("/data/http/petstore.json"))
```

#### Validate data using validations from metadata source

##### [I have expectations from Great Expectations](https://github.com/data-catering/data-caterer-example/blob/b0f03fb26f185ec8613241205b998aef1d5f5a01/src/main/scala/io/github/datacatering/plan/AdvancedGreatExpectationsPlanRun.scala)

```scala
parquet("customer_parquet", "/data/parquet/customer")
  .validations(metadataSource.greatExpectations("/data/great-expectations/taxi-expectations.json"))
```

