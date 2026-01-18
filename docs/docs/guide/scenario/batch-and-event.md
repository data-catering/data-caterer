---
title: "Automated batch and event data generation and validation tool"
description: "Example of automatically discovering, generating and validating for batch (i.e. CSV file) and event systems (i.e. Kafka)."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Generate Batch and Event Data

Creating a data generator for Kafka topic with matching records in a CSV file.

## Requirements

- 5 minutes
- Git
- Gradle
- Docker

## Get Started

First, we will clone the data-caterer repo and navigate to the example folder which will already have the base project setup required.

=== "Java"

    ```shell
    git clone git@github.com:data-catering/data-caterer.git
    cd data-caterer/example
    ```

=== "Scala"

    ```shell
    git clone git@github.com:data-catering/data-caterer.git
    cd data-caterer/example
    ```

=== "YAML"

    ```shell
    git clone git@github.com:data-catering/data-caterer.git
    cd data-caterer/example
    ```

=== "UI"

    [Run Data Caterer UI via the 'Quick Start' found here.](../../../get-started/quick-start.md)

### Kafka Setup

If you don't have your own Kafka up and running, you can set up and run an instance configured in the `docker`
folder via.

```shell
cd docker
docker-compose up -d kafka
docker exec docker-kafkaserver-1 kafka-topics --bootstrap-server localhost:9092 --list
```

Let's create a task for inserting data into the `account-topic` that is already defined
under`docker/data/kafka/setup_kafka.sh`.

### Plan Setup

Create a new Java or Scala class.

- Java: `src/main/java/io/github/datacatering/plan/MyAdvancedBatchEventJavaPlanRun.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyAdvancedBatchEventPlanRun.scala`

Make sure your class extends `PlanRun`.

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    ...
    
    public class MyAdvancedBatchEventJavaPlanRun extends PlanRun {
        {
            var kafkaTask = new AdvancedKafkaJavaPlanRun().getKafkaTask();
        }
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    ...
    
    class MyAdvancedBatchEventPlanRun extends PlanRun {
      val kafkaTask = new AdvancedKafkaPlanRun().kafkaTask
    }
    ```

=== "YAML"

    Create file `docker/data/custom/unified/batch-event.yaml`:
    ```yaml
    name: "batch_event_plan"
    description: "Generate batch and event data"

    dataSources:
      - name: "my_kafka"
        connection:
          type: "kafka"
          options:
            url: "localhost:9092"
        steps:
          - name: "account_topic"
            fields:
              - name: "key"
              - name: "tmp_year"
              - name: "tmp_name"
              - name: "value"
      - name: "my_csv"
        connection:
          type: "csv"
          options:
            path: "/opt/app/data/csv/account"
        steps:
          - name: "csv_accounts"
            fields:
              - name: "account_number"
              - name: "year"
              - name: "name"
              - name: "payload"
    ```

We will borrow the Kafka task that is already defined under the class `KafkaPlanRun`
or `KafkaJavaPlanRun`. You can go through the Kafka guide [**here**](../data-source/messaging/kafka.md) for more details.

#### Schema

Let us set up the corresponding schema for the CSV file where we want to match the values that are generated for the
Kafka messages.

=== "Java"

    ```java
    var kafkaTask = new AdvancedKafkaJavaPlanRun().getKafkaTask();
    
    var csvTask = csv("my_csv", "/opt/app/data/csv/account")
            .fields(
                    field().name("account_number"),
                    field().name("year"),
                    field().name("name"),
                    field().name("payload")
            );
    ```

=== "Scala"

    ```scala
    val kafkaTask = new AdvancedKafkaPlanRun().kafkaTask

    val csvTask = csv("my_csv", "/opt/app/data/csv/account")
      .fields(
        field.name("account_number"),
        field.name("year"),
        field.name("name"),
        field.name("payload")
    )
    ```

=== "YAML"

    In a unified YAML file:
    ```yaml
    dataSources:
      - name: "my_csv"
        connection:
          type: "csv"
          options:
            path: "/opt/app/data/csv/account"
        steps:
          - name: "csv_accounts"
            fields:
              - name: "account_number"
              - name: "year"
              - name: "name"
              - name: "payload"
    ```

This is a simple schema where we want to use the values and metadata that is already defined in the `kafkaTask` to
determine what the data will look like for the CSV file. Even if we defined some metadata here, it would be overridden
when we define our foreign key relationships.

#### Foreign Keys

From the above CSV schema, we see note the following against the Kafka schema:

- `account_number` in CSV needs to match with the `account_id` in Kafka
    - We see that `account_id` is referred to in the `key` field as `field.name("key").sql("content.account_id")`
- `year` needs to match with `content.year` in Kafka, which is a nested field
    - We can only do foreign key relationships with top level fields, not nested fields. So we define a new field
      called `tmp_year` which will not appear in the final output for the Kafka messages but is used as an intermediate
      step `field.name("tmp_year").sql("content.year").omit(true)`
- `name` needs to match with `content.details.name` in Kafka, also a nested field
    - Using the same logic as above, we define a temporary field called `tmp_name` which will take the value of the
      nested field but will be omitted `field.name("tmp_name").sql("content.details.name").omit(true)`
- `payload` represents the whole JSON message sent to Kafka, which matches to `value` field

Our foreign keys are therefore defined like below. Order is important when defining the list of fields. The index needs
to match with the corresponding field in the other data source.

=== "Java"

    ```java
    var myPlan = plan().addForeignKeyRelationship(
            kafkaTask, List.of("key", "tmp_year", "tmp_name", "value"),
            List.of(Map.entry(csvTask, List.of("account_number", "year", "name", "payload")))
    );
  
    var conf = configuration()
          .generatedReportsFolderPath("/opt/app/data/report");

    execute(myPlan, conf, kafkaTask, csvTask);
    ```

=== "Scala"

    ```scala
    val myPlan = plan.addForeignKeyRelationship(
        kafkaTask, List("key", "tmp_year", "tmp_name", "value"),
        List(csvTask -> List("account_number", "year", "name", "payload"))
    )
  
    val conf = configuration.generatedReportsFolderPath("/opt/app/data/report")

    execute(myPlan, conf, kafkaTask, csvTask)
    ```

=== "YAML"

    Update `docker/data/custom/unified/batch-event.yaml`:
    ```yaml
    name: "batch_event_plan"
    description: "Generate batch and event data"

    config:
      folders:
        generatedReportsFolderPath: "/opt/app/data/report"

    dataSources:
      - name: "my_kafka"
        connection:
          type: "kafka"
          options:
            url: "localhost:9092"
        steps:
          - name: "account_topic"
            fields:
              - name: "key"
              - name: "tmp_year"
              - name: "tmp_name"
              - name: "value"
      - name: "my_csv"
        connection:
          type: "csv"
          options:
            path: "/opt/app/data/csv/account"
        steps:
          - name: "csv_accounts"
            fields:
              - name: "account_number"
              - name: "year"
              - name: "name"
              - name: "payload"

    foreignKeys:
      - source:
          dataSource: "my_kafka"
          step: "account_topic"
          fields: ["key", "tmp_year", "tmp_name", "value"]
        generate:
          - dataSource: "my_csv"
            step: "csv_accounts"
            fields: ["account_number", "year", "name", "payload"]
    ```

### Run

Let's try run.

```shell
cd ..
./run.sh
#input class MyAdvancedBatchEventJavaPlanRun or MyAdvancedBatchEventPlanRun
#after completing
docker exec docker-kafkaserver-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic account-topic --from-beginning
```

It should look something like this.

```shell
{"account_id":"ACC03093143","year":2023,"amount":87990.37196728592,"details":{"name":"Nadine Heidenreich Jr.","first_txn_date":"2021-11-09","updated_by":{"user":"YfEyJCe8ohrl0j IfyT","time":"2022-09-26T20:47:53.404Z"}},"transactions":[{"txn_date":"2021-11-09","amount":97073.7914706189}]}
{"account_id":"ACC08764544","year":2021,"amount":28675.58758765888,"details":{"name":"Delila Beer","first_txn_date":"2021-05-19","updated_by":{"user":"IzB5ksXu","time":"2023-01-26T20:47:26.389Z"}},"transactions":[{"txn_date":"2021-10-01","amount":80995.23818711648},{"txn_date":"2021-05-19","amount":92572.40049217848},{"txn_date":"2021-12-11","amount":99398.79832225188}]}
{"account_id":"ACC62505420","year":2023,"amount":96125.3125884202,"details":{"name":"Shawn Goodwin","updated_by":{"user":"F3dqIvYp2pFtena4","time":"2023-02-11T04:38:29.832Z"}},"transactions":[]}
```

Let's also check if there is a corresponding record in the CSV file.

```shell
$ cat docker/sample/csv/account/part-0000* | grep ACC03093143
ACC03093143,2023,Nadine Heidenreich Jr.,"{\"account_id\":\"ACC03093143\",\"year\":2023,\"amount\":87990.37196728592,\"details\":{\"name\":\"Nadine Heidenreich Jr.\",\"first_txn_date\":\"2021-11-09\",\"updated_by\":{\"user\":\"YfEyJCe8ohrl0j IfyT\",\"time\":\"2022-09-26T20:47:53.404Z\"}},\"transactions\":[{\"txn_date\":\"2021-11-09\",\"amount\":97073.7914706189}]}"
```

Great! The account, year, name and payload look to all match up.

### Additional Topics

#### Order of execution

You may notice that the events are generated first, then the CSV file. This is because as part of the `execute`
function, we passed in the `kafkaTask` first, before the `csvTask`. You can change the order of execution by
passing in `csvTask` before `kafkaTask` into the `execute` function.
