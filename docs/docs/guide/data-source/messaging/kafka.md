---
title: "Kafka Test Data Management"
description: "Example of Kafka data generation and testing tool that can automatically discover, generate and validate."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Kafka

Creating a data generator for Kafka. You will build a Docker image that will be able to populate data in kafka
for the topics you configure.

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer-example/blob/main/src/main/scala/io/github/datacatering/plan/KafkaPlanRun.scala) | [:material-coffee: Java Example](https://github.com/data-catering/data-caterer-example/blob/main/src/main/java/io/github/datacatering/plan/KafkaJavaPlanRun.java) | [:material-file-yaml-outline: Basic YAML](https://github.com/data-catering/data-caterer-example/blob/main/docker/data/custom/task/kafka) | [:material-file-yaml-outline: Advanced YAML](https://github.com/data-catering/data-caterer-example/blob/main/docker/data/custom/task/kafka/advanced-kafka-task.yaml)

## Requirements

- 10 minutes
- Git
- Gradle
- Docker

## Get Started

First, we will clone the data-caterer-example repo which will already have the base project setup required.

=== "Java"

    ```shell
    git clone git@github.com:data-catering/data-caterer-example.git
    ```

=== "Scala"

    ```shell
    git clone git@github.com:data-catering/data-caterer-example.git
    ```

=== "YAML"

    ```shell
    git clone git@github.com:data-catering/data-caterer-example.git
    ```

=== "UI"

    [Run Data Caterer UI via the 'Quick Start' found here.](../../../../get-started/quick-start.md)

If you already have a Kafka instance running, you can skip to [this step](#plan-setup).

### Kafka Setup

Next, let's make sure you have an instance of Kafka up and running in your local environment. This will make it
easy for us to iterate and check our changes.

```shell
cd docker
docker-compose up -d kafka
```

### Plan Setup

Create a file depending on which interface you want to use.

- Java: `src/main/java/io/github/datacatering/plan/MyAdvancedKafkaJavaPlan.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyAdvancedKafkaPlan.scala`
- YAML: `docker/data/custom/plan/my-kafka.yaml`

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    
    public class MyAdvancedKafkaJavaPlan extends PlanRun {
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    
    class MyAdvancedKafkaPlan extends PlanRun {
    }
    ```

=== "YAML"

    In `docker/data/custom/plan/my-kafka.yaml`:
    ```yaml
    name: "my_kafka_plan"
    description: "Create account data via Kafka"
    tasks:
      - name: "kafka_task"
        dataSourceName: "my_kafka"
    ```

=== "UI"

    1. Click on `Connection` towards the top of the screen
    1. For connection name, set to `my_kafka`
    1. Click on `Select data source type..` and select `Kafka`
    1. Set URL as `localhost:9092`
        1. Optionally, we could set a topic name but if you have more than 1 topic, you would have to create new connection for each topic
    1. Click on `Create`
    1. You should see your connection `my_kafka` show under `Existing connections`
    1. Click on `Home` towards the top of the screen
    1. Set plan name to `my_kafka_plan`
    1. Set task name to `kafka_task`
    1. Click on `Select data source..` and select `my_kafka`

This class defines where we need to define all of our configurations for generating data. There are helper variables and
methods defined to make it simple and easy to use.

#### Connection Configuration

Within our class, we can start by defining the connection properties to connect to Kafka.

=== "Java"

    ```java
    var accountTask = kafka(
        "my_kafka",       //name
        "localhost:9092", //url
        Map.of()          //optional additional connection options
    );
    ```
    
    Additional options can be found [**here**](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#writing-data-to-kafka).

=== "Scala"

    ```scala
    val accountTask = kafka(
        "my_kafka",       //name
        "localhost:9092", //url
        Map()             //optional additional connection options
    )
    ```
    
    Additional options can be found [**here**](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#writing-data-to-kafka).

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    kafka {
        my_kafka {
            kafka.bootstrap.servers = "localhost:9092"
            kafka.bootstrap.servers = ${?KAFKA_BOOTSTRAP_SERVERS}
        }
    }
    ```

=== "UI"

    1. We have already created the connection details in [this step](#plan-setup)

#### Schema

Let's create a task for inserting data into the `account-topic` that is already
defined under`docker/data/kafka/setup_kafka.sh`. This topic should already be setup for you if you followed this
[step](#kafka-setup). We can check if the topic is set up already via the following command:

```shell
docker exec docker-kafkaserver-1 kafka-topics --bootstrap-server localhost:9092 --list
```

Trimming the connection details to work with the docker-compose Kafka, we have a base Kafka connection to define
the topic we will publish to. Let's define each field along with their corresponding data type. You will notice that
the `text` fields do not have a data type defined. This is because the default data type is `StringType`.

=== "Java"

    ```java
    {
        var kafkaTask = kafka("my_kafka", "kafkaserver:29092")
                .topic("account-topic")
                .fields(
                        field().name("key").sql("body.account_id"),
                        //field().name("partition").type(IntegerType.instance()),   //can define message partition here
                        field().messageHeaders(
                                field().messageHeader("account-id", "body.account_id"),
                                field().messageHeader("updated", "body.details.updated_by-time")
                        )
                ).fields(
                        field().messageBody(
                                field().name("account_id").regex("ACC[0-9]{8}"),
                                field().name("year").type(IntegerType.instance()).min(2021).max(2023),
                                field().name("amount").type(DoubleType.instance()),
                                field().name("details")
                                        .fields(
                                                field().name("name").expression("#{Name.name}"),
                                                field().name("first_txn_date").type(DateType.instance()).sql("ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"),
                                                field().name("updated_by")
                                                        .fields(
                                                                field().name("user"),
                                                                field().name("time").type(TimestampType.instance())
                                                        )
                                        ),
                                field().name("transactions").type(ArrayType.instance())
                                        .fields(
                                                field().name("txn_date").type(DateType.instance()).min(Date.valueOf("2021-01-01")).max("2021-12-31"),
                                                field().name("amount").type(DoubleType.instance())
                                        )
                        )
                )
    }
    ```

=== "Scala"

    ```scala
    val kafkaTask = kafka("my_kafka", "kafkaserver:29092")
      .topic("account-topic")
      .fields(
        field.name("key").sql("body.account_id"),
        //field.name("partition").type(IntegerType),  can define partition here
        field.messageHeaders(
          field.messageHeader("account-id", "body.account_id"),
          field.messageHeader("updated", "body.details.updated_by.time"),
        )
      )
      .fields(
        field.messageBody(
          field.name("account_id").regex("ACC[0-9]{8}"),
          field.name("year").`type`(IntegerType).min(2021).max(2023),
          field.name("account_status").oneOf("open", "closed", "suspended", "pending"),
          field.name("amount").`type`(DoubleType),
          field.name("details").`type`(StructType)
            .fields(
              field.name("name").expression("#{Name.name}"),
              field.name("first_txn_date").`type`(DateType).sql("ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"),
              field.name("updated_by").`type`(StructType)
                .fields(
                  field.name("user"),
                  field.name("time").`type`(TimestampType),
                ),
            ),
          field.name("transactions").`type`(ArrayType)
            .fields(
              field.name("txn_date").`type`(DateType).min(Date.valueOf("2021-01-01")).max("2021-12-31"),
              field.name("amount").`type`(DoubleType),
            )
        )
      )
    ```

=== "YAML"

    In `docker/data/custom/task/kafka/kafka-task.yaml`:
    ```yaml
    name: "kafka_task"
    steps:
      - name: "kafka_account"
        options:
          topic: "account-topic"
        fields:
          - name: "key"
            options:
              sql: "body.account_id"
          - name: "messageBody"
            fields:
              - name: "account_id"
              - name: "year"
                type: "int"
                options:
                  min: "2021"
                  max: "2022"
              - name: "amount"
                type: "double"
                options:
                  min: "10.0"
                  max: "100.0"
              - name: "details"
                fields:
                  - name: "name"
                  - name: "first_txn_date"
                    type: "date"
                    options:
                      sql: "ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"
                  - name: "updated_by"
                    fields:
                      - name: "user"
                      - name: "time"
                        type: "timestamp"
              - name: "transactions"
                type: "array"
                fields:
                  - name: "txn_date"
                    type: "date"
                  - name: "amount"
                    type: "double"
          - name: "messageHeaders"
            fields:
              - name: "account-id"
                options:
                  sql: "body.account_id"
              - name: "updated"
                options:
                  sql: "body.details.update_by.time"
    ```

=== "UI"

    1. Click on `Generation` and tick the `Manual` checkbox
    1. Click on `+ Field`
        1. Add name as `key`
        1. Click on `Select data type` and select `string`
        1. Click `+` next to data type and select `Sql`. Then enter `body.account_id`
        1. Click on `+ Field` and add name as `messageBody`
        1. Click on `Select data type` and select `struct`
        1. Click on `+ Field` under `messageBody` and add name as `account_id`
        1. Add additional fields under `messageBody` with your own metadata
        1. Click on `+ Field` and add name as `messageHeaders`
        1. Click on `Select data type` and select `struct`
        1. Click on `+ Field` under `messageHeaders` and add name as `account_id`
        1. Add additional fields under `messageHeaders` with your own metadata

#### Fields

The schema defined for Kafka has a format that needs to be followed as noted above. Specifically, the required fields are:
- `messageBody`

Whilst, the other fields are optional:
- `key`
- `partition`
- `messageHeaders`

##### Message Headers

If your messages contain headers, you can follow the details below on generating header values. These can be based off 
values contained within you message body or could be static values, just like any other generated field. The main
restriction imposed here is that the `key` of the message header is static and the `value` has to be a valid SQL
expression.

=== "Java"

    ```java
    field().messageHeaders(
            field().messageHeader("account-id", "body.account_id"),
            field().messageHeader("updated", "body.details.updated_by-time")
    )
    ```

=== "Scala"

    ```scala
    field.messageHeaders(
      field.messageHeader("account-id", "body.account_id"),
      field.messageHeader("updated", "body.details.updated_by.time"),
    )
    ```

=== "YAML"

    In `docker/data/custom/task/kafka/kafka-task.yaml`:
    ```yaml
    name: "kafka_task"
    steps:
      - name: "kafka_account"
        options:
          topic: "account-topic"
        fields:
          - name: "messageHeaders"
            fields:
              - name: "account-id"
                options:
                  sql: "body.account_id"
              - name: "updated"
                options:
                  sql: "body.details.update_by.time"
    ```

=== "UI"

    1. Click on `+ Field` and add name as `messageHeaders`
    1. Click on `Select data type` and select `struct`
    1. Click on `+ Field` under `messageHeaders` and add name as `account_id`
    1. Add additional fields under `messageHeaders` with your own metadata

##### transactions

`transactions` is an array that contains an inner structure of `txn_date` and `amount`. The size of the array generated
can be controlled via `arrayMinLength` and `arrayMaxLength`.

=== "Java"

    ```java
    field().name("transactions").type(ArrayType.instance())
            .fields(
                    field().name("txn_date").type(DateType.instance()).min(Date.valueOf("2021-01-01")).max("2021-12-31"),
                    field().name("amount").type(DoubleType.instance())
            )
    ```

=== "Scala"

    ```scala
    field.name("transactions").`type`(ArrayType)
      .fields(
        field.name("txn_date").`type`(DateType).min(Date.valueOf("2021-01-01")).max("2021-12-31"),
        field.name("amount").`type`(DoubleType),
      )
    ```

=== "YAML"

    In `docker/data/custom/task/kafka/kafka-task.yaml`:
    ```yaml
    name: "kafka_task"
    steps:
      - name: "kafka_account"
        options:
          topic: "account-topic"
        fields:
          - name: "messageBody"
            fields:
              - name: "transactions"
                type: "array"
                fields:
                  - name: "txn_date"
                    type: "date"
                  - name: "amount"
                    type: "double"
    ```

=== "UI"

    !!! warning "Warning"
        Inner field definition for array type is currently not supported from the UI. Will be added in the near future!

##### details

`details` is another example of a nested schema structure where it also has a nested structure itself in `updated_by`.
One thing to note here is the `first_txn_date` field has a reference to the `body.transactions` array where it will 
sort the array by `txn_date` and get the first element.

=== "Java"

    ```java
    field().name("details")
            .fields(
                    field().name("name").expression("#{Name.name}"),
                    field().name("first_txn_date").type(DateType.instance()).sql("ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"),
                    field().name("updated_by")
                            .fields(
                                    field().name("user"),
                                    field().name("time").type(TimestampType.instance())
                            )
            )
    ```

=== "Scala"

    ```scala
    field.name("details")
      .fields(
        field.name("name").expression("#{Name.name}"),
        field.name("first_txn_date").`type`(DateType).sql("ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"),
        field.name("updated_by")
          .fields(
            field.name("user"),
            field.name("time").`type`(TimestampType),
          ),
      )
    ```

=== "YAML"

    In `docker/data/custom/task/kafka/kafka-task.yaml`:
    ```yaml
    name: "kafka_task"
    steps:
      - name: "kafka_account"
        options:
          topic: "account-topic"
        fields:
          - name: "messageBody"
            fields:
              - name: "details"
                fields:
                  - name: "name"
                  - name: "first_txn_date"
                    type: "date"
                    options:
                      sql: "ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"
                  - name: "updated_by"
                    fields:
                      - name: "user"
                      - name: "time"
                        type: "timestamp"
    ```

=== "UI"

    1. Click on `+ Field` and add name as `messageBody`
    1. Click on `Select data type` and select `struct`
    1. Click on `+ Field` under `messageBody` and add name as `details`
    1. Add additional fields under `details` with your own metadata

#### Additional Configurations

At the end of data generation, a report gets generated that summarises the actions it performed. We can control the
output folder of that report via configurations.

=== "Java"

    ```java
    var config = configuration()
            .generatedReportsFolderPath("/opt/app/data/report");
    ```

=== "Scala"

    ```scala
    val config = configuration
      .generatedReportsFolderPath("/opt/app/data/report")
    ```

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    folders {
      generatedReportsFolderPath = "/opt/app/data/report"
    }
    ```

=== "UI"

    1. Click on `Advanced Configuration` towards the bottom of the screen
    1. Click on `Folder` and enter `/tmp/data-caterer/report` for `Generated Reports Folder Path`

### Run

Now we can run via the script `./run.sh` that is in the top level directory of the `data-caterer-example` to run the class we just
created.

=== "Java"

    ```shell
    ./run.sh AdvancedKafkaJavaPlanRun
    docker exec docker-kafkaserver-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic account-topic --from-beginning
    ```

=== "Scala"

    ```shell
    ./run.sh AdvancedKafkaPlanRun
    docker exec docker-kafkaserver-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic account-topic --from-beginning
    ```

=== "YAML"

    ```shell
    ./run.sh my-kafka.yaml
    docker exec docker-kafkaserver-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic account-topic --from-beginning
    ```

=== "UI"

    1. Click the button `Execute` at the top
    1. Progress updates will show in the bottom right corner
    1. Click on `History` at the top
    1. Check for your plan name and see the result summary
    1. Click on `Report` on the right side to see more details of what was executed

Your output should look like this.

```shell
{"account_id":"ACC56292178","year":2022,"amount":18338.627721151555,"details":{"name":"Isaias Reilly","first_txn_date":"2021-01-22","updated_by":{"user":"FgYXbKDWdhHVc3","time":"2022-12-30T13:49:07.309Z"}},"transactions":[{"txn_date":"2021-01-22","amount":30556.52125487579},{"txn_date":"2021-10-29","amount":39372.302259554635},{"txn_date":"2021-10-29","amount":61887.31389495968}]}
{"account_id":"ACC37729457","year":2022,"amount":96885.31758764731,"details":{"name":"Randell Witting","first_txn_date":"2021-06-30","updated_by":{"user":"HCKYEBHN8AJ3TB","time":"2022-12-02T02:05:01.144Z"}},"transactions":[{"txn_date":"2021-06-30","amount":98042.09647765031},{"txn_date":"2021-10-06","amount":41191.43564742036},{"txn_date":"2021-11-16","amount":78852.08184809204},{"txn_date":"2021-10-09","amount":13747.157653571106}]}
{"account_id":"ACC23127317","year":2023,"amount":81164.49304198896,"details":{"name":"Jed Wisozk","updated_by":{"user":"9MBFZZ","time":"2023-07-12T05:56:52.397Z"}},"transactions":[]}
```

Also check the HTML report, found at `docker/sample/report/index.html`, that gets generated to get an overview of what
was executed.

![Sample report](../../../../sample/report/report_screenshot.png)
