---
title: "Generate Multiple Records Per Field Example"
description: "Example for generating multiple records per field for data with Data Caterer."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Multiple Records Per Field

Creating a data generator for a CSV file where there are multiple records per field values.

## Requirements

- 5 minutes
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

    [Run Data Caterer UI via the 'Quick Start' found here.](../../../get-started/quick-start.md)

### Plan Setup

Create a new Java or Scala class.

- Java: `src/main/java/io/github/datacatering/plan/MyMultipleRecordsPerColJavaPlan.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyMultipleRecordsPerColPlan.scala`

Make sure your class extends `PlanRun`.

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    ...
    
    public class MyMultipleRecordsPerColJavaPlan extends PlanRun {
        {
            var transactionTask = csv("customer_transactions", "/opt/app/data/customer/transaction", Map.of("header", "true"))
                    .fields(
                            field().name("account_id"),
                            field().name("full_name"),
                            field().name("amount").type(DoubleType.instance()).min(1).max(100),
                            field().name("time").type(TimestampType.instance()).min(java.sql.Date.valueOf("2022-01-01")),
                            field().name("date").type(DateType.instance()).sql("DATE(time)")
                    );
    
            var config = configuration()
                    .generatedReportsFolderPath("/opt/app/data/report")
                    .enableUniqueCheck(true);
    
            execute(config, transactionTask);
        }
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    ...
    
    class MyMultipleRecordsPerColPlan extends PlanRun {

      val transactionTask = csv("customer_transactions", "/opt/app/data/customer/transaction", Map("header" -> "true"))
        .fields(
          field.name("account_id").regex("ACC[0-9]{8}"), 
          field.name("full_name").expression("#{Name.name}"), 
          field.name("amount").`type`(DoubleType.instance).min(1).max(100),
          field.name("time").`type`(TimestampType.instance).min(java.sql.Date.valueOf("2022-01-01")), 
          field.name("date").`type`(DateType.instance).sql("DATE(time)")
        )
    
      val config = configuration
        .generatedReportsFolderPath("/opt/app/data/report")
    
      execute(config, transactionTask)
    }
    ```

### Record Count

By default, tasks will generate 1000 records. You can alter this value via the `count` configuration which can be
applied to individual tasks. For example, in Scala, `csv(...).count(count.records(100))` to generate only 100 records.

#### Records Per Field

In this scenario, for a given `account_id, full_name`, there should be multiple records for it as we want to simulate a
customer having multiple transactions. We can achieve this through defining the number of records to generate in
the `count` function.

=== "Java"

    ```java
    var transactionTask = csv("customer_transactions", "/opt/app/data/customer/transaction", Map.of("header", "true"))
            .fields(
                    ...
            )
            .count(count().recordsPerField(5, "account_id", "full_name"));
    ```

=== "Scala"

    ```scala
    val transactionTask = csv("customer_transactions", "/opt/app/data/customer/transaction", Map("header" -> "true"))
      .fields(
        ...
      )
      .count(count.recordsPerField(5, "account_id", "full_name"))
    ```

This will generate `1000 * 5 = 5000` records as the default number of records is set (1000) and
per `account_id, full_name` from the initial 1000 records, 5 records will be generated.

#### Random Records Per Field

Generating 5 records per field is okay but still not quite reflective of the real world. Sometimes, people have
accounts with no transactions in them, or they could have many. We can accommodate for this via defining a random number
of records per field.

=== "Java"

    ```java
    var transactionTask = csv("customer_transactions", "/opt/app/data/customer/transaction", Map.of("header", "true"))
            .fields(
                    ...
            )
            .count(count().recordsPerFieldGenerator(generator().min(0).max(5), "account_id", "full_name"));
    ```

=== "Scala"

    ```scala
    val transactionTask = csv("customer_transactions", "/opt/app/data/customer/transaction", Map("header" -> "true"))
      .fields(
        ...
      )
      .count(count.recordsPerFieldGenerator(generator.min(0).max(5), "account_id", "full_name"))
    ```

Here we set the minimum number of records per field to be 0 and the maximum to 5. This will follow a uniform
distribution so the average number of records per account is 2.5. We could also define other metadata,
just like we did with fields, when defining the generator. For example, we could set `standardDeviation` and `mean` for
the number of records generated per field to follow a normal distribution.

### Run

Let's try run.

```shell
#clean up old data
rm -rf docker/sample/customer/account
./run.sh
#input class MyMultipleRecordsPerColJavaPlan or MyMultipleRecordsPerColPlan
#after completing
head docker/sample/customer/transaction/part-00000*
```

It should look something like this.

```shell
ACC29117767,Willodean Sauer
ACC29117767,Willodean Sauer,84.99145871948083,2023-05-14T09:55:51.439Z,2023-05-14
ACC29117767,Willodean Sauer,58.89345733567232,2022-11-22T07:38:20.143Z,2022-11-22
```

You can now look to play around with other count configurations found [**here**](../../generator/count.md).
