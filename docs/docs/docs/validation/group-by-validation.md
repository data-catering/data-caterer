---
title: "Group By/Aggregate Validations"
description: "Examples of group by/aggregate validations to run for data in files, databases, HTTP APIs or messaging systems via Data Catering."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Group By Validation

If you want to run aggregations based on a particular set of fields or just the whole dataset, you can do so via group
by validations. An example would be checking that the sum of `amount` is less than 1000 per `account_id, year`. The
validations applied can be one of the validations from the [basic validation set found here](basic-validation.md).

## Pre-filter

If you want to only run the validation on a specific subset of data, you can define pre-filter conditions. [Find more
details here](../validation.md#pre-filter-data).

## Record count

Check the number of records across the whole dataset.

=== "Java"

    ```java
    validation().groupBy().count().lessThan(1000)
    ```

=== "Scala"

    ```scala
    validation.groupBy().count().lessThan(1000)
    ```

=== "YAML"

    ```yaml
    name: "account_checks"
    dataSources:
      ...
        validations:
          - aggType: "count"
            aggExpr: "count < 1000"
    ```

## Record count per group

Check the number of records for each group.

=== "Java"

    ```java
    validation().groupBy("account_id", "year").count().lessThan(10)
    ```

=== "Scala"

    ```scala
    validation.groupBy("account_id", "year").count().lessThan(10)
    ```

=== "YAML"

    ```yaml
    name: "account_checks"
    dataSources:
      ...
        validations:
          - groupByCols: ["account_id", "year"]
            aggType: "count"
            aggExpr: "count < 10"
    ```

## Sum

Check the sum of a fields values for each group adheres to validation.

=== "Java"

    ```java
    validation().groupBy("account_id", "year").sum("amount").lessThan(1000)
    ```

=== "Scala"

    ```scala
    validation.groupBy("account_id", "year").sum("amount").lessThan(1000)
    ```

=== "YAML"

    ```yaml
    name: "account_checks"
    dataSources:
      ...
        validations:
          - groupByCols: ["account_id", "year"]
            aggExpr: "sum(amount) < 1000"
    ```

## Count

Check the count for each group adheres to validation.

=== "Java"

    ```java
    validation().groupBy("account_id", "year").count("amount").lessThan(10)
    ```

=== "Scala"

    ```scala
    validation.groupBy("account_id", "year").count("amount").lessThan(10)
    ```

=== "YAML"

    ```yaml
    name: "account_checks"
    dataSources:
      ...
        validations:
          - groupByCols: ["account_id", "year"]
            aggType: "count"
            aggExpr: "count(amount) < 10"
    ```

## Min

Check the min for each group adheres to validation.

=== "Java"

    ```java
    validation().groupBy("account_id", "year").min("amount").greaterThan(0)
    ```

=== "Scala"

    ```scala
    validation.groupBy("account_id", "year").min("amount").greaterThan(0)
    ```

=== "YAML"

    ```yaml
    name: "account_checks"
    dataSources:
      ...
        validations:
          - groupByCols: ["account_id", "year"]
            aggExpr: "min(amount) > 0"
    ```

## Max

Check the max for each group adheres to validation.

=== "Java"

    ```java
    validation().groupBy("account_id", "year").max("amount").lessThanOrEqual(100)
    ```

=== "Scala"

    ```scala
    validation.groupBy("account_id", "year").max("amount").lessThanOrEqual(100)
    ```

=== "YAML"

    ```yaml
    name: "account_checks"
    dataSources:
      ...
        validations:
          - groupByCols: ["account_id", "year"]
            aggExpr: "max(amount) < 100"
    ```

## Average

Check the average for each group adheres to validation.

=== "Java"

    ```java
    validation().groupBy("account_id", "year").avg("amount").between(40, 60)
    ```

=== "Scala"

    ```scala
    validation.groupBy("account_id", "year").avg("amount").between(40, 60)
    ```

=== "YAML"

    ```yaml
    name: "account_checks"
    dataSources:
      ...
        validations:
          - groupByCols: ["account_id", "year"]
            aggExpr: "avg(amount) BETWEEN 40 && 60"
    ```

## Standard deviation

Check the standard deviation for each group adheres to validation.

=== "Java"

    ```java
    validation().groupBy("account_id", "year").stddev("amount").between(0.5, 0.6)
    ```

=== "Scala"

    ```scala
    validation.groupBy("account_id", "year").stddev("amount").between(0.5, 0.6)
    ```

=== "YAML"

    ```yaml
    name: "account_checks"
    dataSources:
      ...
        validations:
          - groupByCols: ["account_id", "year"]
            aggExpr: "stddev(amount) BETWEEN 0.5 && 0.6"
    ```
