---
title: "Field Name Validations"
description: "Examples of field name level validations for data in files, databases, HTTP APIs or messaging systems via Data Catering."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Field Name Validations

Run validations on the field names to check for field name count of existence of field names.

## Count Equal

Ensure field name count is equal to certain number.

=== "Java"

    ```java
    validation().fieldNames().countEqual(3)
    ```

=== "Scala"

    ```scala
    validation.fieldNames.countEqual(3)
    ```

=== "YAML"

    ```yaml
    - fieldNameType: "fieldCountEqual"
      count: "3"
    ```

## Not Equal

Ensure field name count is between two numbers.

=== "Java"

    ```java
    validation().fieldNames().countBetween(10, 12)
    ```

=== "Scala"

    ```scala
    validation.fieldNames.countBetween(10, 12)
    ```

=== "YAML"

    ```yaml
    - fieldNameType: "fieldCountBetween"
      minCount: "10"
      maxCount: "12"
    ```

## Match Order

Ensure all field names match particular ordering and is complete.

=== "Java"

    ```java
    validation().fieldNames().matchOrder("account_id", "amount", "name")
    ```

=== "Scala"

    ```scala
    validation.fieldNames.matchOrder("account_id", "amount", "name")
    ```

=== "YAML"

    ```yaml
    - fieldNameType: "fieldNameMatchOrder"
      names: ["account_id", "amount", "name"]
    ```

## Match Set

Ensure field names contains set of expected names. Order is not checked.

=== "Java"

    ```java
    validation().fieldNames().matchSet("account_id", "first_name")
    ```

=== "Scala"

    ```scala
    validation.fieldNames.matchSet("account_id", "first_name")
    ```

=== "YAML"

    ```yaml
    - fieldNameType: "fieldNameMatchSet"
      names: ["account_id", "first_name"]
    ```
