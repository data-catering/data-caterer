---
title: "Field Validations"
description: "Examples of field level validations for data in files, databases, HTTP APIs or messaging systems via Data Catering."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Basic Validations

Run validations on a field to ensure the values adhere to your requirement. Can be set to complex validation logic
via SQL expression as well if needed (see [**here**](#expression)).

## Pre-filter

If you want to only run the validation on a specific subset of data, you can define pre-filter conditions. [Find more 
details here](../validation.md#pre-filter-data).

## Equal

Ensure all data in field is equal/not equal to certain value. Value can be of any data type. Can use `isEqualField` to 
define SQL expression that can reference other fields.

=== "Java"

    ```java
    validation().field("year").isEqual(2021),
    validation().field("year").isEqualField("YEAR(date)"),

    validation().field("year").isEqual(2021, true), //check not equal to
    validation().field("year").isEqualField("YEAR(date)", true),
    ```

=== "Scala"

    ```scala
    validation.field("year").isEqual(2021),
    validation.field("year").isEqualField("YEAR(date)"),

    validation.field("year").isEqual(2021, true),  //check not equal to
    validation.field("year").isEqualField("YEAR(date)", true),
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "year"
            validation:
              - type: "equal"
                value: 2021
              - type: "equal"
                value: 2021
                negate: true
    ```

## Null

Ensure all data in field is null or not null.

=== "Java"

    ```java
    validation().field("year").isNull()

    validation().field("year").isNull(true) //check not null
    ```

=== "Scala"

    ```scala
    validation.field("year").isNull()

    validation.field("year").isNull(true) //check not null
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "year"
            validation:
              - type: "null"
              - type: "null"
                negate: true
    ```

## Contains

Ensure all data in field is contains/not contains a certain string. Field has to have type string.

=== "Java"

    ```java
    validation().field("name").contains("peter")

    validation().field("name").contains("peter", true)  //check not contains
    ```

=== "Scala"

    ```scala
    validation.field("name").contains("peter")

    validation.field("name").contains("peter", true)  //check not contains
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "name"
            validation:
              - type: "contains"
                value: "peter"
              - type: "contains"
                value: "peter"
                negate: true
    ```

## Unique

Ensure all data in field is unique.


=== "Java"

    ```java
    validation().unique("account_id", "name")
    ```

=== "Scala"

    ```scala
    validation.unique("account_id", "name")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - unique: ["account_id", "name"]
    ```

## Less Than

Ensure all data in field is less than certain value. Can use `lessThanField` to define SQL expression that can reference 
other fields.

=== "Java"

    ```java
    validation().field("amount").lessThan(100),
    validation().field("amount").lessThanField("balance + 1"),

    validation().field("amount").lessThan(100, false), //check less than or equal to
    validation().field("amount").lessThanField("balance + 1", false),
    ```

=== "Scala"

    ```scala
    validation.field("amount").lessThan(100),
    validation.field("amount").lessThanField("balance + 1"),

    validation.field("amount").lessThan(100, false), //check less than or equal to
    validation.field("amount").lessThanField("balance + 1", false),
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "amount < 100"
          - expr: "amount < balance + 1"
          - field: "amount"
            validation:
              - type: "lessThan"
                value: 100
              - type: "lessThan"
                value: 100
                strictly: false
    ```

## Greater Than

Ensure all data in field is greater than certain value. Can use `greaterThanField` to define SQL expression
that can reference other fields.

=== "Java"

    ```java
    validation().field("amount").greaterThan(100),
    validation().field("amount").greaterThanField("balance"),

    validation().field("amount").greaterThan(100, false), //check greater than or equal to
    validation().field("amount").greaterThanField("balance", false),
    ```

=== "Scala"

    ```scala
    validation.field("amount").greaterThan(100),
    validation.field("amount").greaterThanField("balance"),

    validation.field("amount").greaterThan(100), //check greater than or equal to
    validation.field("amount").greaterThanField("balance"),
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "amount > 100"
          - expr: "amount > balance"
          - field: "amount"
            validation:
              - type: "greaterThan"
                value: 100
              - type: "greaterThan"
                value: 100
                strictly: false
    ```

## Between

Ensure all data in field is between two values. Can use `betweenFields` to define SQL expression that references other 
fields.

=== "Java"

    ```java
    validation().field("amount").between(100, 200),
    validation().field("amount").betweenFields("balance * 0.9", "balance * 1.1"),

    validation().field("amount").between(100, 200, true), //check not between
    validation().field("amount").betweenFields("balance * 0.9", "balance * 1.1", true),
    ```

=== "Scala"

    ```scala
    validation.field("amount").between(100, 200),
    validation.field("amount").betweenFields("balance * 0.9", "balance * 1.1"),

    validation.field("amount").between(100, 200, true), //check not between
    validation.field("amount").betweenFields("balance * 0.9", "balance * 1.1", true),
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "amount BETWEEN 100 AND 200"
          - expr: "amount BETWEEN balance * 0.9 AND balance * 1.1"
          - field: "amount"
            validation:
              - type: "between"
                min: 100
                max: 200
              - type: "between"
                min: 100
                max: 200
                negate: true
    ```

## In

Ensure all data in field is in set of defined values.

=== "Java"

    ```java
    validation().field("status").in("open", "closed")

    validation().field("status").in(List.of("open", "closed"), true)  //check not in
    ```

=== "Scala"

    ```scala
    validation.field("status").in("open", "closed")

    validation.field("status").in(List("open", "closed"), true) //check not in
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "status IN ('open', 'closed')"
          - field: "status"
            validation:
              - type: "in"
                values: ["open", "closed"]
              - type: "in"
                values: ["open", "closed"]
                negate: true
    ```

## Matches

Ensure all data in field matches certain regex expression(s).

=== "Java"

    ```java
    validation().field("account_id").matches("ACC[0-9]{8}")
    validation().field("account_id").matchesList(List.of("ACC[0-9]{8}", "ACC[0-9]{10}"))  //check matches all regexes

    validation().field("account_id").matches("ACC[0-9]{8}", true) //check not matches
    validation().field("account_id").matchesList(List.of("ACC[0-9]{8}", "ACC[0-9]{10}"), true, false) //check does not match all regexes

    validation().field("account_id").matchesList(List.of("ACC[0-9]{8}", "ACC[0-9]{10}"), false, true)  //check matches at least one regex

    validation().field("account_id").matchesList(List.of("ACC[0-9]{8}", "ACC[0-9]{10}"), false, false)  //check does not match at least one regex
    ```

=== "Scala"

    ```scala
    validation.field("account_id").matches("ACC[0-9]{8}")
    validation.field("account_id").matchesList(List("ACC[0-9]{8}", "ACC[0-9]{10}"))  //check matches all regexes

    validation.field("account_id").matches("ACC[0-9]{8}", true) //check not matches
    validation.field("account_id").matchesList(List("ACC[0-9]{8}", "ACC[0-9]{10}"), true, false)  //check does not match all regexes

    validation.field("account_id").matchesList(List("ACC[0-9]{8}", "ACC[0-9]{10}"), false, true)  //check matches at least one regex

    validation.field("account_id").matchesList(List("ACC[0-9]{8}", "ACC[0-9]{10}"), false, false)  //check does not match at least one regex
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "REGEXP(account_id, ACC[0-9]{8})"
          - field: "account_id"
            validation:
              - type: "matches"
                regex: "ACC[0-9]{8}"
              - type: "matches"
                regex: "ACC[0-9]{8}"
                negate: true
              - type: "matchesList"
                regexes: ["ACC[0-9]{8}", "ACC[0-9]{10}"]
                matchAll: true
                negate: true
    ```

## Starts With

Ensure all data in field starts with certain string. Field has to have type string.

=== "Java"

    ```java
    validation().field("account_id").startsWith("ACC")

    validation().field("account_id").startsWith("ACC", true)  //check does not start with
    ```

=== "Scala"

    ```scala
    validation.field("account_id").startsWith("ACC")

    validation.field("account_id").startsWith("ACC", true)  //check does not start with
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "STARTSWITH(account_id, 'ACC')"
          - field: "account_id"
            validation:
              - type: "startsWith"
                value: "ACC"
              - type: "startsWith"
                value: "ACC"
                negate: true
    ```

## Ends With

Ensure all data in field ends with certain string. Field has to have type string.

=== "Java"

    ```java
    validation().field("account_id").endsWith("ACC")

    validation().field("account_id").endsWith("ACC", true)  //check does not end with
    ```

=== "Scala"

    ```scala
    validation.field("account_id").endsWith("ACC")

    validation.field("account_id").endsWith("ACC", true)  //check does not end with
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "ENDWITH(account_id, 'ACC')"
          - field: "account_id"
            validation:
              - type: "endsWith"
                value: "ACC"
              - type: "endsWith"
                value: "ACC"
                negate: true
    ```

## Size

Ensure all data in field has certain size. Field has to have type array or map.

=== "Java"

    ```java
    validation().field("transactions").size(5)

    validation().field("transactions").size(5, true)  //check does not have size
    ```

=== "Scala"

    ```scala
    validation.field("transactions").size(5)

    validation.field("transactions").size(5, true)  //check does not have size
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "SIZE(transactions, 5)"
          - field: "transactions"
            validation:
              - type: "size"
                value: 5
              - type: "size"
                value: 5
                negate: true
    ```

## Less Than Size

Ensure all data in field has size less than certain value. Field has to have type array or map.

=== "Java"

    ```java
    validation().field("transactions").lessThanSize(5)

    validation().field("transactions").lessThanSize(5, false) //check for less than or equal to size
    ```

=== "Scala"

    ```scala
    validation.field("transactions").lessThanSize(5)

    validation.field("transactions").lessThanSize(5, false) //check for less than or equal to size
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "SIZE(transactions) < 5"
          - field: "transactions"
            validation:
              - type: "lessThanSize"
                value: 5
              - type: "lessThanSize"
                value: 5
                strictly: false
    ```

## Greater Than Size

Ensure all data in field has size greater than certain value. Field has to have type array or map.

=== "Java"

    ```java
    validation().field("transactions").greaterThanSize(5)

    validation().field("transactions").greaterThanSize(5, false)  //check for less than or equal to size
    ```

=== "Scala"

    ```scala
    validation.field("transactions").greaterThanSize(5)

    validation.field("transactions").greaterThanSize(5, false)  //check for less than or equal to size
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "SIZE(transactions) > 5"
          - field: "transactions"
            validation:
              - type: "greaterThanSize"
                value: 5
              - type: "greaterThanSize"
                value: 5
                strictly: false
    ```

## Luhn Check

Ensure all data in field passes Luhn check. Luhn check is used to validate credit card numbers and certain
identification numbers (see [here](https://en.wikipedia.org/wiki/Luhn_algorithm) for more details).

=== "Java"

    ```java
    validation().field("credit_card").luhnCheck()

    validation().field("credit_card").luhnCheck(true) //check does not pass Luhn check
    ```

=== "Scala"

    ```scala
    validation.field("credit_card").luhnCheck()

    validation.field("credit_card").luhnCheck(true) //check does not pass Luhn check
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "LUHN_CHECK(credit_card)"
          - field: "credit_card"
            validation:
              - type: "luhnCheck"
              - type: "luhnCheck"
                negate: true
    ```

## Has Type

Ensure all data in field has certain data type.

=== "Java"

    ```java
    validation().field("id").hasType("string")
    validation().field("id").hasTypes(List.of("string", "double"))

    validation().field("id").hasType("string", true)  //check does not have type
    validation().field("id").hasTypes(List("string", "double"), true)
    ```

=== "Scala"

    ```scala
    validation.field("id").hasType("string")
    validation.field("id").hasTypes(List.of("string", "double"))

    validation.field("id").hasType("string", true)  //check does not have type
    validation.field("id").hasTypes(List("string", "double"), true)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "TYPEOF(id) == 'string'"
          - field: "id"
            validation:
              - type: "hasType"
                value: "string"
              - type: "hasType"
                value: "string"
                negate: true
              - type: "hasTypes"
                values: ["string", "double"]
              - type: "hasType"
                values: ["string", "double"]
                negate: true
    ```

## Distinct Values In Set

Check if distinct values of field exist in set.

=== "Java"

    ```java
    validation().field("name").distinctInSet("peter", "john")

    validation().field("name").distinctInSet(List.of("peter", "john"), false) //check for distinct values not in set
    ```

=== "Scala"

    ```scala
    validation.field("name").distinctInSet("peter", "john")

    validation.field("name").distinctInSet(List("peter", "john"), true) //check for distinct values not in set
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "name"
            validation:
              - type: "distinctInSet"
                values: ["peter", "john"]
              - type: "distinctInSet"
                values: ["peter", "john"]
                negate: true
    ```

## Distinct Values Contains Set

Check if distinct values of field contains set of values.

=== "Java"

    ```java
    validation().field("name").distinctContainsSet("peter", "john")

    validation().field("name").distinctContainsSet(List.of("peter", "john"), false) //check for distinct values not contains set
    ```

=== "Scala"

    ```scala
    validation.field("name").distinctContainsSet("peter", "john")

    validation.field("name").distinctContainsSet(List("peter", "john"), true) //check for distinct values not contains set
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "name"
            validation:
              - type: "distinctContainsSet"
                values: ["peter", "john"]
              - type: "distinctContainsSet"
                values: ["peter", "john"]
                negate: true
    ```

## Distinct Values Equal

Check if distinct values of field equals set of values.

=== "Java"

    ```java
    validation().field("name").distinctEqual("peter", "john")

    validation().field("name").distinctEqual(List.of("peter", "john"), false) //check for distinct values not equals set
    ```

=== "Scala"

    ```scala
    validation.field("name").distinctEqual("peter", "john")

    validation.field("name").distinctEqual(List("peter", "john"), true) //check for distinct values not equals set
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "name"
            validation:
              - type: "distinctEqual"
                values: ["peter", "john"]
              - type: "distinctEqual"
                values: ["peter", "john"]
                negate: true
    ```

## Max, Mean, Median, Min, Standard Deviation, Sum Between

Check if aggregation of values for field is between set of values.

=== "Java"

    ```java
    validation().field("amount").maxBetween(1, 100)
    validation().field("amount").meanBetween(1, 100)
    validation().field("amount").medianBetween(1, 100)
    validation().field("amount").minBetween(1, 100)
    validation().field("amount").stdDevBetween(1, 100)
    validation().field("amount").sumBetween(1, 100)

    validation().field("amount").maxBetween(1, 100, true) //check max amount is not between 1 and 100
    validation().field("amount").meanBetween(1, 100, true)
    validation().field("amount").medianBetween(1, 100, true)
    validation().field("amount").minBetween(1, 100, true)
    validation().field("amount").stdDevBetween(1, 100, true)
    validation().field("amount").sumBetween(1, 100, true)
    ```

=== "Scala"

    ```scala
    validation.field("amount").maxBetween(1, 100)
    validation.field("amount").meanBetween(1, 100)
    validation.field("amount").medianBetween(1, 100)
    validation.field("amount").minBetween(1, 100)
    validation.field("amount").stdDevBetween(1, 100)
    validation.field("amount").sumBetween(1, 100)

    validation.field("amount").maxBetween(1, 100, true) //check max amount is not between 1 and 100
    validation.field("amount").meanBetween(1, 100, true)
    validation.field("amount").medianBetween(1, 100, true)
    validation.field("amount").minBetween(1, 100, true)
    validation.field("amount").stdDevBetween(1, 100, true)
    validation.field("amount").sumBetween(1, 100, true)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "amount"
            validation:
              - type: "maxBetween"
                min: 1
                max: 100
              - type: "meanBetween"
                min: 1
                max: 100
              - type: "medianBetween"
                min: 1
                max: 100
              - type: "minBetween"
                min: 1
                max: 100
              - type: "stdDevBetween"
                min: 1
                max: 100
              - type: "sumBetween"
                min: 1
                max: 100
                negate: true
    ```

## Length Equal/Between

Check if length of field values is between or equal to value(s).

=== "Java"

    ```java
    validation().field("name").lengthBetween(1, 10)
    validation().field("name").lengthEqual(5)

    validation().field("name").lengthBetween(1, 10, false) //check for length not between 1 and 10
    validation().field("name").lengthEqual(5, false)
    ```

=== "Scala"

    ```scala
    validation.field("name").lengthBetween(1, 10)
    validation.field("name").lengthEqual(5)

    validation.field("name").lengthBetween(1, 10, false) //check for length not between 1 and 10
    validation.field("name").lengthEqual(5, false)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "name"
            validation:
              - type: "lengthBetween"
                min: 1
                max: 10
              - type: "lengthEqual"
                value: 5
              - type: "lengthBetween"
                min: 1
                max: 10
                negate: true
              - type: "lengthEqual"
                value: 5
                negate: true
    ```

## Is Increasing/Decreasing

Check if values of a field are increasing or decreasing.

=== "Java"

    ```java
    validation().field("amount").isDecreasing()
    validation().field("amount").isIncreasing()

    validation().field("amount").isDecreasing(false) //check it is not strictly decreasing
    validation().field("amount").isIncreasing(false)
    ```

=== "Scala"

    ```scala
    validation.field("amount").isDecreasing()
    validation.field("amount").isIncreasing()

    validation.field("amount").isDecreasing(false) //check it is not strictly decreasing
    validation.field("amount").isIncreasing(false)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "amount"
            validation:
              - type: "isDecreasing"
              - type: "isIncreasing"
              - type: "isDecreasing"
                strictly: false
              - type: "isIncreasing"
                strictly: false
    ```

## Is JSON Parsable

Check if values of a field are JSON parsable.

=== "Java"

    ```java
    validation().field("details").isJsonParsable()

    validation().field("details").isJsonParsable(true) //check it is not JSON parsable
    ```

=== "Scala"

    ```scala
    validation.field("details").isJsonParsable()

    validation.field("details").isJsonParsable(true) //check it is not JSON parsable
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "details"
            validation:
              - type: "isJsonParsable"
              - type: "isJsonParsable"
                negate: true
    ```

## Match JSON Schema

Check if values of a field match JSON schema.

=== "Java"

    ```java
    validation().field("details").matchJsonSchema("id STRING, amount DOUBLE")

    validation().field("details").matchJsonSchema("id STRING, amount DOUBLE", true) //check values do not match JSON schema
    ```

=== "Scala"

    ```scala
    validation.field("details").matchJsonSchema("id STRING, amount DOUBLE")

    validation.field("details").matchJsonSchema("id STRING, amount DOUBLE", true) //check values do not match JSON schema
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "details"
            validation:
              - type: "matchJsonSchema"
                schema: "id STRING, amount DOUBLE"
              - type: "matchJsonSchema"
                schema: "id STRING, amount DOUBLE"
                negate: true
    ```

## Match Date Time Format

Check if values of a field match date time format ([defined formats](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)).

=== "Java"

    ```java
    validation().field("date").matchDateTimeFormat("yyyy-MM-dd")

    validation().field("date").matchDateTimeFormat("yyyy-MM-dd", true) //check values do not match date time format
    ```

=== "Scala"

    ```scala
    validation.field("date").matchDateTimeFormat("yyyy-MM-dd")

    validation.field("date").matchDateTimeFormat("yyyy-MM-dd", true) //check values do not match date time format
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "date"
            validation:
              - type: "matchDateTimeFormat"
                format: "yyyy-MM-dd"
              - type: "matchDateTimeFormat"
                format: "yyyy-MM-dd"
                negate: true
    ```

## Most Common Value In Set

Check if the most common field value exists in set of values.

=== "Java"

    ```java
    validation().field("name").mostCommonValueInSet("peter", "john")

    validation().field("name").mostCommonValueInSet(List.of("peter", "john"), true) //check is most common value does not exist in set
    ```

=== "Scala"

    ```scala
    validation.field("name").mostCommonValueInSet("peter", "john")

    validation.field("name").mostCommonValueInSet(List("peter", "john"), true) //check is most common value does not exist in set
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "name"
            validation:
              - type: "mostCommonValueInSet"
                values: ["peter", "john"]
              - type: "mostCommonValueInSet"
                values: ["peter", "john"]
                negate: true
    ```

## Unique Values Proportion Between

Check if the fields proportion of unique values is between two values.

=== "Java"

    ```java
    validation().field("name").uniqueValuesProportionBetween(0.1, 0.3)

    validation().field("name").uniqueValuesProportionBetween(0.1, 0.3, true) //check if proportion of unique values is not between two values
    ```

=== "Scala"

    ```scala
    validation.field("name").uniqueValuesProportionBetween(0.1, 0.3)

    validation.field("name").uniqueValuesProportionBetween(0.1, 0.3, true) //check if proportion of unique values is not between two values
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "name"
            validation:
              - type: "uniqueValuesProportionBetween"
                min: 0.1
                max: 0.3
              - type: "uniqueValuesProportionBetween"
                min: 0.1
                max: 0.3
                negate: true
    ```

## Quantile Values Between

Check if quantiles of field values is within range.

=== "Java"

    ```java
    validation().field("amount").quantileValuesBetween(Map.of(0.1, new Tuple2(1.0, 2.0)))

    validation().field("amount").quantileValuesBetween(Map.of(0.1, new Tuple2(1.0, 2.0)), true) //check if quantile value is not between two values
    ```

=== "Scala"

    ```scala
    validation.field("amount").quantileValuesBetween(Map(0.1 -> (1.0, 2.0)))

    validation.field("amount").quantileValuesBetween(Map(0.1 -> (1.0, 2.0)), true) //check if quantile value is not between two values
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - field: "amount"
            validation:
              - type: quantileValuesBetween
                quantileRanges:
                  "0.1":
                    - 1.0
                    - 10.0
              - type: "quantileValuesBetween"
                quantileRanges:
                  "0.1":
                    - 1.0
                    - 10.0
                negate: true
    ```

## Expression

Ensure all data in field adheres to SQL expression defined that returns back a boolean. You can define complex logic in
here that could combine multiple fields.
  
For example, `CASE WHEN status == 'open' THEN balance > 0 ELSE balance == 0 END` would check all rows with `status`
open to have `balance` greater than 0, otherwise, check the `balance` is 0.

=== "Java"

    ```java
    var csvTxns = csv("transactions", "/tmp/csv", Map.of("header", "true"))
      .validations(
        validation().expr("amount < 100"),
        validation().expr("year == 2021").errorThreshold(0.1),  //equivalent to if error percentage is > 10%, then fail
        validation().expr("REGEXP_LIKE(name, 'Peter .*')").errorThreshold(200)  //equivalent to if number of errors is > 200, then fail
      );
    
    var conf = configuration().enableValidation(true);
    ```

=== "Scala"

    ```scala
    val csvTxns = csv("transactions", "/tmp/csv", Map("header" -> "true"))
      .validations(
        validation.expr("amount < 100"),
        validation.expr("year == 2021").errorThreshold(0.1),  //equivalent to if error percentage is > 10%, then fail
        validation.expr("REGEXP_LIKE(name, 'Peter .*')").errorThreshold(200)  //equivalent to if number of errors is > 200, then fail
      )
    
    val conf = configuration.enableValidation(true)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      transactions:
        options:
          path: "/tmp/csv"
        validations:
          - expr: "amount < 100"
          - expr: "year == 2021"
            errorThreshold: 0.1   #equivalent to if error percentage is > 10%, then fail
          - expr: "REGEXP_LIKE(name, 'Peter .*')"
            errorThreshold: 200   #equivalent to if number of errors is > 200, then fail
            description: "Should be lots of Peters"

    #enableValidation inside application.conf
    ```
