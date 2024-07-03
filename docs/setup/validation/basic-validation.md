---
title: "Column Validations"
description: "Examples of column level validations for data in files, databases, HTTP APIs or messaging systems via Data Catering."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Basic Validations

Run validations on a column to ensure the values adhere to your requirement. Can be set to complex validation logic
via SQL expression as well if needed (see [**here**](#expression)).

## Pre-filter

If you want to only run the validation on a specific subset of data, you can define pre-filter conditions. [Find more 
details here](../validation.md#pre-filter-data).

## Equal

Ensure all data in column is equal to certain value. Value can be of any data type. Can use `isEqualCol` to define SQL
expression that can reference other columns.

=== "Java"

    ```java
    validation().col("year").isEqual(2021),
    validation().col("year").isEqualCol("YEAR(date)"),
    ```

=== "Scala"

    ```scala
    validation.col("year").isEqual(2021),
    validation.col("year").isEqualCol("YEAR(date)"),
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "year == 2021"
    ```

## Not Equal

Ensure all data in column is not equal to certain value. Value can be of any data type. Can use `isNotEqualCol` to 
define SQL expression that can reference other columns.

=== "Java"

    ```java
    validation().col("year").isNotEqual(2021),
    validation().col("year").isNotEqualCol("YEAR(date)"),
    ```

=== "Scala"

    ```scala
    validation.col("year").isNotEqual(2021)
    validation.col("year").isEqualCol("YEAR(date)"),
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "year != 2021"
    ```

## Null

Ensure all data in column is null.

=== "Java"

    ```java
    validation().col("year").isNull()
    ```

=== "Scala"

    ```scala
    validation.col("year").isNull
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "ISNULL(year)"
    ```

## Not Null

Ensure all data in column is not null.

=== "Java"

    ```java
    validation().col("year").isNotNull()
    ```

=== "Scala"

    ```scala
    validation.col("year").isNotNull
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "ISNOTNULL(year)"
    ```

## Contains

Ensure all data in column is contains certain string. Column has to have type string.

=== "Java"

    ```java
    validation().col("name").contains("peter")
    ```

=== "Scala"

    ```scala
    validation.col("name").contains("peter")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "CONTAINS(name, 'peter')"
    ```

## Not Contains

Ensure all data in column does not contain certain string. Column has to have type string.

=== "Java"

    ```java
    validation().col("name").notContains("peter")
    ```

=== "Scala"

    ```scala
    validation.col("name").notContains("peter")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "!CONTAINS(name, 'peter')"
    ```

## Unique

Ensure all data in column is unique.


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

Ensure all data in column is less than certain value. Can use `lessThanCol` to define SQL expression that can reference 
other columns.

=== "Java"

    ```java
    validation().col("amount").lessThan(100),
    validation().col("amount").lessThanCol("balance + 1"),
    ```

=== "Scala"

    ```scala
    validation.col("amount").lessThan(100),
    validation.col("amount").lessThanCol("balance + 1"),
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
    ```

## Less Than Or Equal

Ensure all data in column is less than or equal to certain value. Can use `lessThanOrEqualCol` to define SQL expression 
that can reference other columns.

=== "Java"

    ```java
    validation().col("amount").lessThanOrEqual(100),
    validation().col("amount").lessThanOrEqualCol("balance + 1"),
    ```

=== "Scala"

    ```scala
    validation.col("amount").lessThanOrEqual(100),
    validation.col("amount").lessThanCol("balance + 1"),
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "amount <= 100"
          - expr: "amount <= balance + 1"
    ```

## Greater Than

Ensure all data in column is greater than certain value. Can use `greaterThanCol` to define SQL expression
that can reference other columns.

=== "Java"

    ```java
    validation().col("amount").greaterThan(100),
    validation().col("amount").greaterThanCol("balance"),
    ```

=== "Scala"

    ```scala
    validation.col("amount").greaterThan(100),
    validation.col("amount").greaterThanCol("balance"),
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
    ```

## Greater Than Or Equal

Ensure all data in column is greater than or equal to certain value. Can use `greaterThanOrEqualCol` to define SQL 
expression that can reference other columns.

=== "Java"

    ```java
    validation().col("amount").greaterThanOrEqual(100),
    validation().col("amount").greaterThanOrEqualCol("balance"),
    ```

=== "Scala"

    ```scala
    validation.col("amount").greaterThanOrEqual(100),
    validation.col("amount").greaterThanOrEqualCol("balance"),
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "amount >= 100"
          - expr: "amount >= balance"
    ```

## Between

Ensure all data in column is between two values. Can use `betweenCol` to define SQL expression that references other 
columns.

=== "Java"

    ```java
    validation().col("amount").between(100, 200),
    validation().col("amount").betweenCol("balance * 0.9", "balance * 1.1"),
    ```

=== "Scala"

    ```scala
    validation.col("amount").between(100, 200),
    validation.col("amount").betweenCol("balance * 0.9", "balance * 1.1"),
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
    ```

## Not Between

Ensure all data in column is not between two values. Can use `notBetweenCol` to define SQL expression that references 
other columns.

=== "Java"

    ```java
    validation().col("amount").notBetween(100, 200),
    validation().col("amount").notBetweenCol("balance * 0.9", "balance * 1.1"),
    ```

=== "Scala"

    ```scala
    validation.col("amount").notBetween(100, 200)
    validation.col("amount").notBetweenCol("balance * 0.9", "balance * 1.1"),
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "amount NOT BETWEEN 100 AND 200"
          - expr: "amount NOT BETWEEN balance * 0.9 AND balance * 1.1"
    ```

## In

Ensure all data in column is in set of defined values.

=== "Java"

    ```java
    validation().col("status").in("open", "closed")
    ```

=== "Scala"

    ```scala
    validation.col("status").in("open", "closed")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "status IN ('open', 'closed')"
    ```

## Matches

Ensure all data in column matches certain regex expression.

=== "Java"

    ```java
    validation().col("account_id").matches("ACC[0-9]{8}")
    ```

=== "Scala"

    ```scala
    validation.col("account_id").matches("ACC[0-9]{8}")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "REGEXP(account_id, ACC[0-9]{8})"
    ```

## Not Matches

Ensure all data in column does not match certain regex expression.

=== "Java"

    ```java
    validation().col("account_id").notMatches("^acc.*")
    ```

=== "Scala"

    ```scala
    validation.col("account_id").notMatches("^acc.*")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "!REGEXP(account_id, '^acc.*')"
    ```

## Starts With

Ensure all data in column starts with certain string. Column has to have type string.

=== "Java"

    ```java
    validation().col("account_id").startsWith("ACC")
    ```

=== "Scala"

    ```scala
    validation.col("account_id").startsWith("ACC")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "STARTSWITH(account_id, 'ACC')"
    ```

## Not Starts With

Ensure all data in column does not start with certain string. Column has to have type string.

=== "Java"

    ```java
    validation().col("account_id").notStartsWith("ACC")
    ```

=== "Scala"

    ```scala
    validation.col("account_id").notStartsWith("ACC")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "!STARTSWITH(account_id, 'ACC')"
    ```

## Ends With

Ensure all data in column ends with certain string. Column has to have type string.

=== "Java"

    ```java
    validation().col("account_id").endsWith("ACC")
    ```

=== "Scala"

    ```scala
    validation.col("account_id").endsWith("ACC")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "ENDWITH(account_id, 'ACC')"
    ```

## Not Ends With

Ensure all data in column does not end with certain string. Column has to have type string.

=== "Java"

    ```java
    validation().col("account_id").notEndsWith("ACC")
    ```

=== "Scala"

    ```scala
    validation.col("account_id").notEndsWith("ACC")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "!ENDWITH(account_id, 'ACC')"
    ```

## Size

Ensure all data in column has certain size. Column has to have type array or map.

=== "Java"

    ```java
    validation().col("transactions").size(5)
    ```

=== "Scala"

    ```scala
    validation.col("transactions").size(5)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "SIZE(transactions, 5)"
    ```

## Not Size

Ensure all data in column does not have certain size. Column has to have type array or map.

=== "Java"

    ```java
    validation().col("transactions").notSize(5)
    ```

=== "Scala"

    ```scala
    validation.col("transactions").notSize(5)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "SIZE(transactions) != 5"
    ```

## Less Than Size

Ensure all data in column has size less than certain value. Column has to have type array or map.

=== "Java"

    ```java
    validation().col("transactions").lessThanSize(5)
    ```

=== "Scala"

    ```scala
    validation.col("transactions").lessThanSize(5)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "SIZE(transactions) < 5"
    ```

## Less Than Or Equal Size

Ensure all data in column has size less than or equal to certain value. Column has to have type array or map.

=== "Java"

    ```java
    validation().col("transactions").lessThanOrEqualSize(5)
    ```

=== "Scala"

    ```scala
    validation.col("transactions").lessThanOrEqualSize(5)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "SIZE(transactions) <= 5"
    ```

## Greater Than Size

Ensure all data in column has size greater than certain value. Column has to have type array or map.

=== "Java"

    ```java
    validation().col("transactions").greaterThanSize(5)
    ```

=== "Scala"

    ```scala
    validation.col("transactions").greaterThanSize(5)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "SIZE(transactions) > 5"
    ```

## Greater Than Or Equal Size

Ensure all data in column has size greater than or equal to certain value. Column has to have type array or map.

=== "Java"

    ```java
    validation().col("transactions").greaterThanOrEqualSize(5)
    ```

=== "Scala"

    ```scala
    validation.col("transactions").greaterThanOrEqualSize(5)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "SIZE(transactions) >= 5"
    ```

## Luhn Check

Ensure all data in column passes luhn check. Luhn check is used to validate credit card numbers and certain
identification numbers (see [here](https://en.wikipedia.org/wiki/Luhn_algorithm) for more details).

=== "Java"

    ```java
    validation().col("credit_card").luhnCheck()
    ```

=== "Scala"

    ```scala
    validation.col("credit_card").luhnCheck
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "LUHN_CHECK(credit_card)"
    ```

## Has Type

Ensure all data in column has certain data type.

=== "Java"

    ```java
    validation().col("id").hasType("string")
    ```

=== "Scala"

    ```scala
    validation.col("id").hasType("string")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      ...
        validations:
          - expr: "TYPEOF(id) == 'string'"
    ```

## Expression

Ensure all data in column adheres to SQL expression defined that returns back a boolean. You can define complex logic in
here that could combine multiple columns.
  
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
