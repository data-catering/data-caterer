---
title: "Data Validations"
description: "Data Caterer can execute complex validations including field level, aggregate/group by, upstream data source and field name validations. It can also retrieve existing data quality/validation rules from sources such as Great Expectations or OpenMetadata."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Validations

Validations can be used to run data checks after you have run the data generator or even as a standalone task. A report
summarising the success or failure of the validations is produced and can be examined for further investigation.

<div class="grid cards" markdown>

- __[Basic]__ - Basic field level validations
- __[Group by/Aggregate]__ - Run aggregates over grouped data, then validate
- __[Upstream data source]__ - Ensure record values exist in datasets based on other data sources or data generated
- __[Field names]__ - Validate field names and ordering
- __[External validation source]__ - Use pre-existing validation rules from sources such as Great Expectations or OpenMetadata
- __Data Profile (Coming soon)__ - Score how close the data profile of generated data is against the target data profile

</div>

  [Basic]: validation/basic-validation.md
  [Group by/Aggregate]: validation/group-by-validation.md
  [Upstream data source]: validation/upstream-data-source-validation.md
  [Field names]: validation/field-name-validation.md
  [External validation source]: validation/external-source-validation.md

## Define Validations

Full example validations can be found below. For more details, check out each of the subsections defined further below.

=== "Java"

    ```java
    var csvTxns = csv("transactions", "/tmp/csv")
      .validations(
        validation().field("amount").lessThan(100),
        validation().field("year").isEqual(2021).errorThreshold(0.1),       //equivalent to if error percentage is > 10%, then fail
        validation().field("name").matches("Peter .*").errorThreshold(200)  //equivalent to if number of errors is > 200, then fail
      )
      .validationWait(waitCondition().pause(1));

    var conf = configuration().enableValidation(true);
    ```

=== "Scala"

    ```scala
    val csvTxns = csv("transactions", "/tmp/csv")
      .validations(
        validation.field("amount").lessThan(100),
        validation.field("year").isEqual(2021).errorThreshold(0.1),       //equivalent to if error percentage is > 10%, then fail
        validation.field("name").matches("Peter .*").errorThreshold(200)  //equivalent to if number of errors is > 200, then fail
      )  
      .validationWait(waitCondition.pause(1))

    val conf = configuration.enableValidation(true)
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      transactions:
        - options:
            path: "/tmp/csv"
          validations:
            - expr: "amount < 100"
            - field: "amount" #or
              validation:
                - type: "lessThan"
                  value: 100
            - expr: "year == 2021"
              errorThreshold: 0.1   #equivalent to if error percentage is > 10%, then fail
            - expr: "REGEXP_LIKE(name, 'Peter .*')"
              errorThreshold: 200   #equivalent to if number of errors is > 200, then fail
              description: "Should be lots of Peters"
            - expr: "amount > 100"
              preFilterExpr: "STARTSWITH(account_id, 'ACC')"
            - expr: "ISNOTNULL(name)"
              preFilterExpr: "STARTSWITH(account_id, 'ACC') AND ISNOTNULL(merchant)"
          waitCondition:
            pauseInSeconds: 1
    ```

## Pre-filter Data

If you need to run data validations on a subset of data, then you can define pre-filter conditions. An example would be 
when you want to check that for all records with `status=closed`, that `balance=0`, you would define a pre-filter like below:

=== "Java"

    ```java
    var csvTxns = csv("transactions", "/tmp/csv")
      .validations(
        validation().preFilter(fieldPreFilter("status").isEqual("closed")).field("balance").isEqual(0)
      );
    ```

=== "Scala"

    ```scala
    val csvTxns = csv("transactions", "/tmp/csv")
      .validations(
        validation.preFilter(fieldPreFilter("status").isEqual("closed")).field("balance").isEqual(0)
      )  
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      transactions:
        - options:
            path: "/tmp/csv"
          validations:
            - expr: "balance == 0"
              preFilterExpr: "status == 'closed'"
    ```

## Wait Condition

Once data has been generated, you may want to wait for a certain condition to be met before starting the data
validations. This can be via:

- Pause for seconds
- When file is available
- Data exists
- Webhook

### Pause

=== "Java"

    ```java
    var csvTxns = csv("transactions", "/tmp/csv")
      .validationWait(waitCondition().pause(1));
    ```

=== "Scala"

    ```scala
    val csvTxns = csv("transactions", "/tmp/csv")
      .validationWait(waitCondition.pause(1))
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      transactions:
        - options:
            path: "/tmp/csv"
          waitCondition:
            pauseInSeconds: 1
    ```

### Data exists

=== "Java"

    ```java
    var csvTxns = csv("transactions", "/tmp/csv")
      .validationWaitDataExists("updated_date > DATE('2023-01-01')");
    ```

=== "Scala"

    ```scala
    val csvTxns = csv("transactions", "/tmp/csv")
      .validationWaitDataExists("updated_date > DATE('2023-01-01')")
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      transactions:
        - options:
            path: "/tmp/csv"
          waitCondition:
            dataSourceName: "transactions"
            options:
              path: "/tmp/csv"
            expr: "updated_date > DATE('2023-01-01')"
    ```

### Webhook

=== "Java"

    ```java
    var csvTxns = csv("transactions", "/tmp/csv")
      .validationWait(waitCondition().webhook("http://localhost:8080/finished")); //by default, GET request successful when 200 status code
    
    //or
    
    var csvTxnsWithStatusCodes = csv("transactions", "/tmp/csv")
      .validationWait(waitCondition().webhook("http://localhost:8080/finished", "GET", 200, 202));  //successful if 200 or 202 status code

    //or
    
    var csvTxnsWithExistingHttpConnection = csv("transactions", "/tmp/csv")
      .validationWait(waitCondition().webhook("my_http", "http://localhost:8080/finished"));  //use connection configuration from existing 'my_http' connection definition
    ```

=== "Scala"

    ```scala
    val csvTxns = csv("transactions", "/tmp/csv")
      .validationWait(waitCondition.webhook("http://localhost:8080/finished"))  //by default, GET request successful when 200 status code

    //or

    val csvTxnsWithStatusCodes = csv("transactions", "/tmp/csv")
      .validationWait(waitCondition.webhook("http://localhost:8080/finished", "GET", 200, 202)) //successful if 200 or 202 status code

    //or

    val csvTxnsWithExistingHttpConnection = csv("transactions", "/tmp/csv")
      .validationWait(waitCondition.webhook("my_http", "http://localhost:8080/finished")) //use connection configuration from existing 'my_http' connection definition
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      transactions:
        - options:
            path: "/tmp/csv"
          waitCondition:
            url: "http://localhost:8080/finished" #by default, GET request successful when 200 status code

    #or
    
    ---
    name: "account_checks"
    dataSources:
      transactions:
        - options:
            path: "/tmp/csv"
          waitCondition:
            url: "http://localhost:8080/finished"
            method: "GET"
            statusCodes: [200, 202] #successful if 200 or 202 status code

    #or
    
    ---
    name: "account_checks"
    dataSources:
      transactions:
        - options:
            path: "/tmp/csv"
          waitCondition:
            dataSourceName: "my_http" #use connection configuration from existing 'my_http' connection definition
            url: "http://localhost:8080/finished"
    ```

### File exists

=== "Java"

    ```java
    var csvTxns = csv("transactions", "/tmp/csv")
      .validationWait(waitCondition().file("/tmp/json"));
    ```

=== "Scala"

    ```scala
    val csvTxns = csv("transactions", "/tmp/csv")
      .validationWait(waitCondition.file("/tmp/json"))
    ```

=== "YAML"

    ```yaml
    ---
    name: "account_checks"
    dataSources:
      transactions:
        - options:
            path: "/tmp/csv"
          waitCondition:
            path: "/tmp/json"
    ```

## Report

Once run, it will produce a report like [this](../sample/report/html/validations.html).
