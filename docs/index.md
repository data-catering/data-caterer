---
title: "Automated test data management tool"
description: "An open-source automated test data management tool that can automatically discover, generate and validate for
files, databases, HTTP APIs and messaging systems. Synthetically generate production-like data, verify via data quality rules
and delete data after finishing."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
hide:
  - navigation
  - toc
---

<h1 align="center" style="padding-top: 25px;"><b>Say Goodbye to Slow and Complex Integration Tests</b></h1>
<h3 align="center" style="padding-bottom: 25px">Automate end-to-end data tests for any job or application</h3>

<picture class="center-content">
<source media="(min-width: 650px)" srcset="diagrams/index/high_level_flow-run-config-basic-flow-basic-flow.svg">
<img src="diagrams/index/high_level_flow-run-config-basic-flow-basic-flow-vertical.svg"
alt="Data Caterer generate, validate and clean data testing flow">
</picture>

## Generate

Generate production-like data to test your jobs or applications. Create data in files, databases, HTTP APIs and messaging systems.

??? example "Generate data in existing Postgres database"

    You have existing tables in Postgres and you want to generate data for them whilst maintaining relationships between tables.

    === "Scala"
        ```scala
        val accountTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
        val config = configuration.enableGeneratePlanAndTasks(true)
        ```

    === "Java"
        ```java
        var accountTask = postgres("customer_accounts", "jdbc:postgresql://localhost:5432/customer");
        var config = configuration().enableGeneratePlanAndTasks(true);
        ```

    === "YAML"
        ```yaml
        #with env variable ENABLE_GENERATE_PLAN_AND_TASKS=true
        name: "postgres_example_plan"
        tasks:
          - name: "postgres_account"
            dataSourceName: "postgresCustomer"
        ```

    === "UI"
        1. Click on `Connection` tab and add your Postgres conneciton
        1. Go back to `Home` tab and `Select data source` as your Postgres connection
        1. Click on `Generate` and select `Auto`
        1. Click on `Execute` to generate data

    ??? example "Sample"
        ```sql
        SELECT * FROM account.accounts LIMIT 1;
    
        account_number | account_status |     created_by      | created_by |     open_timestamp      
        ---------------+----------------+---------------------+------------+------------------------
        0499572486     | closed         | Stewart Hartmann    | eod        | 2023-12-02 12:30:37.602 
    
        SELECT * FROM account.balances where account_number='0499572486';
    
        account_number |  balance  |    update_timestamp      
        ---------------+-----------+------------------------
        0499572486     | 285293.23 | 2024-01-30 03:30:29.012 
    
        SELECT * FROM account.transactions where account_number='0499572486';
    
        account_number |  amount  |    create_timestamp      
        ---------------+----------+------------------------
        0499572486     |  1893.46 | 2024-03-13 18:05:45.565
        ```

??? note "Create, get and delete pets via HTTP API using the same `id`"

    First, generate data for creating pets via POST, then get pets via GET and finally delete pets via DELETE, all using 
    the same `id`.

    === "Scala"
        ```scala
        val httpTask = http("my_http")
          .fields(metadataSource.openApi("/opt/app/http/petstore.json"))
          .count(count.records(10))

        val myPlan = plan.addForeignKeyRelationship(
          foreignField("my_http", "POST/pets", "body.id"),
          foreignField("my_http", "GET/pets/{id}", "pathParamid"),
          foreignField("my_http", "DELETE/pets/{id}", "pathParamid")
        )
        ```

    === "Java"
        ```java
        var httpTask = http("my_http")
            .fields(metadataSource().openApi("/opt/app/http/petstore.json"))
            .count(count().records(10));

        var myPlan = plan().addForeignKeyRelationship(
                foreignField("my_http", "POST/pets", "body.id"),
                foreignField("my_http", "GET/pets/{id}", "pathParamid"),
                foreignField("my_http", "DELETE/pets/{id}", "pathParamid")
        );
        ```

    === "YAML"
        ```yaml
        ---
        name: "http_openapi"
        steps:
          - name: "pets"
            count:
              records: 10
            options:
              metadataSourceType: "openApi"
              schemaLocation: "/opt/app/http/petstore.json"
        ---
        name: "http_plan"
        tasks:
          - name: "http_openapi"
            dataSourceName: "http"
        
        sinkOptions:
          foreignKeys:
            - source:
                dataSource: "http"
                step: "POST/pets"
                fields: ["body.id"]
              generate:
                - dataSource: "http"
                  step: "GET/pets/{id}"
                  fields: ["pathParamid"]
                - dataSource: "http"
                  step: "DELETE/pets/{id}"
                  fields: ["pathParamid"]
        ```
    
    === "UI"
        1. Click on `Connection` tab, add your OpenAPI/Swagger connection to file and add HTTP connection
        1. Go back to `Home` tab and `Select data source` as your HTTP connection
        1. Click on `Generate`, select `Auto with metadata source` and then select your OpenAPI/Swagger connection
        1. Go to `Relationships` and click on `+ Relationship`
            1. For Source, select your task name, field as `body.id`, method as `POST` and endpoint as `/pets`
            1. Click on `Generation` and `+ Link`, select your task name, field as `pathParamid`, method as `GET` and endpoint as `/pets/{id}`
            1. Click on `+ Link`, select your task name, field as `pathParamid`, method as `DELETE` and endpoint as `/pets/{id}`
        1. Click on `Advanced Configuration`, open `Flag` and enable `Generate Plan And Tasks`
        1. Click on `Execute` to generate data

    ??? note "Sample"
        ```json
        [
          {
            "method": "POST",
            "url": "http://localhost/anything/pets",
            "body": {
              "id": "ZzDRmGMnoei9M5D", 
              "name": "Dave Windler"
            }
          },
          {
            "method": "GET",
            "url": "http://localhost/anything/pets/ZzDRmGMnoei9M5D"
          },
          {
            "method": "DELETE",
            "url": "http://localhost/anything/pets/ZzDRmGMnoei9M5D"
          }
        ]
        ```

??? abstract "Populate Kafka topic with account events"

    Create fresh data in your Kafka topics for account events with nested structures.

    === "Scala"
        ```scala
        val kafkaTask = kafka("my_kafka", "localhost:9092")
          .topic("accounts")
          .fields(field.name("key").sql("body.account_id"))
          .fields(
            field.messageBody(
              field.name("account_id").regex("ACC[0-9]{8}"),
              field.name("account_status").oneOf("open", "closed", "suspended", "pending"),
              field.name("balance").`type`(DoubleType).round(2),
              field.name("details")
                .fields(
                  field.name("name").expression("#{Name.name}"),
                  field.name("open_date").`type`(DateType).min(LocalDate.now())
                )
            )
          )
        ```

    === "Java"
        ```java
        var kafkaTask = kafka("my_kafka", "localhost:9092")
                .topic("accounts")
                .fields(field().name("key").sql("body.account_id"))
                .fields(
                        field().messageBody(
                                field().name("account_id").regex("ACC[0-9]{8}"),
                                field().name("account_status").oneOf("open", "closed", "suspended", "pending"),
                                field().name("balance").type(DoubleType.instance()).round(2),
                                field().name("details")
                                        .fields(
                                                field().name("name").expression("#{Name.name}"),
                                                field().name("open_date").type(DateType.instance()).min(LocalDate.now())
                                        )
                        )
                )
        ```

    === "YAML"
        ```yaml
        ---
        name: "simple_kafka"
        steps:
        - name: "kafka_account"
          type: "kafka"
          options:
            topic: "accounts"
          fields:
            - name: "key"
              type: "string"
              options:
                sql: "body.account_id"
            - name: "messageBody"
              type: struct
              fields:
              - name: "account_id"
                options:
                  regex: "ACC[0-9]{8}"
              - name: "account_status"
                options:
                  oneOf: ["open", "closed", "suspended", "pending"]
              - name: "balance"
                type: "double"
                options:
                  round: 2
              - name: "details"
                type: struct
                fields:
                  - name: "name"
                  - name: "open_date"
                    type: "date"
                    options:
                      min: "now()"
        ```

    === "UI"
        1. Click on `Connection` tab, add your Kafka connection
        1. Go back to `Home` tab, `Select data source` as your Kafka connection and put topic as `accounts`
        1. Click on `Generate` and select `Manual` checkbox
            1. Click on `+ Field`, add name `key` with type `string`
                1. Click on `+`, select `SQL` and enter `body.account_id`
            1. Click on `+ Field`, add name `messageBody` with type `struct`
                1. Click on inner `+ Field`, add name `account_id` with type `string`
                1. Click on `+`, select `Regex` and enter `ACC[0-9]{8}`
                1. Click on inner `+ Field`, add name `account_status` with type `string`
                1. Click on `+`, select `One Of` and enter `open, closed, suspended, pending`
                1. Click on inner `+ Field`, add name `balance` with type `double`
                1. Click on `+`, select `Round` and enter `2`
                1. Click on inner `+ Field`, add name `details` with type `struct`
                    1. Click on inner `+ Field`, add name `name` with type `string`
                    1. Click on inner `+ Field`, add name `open_date` with type `date`
                        1. Click on `+`, select `Min` and enter `now()`
        1. Click on `Execute` to generate data

    ??? abstract "Sample"
        ```json
        [
          {
            "account_id":"ACC35554421",
            "account_status":"open",
            "balance":89139.62,
            "details":{
              "name":"Jonie Farrell",
              "open_date":"2025-01-15"
            }
          },
          {
            "account_id":"ACC30149813",
            "account_status":"closed",
            "balance":28861.09,
            "details":{
              "name":"Debrah Douglas",
              "open_date":"2025-01-17"
            }
          },
          {
            "account_id":"ACC58341320",
            "account_status":"pending",
            "balance":57543.91,
            "details":{
              "name":"Elmer Lind",
              "open_date":"2025-01-20"
            }
          }
        ]
        ```

## And Validate

Ensure your job or service is working as expected before going to production by generating data, ingesting it and then 
validating the downstream data sources have the correct information.

??? tip "Check all generated records from CSV exist in Iceberg"

    Run data generation for CSV file (based on schema from data contract), consume it from your job (that produces an 
    Iceberg table) and then validate it.

    === "Scala"
        ```scala
        val csvTask = csv("csv_accounts", "/data/csv/customer/account", Map("header" -> "true"))
          .fields(metadataSource.openDataContractStandard("/opt/app/mount/odcs/full-example.odcs.yaml"))
        
        val icebergTask = iceberg("iceberg_accounts", "dev.accounts", "/data/iceberg/customer/account")
          .validations(
            validation.unique("account_id"),
            validation.groupBy("account_id").sum("balance").greaterThan(0),
            validation.field("open_time").isIncreasing(),
            validation.count().isEqual(1000)
          )
          .validationWait(waitCondition.file("/data/iceberg/customer/account"))
        ```

    === "Java"
        ```java
        var csvTask = csv("csv_accounts", "/data/csv/customer/account", Map.of("header", "true"))
                .fields(metadataSource().openDataContractStandard("/opt/app/mount/odcs/full-example.odcs.yaml"));

        var icebergTask = iceberg("iceberg_accounts", "dev.accounts", "/data/iceberg/customer/account")
                .validations(
                        validation().unique("account_id"),
                        validation().groupBy("account_id").sum("balance").greaterThan(0),
                        validation().field("open_time").isIncreasing(),
                        validation().count().isEqual(1000)
                )
                .validationWait(waitCondition().file("/data/iceberg/customer/account"));
        ```

    === "YAML"
        ```yaml
        ---
        name: "csv_accounts"
        steps:
        - name: "accounts"
          type: "csv"
          options:
            path: "/data/csv/customer/account"
            metadataSourceType: "openDataContractStandard"
            dataContractFile: "/opt/app/mount/odcs/full-example.odcs.yaml"
        ---
        name: "iceberg_account_checks"
        dataSources:
          iceberg:
            - options:
                path: "/data/iceberg/customer/account"
              validations:
                - field: "account_id"
                  validation:
                    - type: "unique"
                - field: "open_time"
                  validation:
                    - type: "isIncreasing"
                - groupByFields: [ "account_id" ]
                  aggType: "sum"
                  aggExpr: "sum(balance) > 0"
                - aggType: "count"
                  aggExpr: "count == 1000"
        ```

    === "UI"
        1. Click on `Connection` tab, add your CSV, Iceberg and ODCS (Open Data Contract Standard) connection
        1. Go back to `Home` tab, enter task name as `csv_accounts` and `Select data source` as your CSV connection
        1. Click on `Generate` and select `Auto from metadata source` checkbox
            1. Select your ODCS connection as the metadata source
        1. Click on `+ Task`, select `Iceberg` and select your Iceberg connection
            1. Click on `+ Validation`, select `Field`, enter `account_id` and select `Unique`
            1. Click on `+ Validation`, select `Group By` and enter `account_id`
                1. Click on `+`, select `Sum` and enter `balance > 0`
            1. Click on `+ Validation`, select `Field` and enter `open_time`
                1. Click on `+`, select `Is Increasing`
            1. Click on `+ Validation`, select `Group By` and enter `account_id`
              1. Click on `+`, select `Count`, click on `+` next to count, select `Equal` and enter `1000`
        1. Click on `Execute` to generate data

??? success "Use validations from Great Expectations"

    If you have existing data quality rules from an external source like Great Expectations, you can use them to 
    validate your data without rewriting them as part of your tests.

    === "Scala"
        ```scala
        val jsonTask = json("my_json", "/opt/app/data/taxi_json")
          .validations(metadataSource.greatExpectations("/opt/app/mount/ge/taxi-expectations.json"))
        ```

    === "Java"
        ```java
        var jsonTask = json("my_json", "/opt/app/data/taxi_json")
                .validations(metadataSource().greatExpectations("/opt/app/mount/ge/taxi-expectations.json"));
        ```

    === "YAML"
        ```yaml
        name: "taxi_data_checks"
        dataSources:
          json:
            - options:
                path: "/opt/app/data/taxi_json"
                metadataSourceType: "greatExpectations"
                expectationsFile: "/opt/app/mount/ge/taxi-expectations.json"
        ```

    === "UI"
        1. Click on `Connection` tab, select data source type as `Great Expecations`
            1. Enter `Expectations file` as `/opt/app/mount/ge/taxi-expectations.json`
        1. Click on `Home` tab, `Select data source` as your JSON connection
        1. Open `Validation` and select checkbox `Auto from metadata source`
            1. Select your Great Expectations connection as the metadata source
        1. Click on `Execute` to generate data

??? question "Complex validations based on pre-conditions or upstream data"

    - Check `balance` is `0` when `status` is `closed`
    - Check `open_time` is the same in CSV and Iceberg
    - Check sum of `amount` in Iceberg is the same as `balance` in CSV for each `account_id`

    === "Scala"
        ```scala
        val icebergTask = iceberg("iceberg_accounts", "dev.accounts", "/data/iceberg/customer/account")
          .validations(
            validation.preFilter(validation.field("status").isEqual("closed")).field("balance").isEqual(0),
            validation.upstreamData(accountTask)
              .joinFields("account_id")
              .validations(
                validation.field("open_time").isEqualField("csv_accounts.open_time"),
                validation.groupBy("account_id", "csv_accounts_balance").sum("amount").isEqualField("csv_accounts_balance")
              )
          )
        ```

    === "Java"
        ```java
        var icebergTask = iceberg("iceberg_accounts", "dev.accounts", "/data/iceberg/customer/account")
                .validations(
                        validation().preFilter(validation().field("status").isEqual("closed")).field("balance").isEqual(0),
                        validation().upstreamData(accountTask)
                                .joinFields("account_id")
                                .validations(
                                        validation().field("open_time").isEqualField("csv_accounts.open_time"),
                                        validation().groupBy("account_id", "csv_accounts_balance").sum("amount").isEqualField("csv_accounts_balance")
                                )
                );
        ```

    === "YAML"
        ```yaml
        ---
        name: "iceberg_account_checks"
        dataSources:
          iceberg:
            - options:
                path: "/data/iceberg/customer/account"
              validations:
                - preFilterExpr: "status == 'closed'"
                  expr: "balance == 0"
                - upstreamDataSource: "csv_accounts"
                  joinFields: ["account_id"]
                  validations:
                    - expr: "open_time == csv_accounts.open_time"
                    - groupByFields: ["account_id", "csv_accounts_balance"]
                      aggType: "sum"
                      aggExpr: "sum(amount) == csv_accounts_balance"
        ```

    === "UI"
        1. Click on `+ Task`, select `Iceberg` and select your Iceberg connection
            1. Pre-filter is not available yet via UI but will be soon!
            1. Click on `+ Validation`, select `Upstream` and enter `csv_accounts`
              1. Click on `+`, select `Join Field(s)` and enter `account_id`
              1. Click on `+ Validation`, select `Field` and enter `open_time`
                1. Click on `+`, select `Equal` and enter `csv_accounts.open_time`
              1. Click on `+ Validation`, select `Group By` and enter `account_id, csv_accounts_balance`
                1. Click on `+`, select `Sum` and enter `amount`
                1. Click on `+`, select `Equal` and enter `csv_accounts_balance`
        1. Click on `Execute` to generate data

## Why use Data Caterer

- **Catch bugs before production**: Bring stability to your data pipelines
- **Speed up your development cycles**: Fast feedback testing locally and in test environments
- **Single tool for all data sources**: No custom scripts needed
- **No production data or connection required**: Secure first approach, fully metadata driven
- **Easy to use for testers and developers**: Use either UI, Java, Scala or YAML
- **Simulate complex data flows**: Maintain relationships across data sources

## Main features

- :material-connection: [Connect to any data source](docs/connection.md)
- :material-auto-fix: [Auto generate production-like data from data connections or metadata sources](docs/guide/scenario/auto-generate-connection.md)
- :material-relation-many-to-one: [Relationships across data sources](docs/generator/foreign-key.md)
- :material-test-tube: [Validate based on data generated](docs/validation.md)
- :material-delete-sweep: [Clean up generated and downstream data](docs/delete-data.md)

<span class="center-content">
[Try now](get-started/quick-start.md){ .md-button .md-button--primary .button-spaced }
[Demo](sample/ui/index.html){ .md-button .md-button--primary .button-spaced }
</span>

## What it is

<div class="grid cards" markdown>

-   :material-tools: __Test data management tool__

    ---

    Generate synthetic production-like data to be consumed and validated. Clean up the data after using to keep your 
    environments clean.

-   :material-test-tube: __Run locally and in test environments__

    ---

    Fast feedback loop for developers and testers to ensure the data is correct before going to production.

-   :material-connection: __Designed for any data source__

    ---

    Support for pushing data to any data source, in any format, batch or real-time.

-   :material-code-tags-check: __High/Low/No code solution__

    ---

    Use the tool via either UI, Java, Scala or YAML.

-   :material-run-fast: __Developer productivity tool__

    ---

    If you are a new developer or seasoned veteran, cut down on your feedback loop when developing with data.

</div>

## Who can use it

| Type      | Interface                                              | User                                 |
|-----------|--------------------------------------------------------|--------------------------------------|
| No Code   | [UI](get-started/quick-start.md#mac)                   | QA, Testers, Data Scientist, Analyst |
| Low Code  | [YAML](get-started/quick-start.md#yaml)                | DevOps, Kubernetes Fans              |
| High Code | [Java/Scala](get-started/quick-start.md#javascala-api) | Software Developers, Data Engineers  |

<span class="center-content">
[Try now](get-started/quick-start.md){ .md-button .md-button--primary .button-spaced }
[Demo](sample/ui/index.html){ .md-button .md-button--primary .button-spaced }
</span>
