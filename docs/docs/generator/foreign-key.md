---
title: "Relationships/Foreign Keys"
description: "Relationships/foreign keys can be defined within Data Caterer to help generate data that requires the same values to be used. Can be simple, compound or complex relationships that reference other fields with transformations."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Relationships/Foreign Keys

![Multiple data source foreign key example](../../diagrams/foreign_keys.drawio.png "Multiple data source foreign keys")

Foreign keys can be defined to represent the relationships between datasets where values are required to match for
particular fields.

## Single field

Define a field in one data source to match against another field.  
Below example shows a `postgres` data source with two tables, `accounts` and `transactions` that have a foreign key
for `account_id`.

=== "Java"

    ```java
    var postgresAcc = postgres("my_postgres", "jdbc:...")
      .table("public.accounts")
      .fields(
        field().name("account_id"),
        field().name("name"),
        ...
      );
    var postgresTxn = postgres(postgresAcc)
      .table("public.transactions")
      .fields(
        field().name("account_id"),
        field().name("full_name"),
        ...
      );
    
    plan().addForeignKeyRelationship(
      postgresAcc, "account_id",
      List.of(Map.entry(postgresTxn, "account_id"))
    );
    ```

=== "Scala"

    ```scala
    val postgresAcc = postgres("my_postgres", "jdbc:...")
      .table("public.accounts")
      .fields(
        field.name("account_id"),
        field.name("name"),
        ...
      )
    val postgresTxn = postgres(postgresAcc)
      .table("public.transactions")
      .fields(
        field.name("account_id"),
        field.name("full_name"),
        ...
      )

    plan.addForeignKeyRelationship(
      postgresAcc, "account_id",
      List(postgresTxn -> "account_id")
    )
    ```

=== "YAML"

    ```yaml
    ---
    name: "postgres_data"
    steps:
      - name: "accounts"
        type: "postgres"
        options:
          dbtable: "account.accounts"
        fields:
          - name: "account_id"
          - name: "name"
      - name: "transactions"
        type: "postgres"
        options:
          dbtable: "account.transactions"
        fields:
          - name: "account_id"
          - name: "full_name"
    ---
    name: "customer_create_plan"
    description: "Create customers in JDBC"
    tasks:
      - name: "postgres_data"
        dataSourceName: "my_postgres"

    sinkOptions:
      foreignKeys:
        - source:
            dataSource: "postgres"
            step: "accounts"
            fields: ["account_id"]
          generate:
            - dataSource: "postgres"
              step: "transactions"
              fields: ["account_id"]
    ```

## Multiple fields

You may have a scenario where multiple fields need to be aligned. From the same example, we want `account_id`
and `name` from `accounts` to match with `account_id` and `full_name` to match in `transactions` respectively.

=== "Java"

    ```java
    var postgresAcc = postgres("my_postgres", "jdbc:...")
      .table("public.accounts")
      .fields(
        field().name("account_id"),
        field().name("name"),
        ...
      );
    var postgresTxn = postgres(postgresAcc)
      .table("public.transactions")
      .fields(
        field().name("account_id"),
        field().name("full_name"),
        ...
      );
    
    plan().addForeignKeyRelationship(
      postgresAcc, List.of("account_id", "name"),
      List.of(Map.entry(postgresTxn, List.of("account_id", "full_name")))
    );
    ```

=== "Scala"

    ```scala
    val postgresAcc = postgres("my_postgres", "jdbc:...")
      .table("public.accounts")
      .fields(
        field.name("account_id"),
        field.name("name"),
        ...
      )
    val postgresTxn = postgres(postgresAcc)
      .table("public.transactions")
      .fields(
        field.name("account_id"),
        field.name("full_name"),
        ...
      )

    plan.addForeignKeyRelationship(
      postgresAcc, List("account_id", "name"),
      List(postgresTxn -> List("account_id", "full_name"))
    )
    ```

=== "YAML"

    ```yaml
    ---
    name: "postgres_data"
    steps:
      - name: "accounts"
        type: "postgres"
        options:
          dbtable: "account.accounts"
        fields:
          - name: "account_id"
          - name: "name"
      - name: "transactions"
        type: "postgres"
        options:
          dbtable: "account.transactions"
        fields:
          - name: "account_id"
          - name: "full_name"
    ---
    name: "customer_create_plan"
    description: "Create customers in JDBC"
    tasks:
      - name: "postgres_data"
        dataSourceName: "my_postgres"

    sinkOptions:
      foreignKeys:
        - source:
            dataSource: "postgres"
            step: "accounts"
            fields: ["account_id", "name"]
          generate:
            - dataSource: "postgres"
              step: "transactions"
              fields: ["account_id", "full_name"]
    ```

## Transformed field

Scenarios exist where there are relationships defined by certain transformations being applied to the source data.

For example, there may be accounts created with a field `account_number` that contains records like `123456`. Then another
data source contains `account_id` which is a concatenation of `ACC` with `account_number` to have values like `ACC123456`.

=== "Java"

    ```java
    var postgresAcc = postgres("my_postgres", "jdbc:...")
      .table("public.accounts")
      .fields(
        field().name("account_number"),
        field().name("name"),
        ...
      );
    var jsonTask = json("my_json", "/tmp/json")
      .fields(
        field().name("account_id").sql("CONCAT('ACC', account_number)"),
        field().name("account_number").omit(true),  #using this field for intermediate calculation, not included in final result with omit=true
        ...
      );
    
    plan().addForeignKeyRelationship(
      postgresAcc, List.of("account_number"),
      List.of(Map.entry(jsonTask, List.of("account_number")))
    );
    ```

=== "Scala"

    ```scala
    val postgresAcc = postgres("my_postgres", "jdbc:...")
      .table("public.accounts")
      .fields(
        field.name("account_number"),
        field.name("name"),
        ...
      )
    var jsonTask = json("my_json", "/tmp/json")
      .fields(
        field.name("account_id").sql("CONCAT('ACC', account_number)"),
        field.name("account_number").omit(true),  #using this field for intermediate calculation, not included in final result with omit=true
        ...
      )

    plan.addForeignKeyRelationship(
      postgresAcc, List("account_number"),
      List(jsonTask -> List("account_number"))
    )
    ```

=== "YAML"

    ```yaml
    ---
    #postgres task yaml
    name: "postgres_data"
    steps:
      - name: "accounts"
        type: "postgres"
        options:
          dbtable: "account.accounts"
        fields:
          - name: "account_number"
          - name: "name"
    ---
    #json task yaml
    name: "json_data"
    steps:
      - name: "transactions"
        type: "json"
        options:
          dbtable: "account.transactions"
        fields:
          - name: "account_id"
            options:
              sql: "CONCAT('ACC', account_number)"
          - name: "account_number"
            options:
              omit: true

    ---
    #plan yaml
    name: "customer_create_plan"
    description: "Create customers in JDBC"
    tasks:
      - name: "postgres_data"
        dataSourceName: "my_postgres"
      - name: "json_data"
        dataSourceName: "my_json"

    sinkOptions:
      foreignKeys:
        - source:
            dataSource: "my_postgres"
            step: "accounts"
            fields: ["account_number"]
          generate:
            - dataSource: "my_json"
              step: "transactions"
              fields: ["account_number"]
    ```

## Nested field

Your schema structure can have nested fields which can also be referenced as foreign keys. But to do so, you need to
create a proxy field that gets omitted from the final saved data.
  
In the example below, the nested `customer_details.name` field inside the `json` task needs to match with `name`
from `postgres`. A new field in the `json` called `_txn_name` is used as a temporary field to facilitate the foreign
key definition.

=== "Java"

    ```java
    var postgresAcc = postgres("my_postgres", "jdbc:...")
      .table("public.accounts")
      .fields(
        field().name("account_id"),
        field().name("name"),
        ...
      );
    var jsonTask = json("my_json", "/tmp/json")
      .fields(
        field().name("account_id"),
        field().name("customer_details")
          .fields(
            field().name("name").sql("_txn_name"), #nested field will get value from '_txn_name'
            ...
          ),
        field().name("_txn_name").omit(true)       #value will not be included in output
      );
    
    plan().addForeignKeyRelationship(
      postgresAcc, List.of("account_id", "name"),
      List.of(Map.entry(jsonTask, List.of("account_id", "_txn_name")))
    );
    ```

=== "Scala"

    ```scala
    val postgresAcc = postgres("my_postgres", "jdbc:...")
      .table("public.accounts")
      .fields(
        field.name("account_id"),
        field.name("name"),
        ...
      )
    var jsonTask = json("my_json", "/tmp/json")
      .fields(
        field.name("account_id"),
        field.name("customer_details")
          .fields(
            field.name("name").sql("_txn_name"), #nested field will get value from '_txn_name'
            ...
          ), 
        field.name("_txn_name").omit(true)       #value will not be included in output
      )

    plan.addForeignKeyRelationship(
      postgresAcc, List("account_id", "name"),
      List(jsonTask -> List("account_id", "_txn_name"))
    )
    ```

=== "YAML"

    ```yaml
    ---
    #postgres task yaml
    name: "postgres_data"
    steps:
      - name: "accounts"
        type: "postgres"
        options:
          dbtable: "account.accounts"
        fields:
          - name: "account_id"
          - name: "name"
    ---
    #json task yaml
    name: "json_data"
    steps:
      - name: "transactions"
        type: "json"
        options:
          dbtable: "account.transactions"
        fields:
          - name: "account_id"
          - name: "_txn_name"
            options:
              omit: true
          - name: "cusotmer_details"
            fields:
              name: "name"
              options:
                sql: "_txn_name"

    ---
    #plan yaml
    name: "customer_create_plan"
    description: "Create customers in JDBC"
    tasks:
      - name: "postgres_data"
        dataSourceName: "my_postgres"
      - name: "json_data"
        dataSourceName: "my_json"

    sinkOptions:
      foreignKeys:
        - source:
            dataSource: "my_postgres"
            step: "accounts"
            fields: ["account_id", "name"]
          generate:
            - dataSource: "my_json"
              step: "transactions"
              fields: ["account_id", "_txn_name"]
    ```

## Ordering

When defining relationships/foreign keys, the order matters. The source of the foreign key is generated first, then the children 
foreign keys are generated. This is to ensure that the source data is available for the children to reference.

When using the HTTP data sources, it gives you the opportunity to define the order in which the requests are executed.
For example, you want the following order:

- Create a pet with `id`
- Get pet with `id`
- Delete pet with `id`

Below is how you can define the order of the HTTP data sources.

=== "Java"

    ```java
    var httpTask = http("my_http")
            .fields(metadataSource().openApi("/opt/app/mount/http/petstore.json"))
            .count(count().records(2));

    var myPlan = plan().addForeignKeyRelationship(
            foreignField("my_http", "POST/pets", "body.id"),
            foreignField("my_http", "GET/pets/{id}", "pathParamid"),
            foreignField("my_http", "DELETE/pets/{id}", "pathParamid")
    );
    ```

=== "Scala"

    ```scala
    val httpTask = http("my_http")
      .fields(metadataSource.openApi("/opt/app/mount/http/petstore.json"))
      .count(count.records(2))

    val myPlan = plan.addForeignKeyRelationship(
      foreignField("my_http", "POST/pets", "body.id"),
      foreignField("my_http", "GET/pets/{id}", "pathParamid"),
      foreignField("my_http", "DELETE/pets/{id}", "pathParamid"),
    )
    ```

=== "YAML"

    In `docker/data/custom/task/http/openapi-task.yaml`:
    ```yaml
    name: "http_task"
    steps:
      - name: "my_petstore"
        options:
          metadataSourceType: "openapi"
          schemaLocation: "/opt/app/mount/http/petstore.json"
    ```

    In `docker/data/custom/plan/my-http.yaml`:
    ```yaml
    name: "my_http_plan"
    description: "Create pet data via HTTP from OpenAPI metadata"
    tasks:
      - name: "http_task"
        dataSourceName: "my_http"

    sinkOptions:
      foreignKeys:
        - source:
            dataSource: "my_http"
            step: "POST/pets"
            fields: ["body.id"]
          generate:
            - dataSource: "my_http"
              step: "GET/pets/{id}"
              fields: ["pathParamid"]
            - dataSource: "my_http"
              step: "DELETE/pets/{id}"
              fields: ["pathParamid"]
    ```
