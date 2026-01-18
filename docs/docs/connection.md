---
title: "Data source connections"
description: "Data Caterer can connect to databases (Postgres, MySQL), file (Parquet, ORC) and messaging systems (Kafka, Solace), HTTP APIs and metadata services."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Data Source Connections

Details of all the connection configuration supported can be found in the below subsections for each type of connection.

These configurations can be done via API or from configuration. Examples of both are shown for each data source below.

## Supported Data Connections

| Data Source Type | Data Source                        | Support                                   |
|------------------|------------------------------------|-------------------------------------------|
| Cloud Storage    | AWS S3                             | :white_check_mark:                        |
| Cloud Storage    | Azure Blob Storage                 | :white_check_mark:                        |
| Cloud Storage    | GCP Cloud Storage                  | :white_check_mark:                        |
| Database         | BigQuery                           | :white_check_mark:                        |
| Database         | Cassandra                          | :white_check_mark:                        |
| Database         | MySQL                              | :white_check_mark:                        |
| Database         | Postgres                           | :white_check_mark:                        |
| Database         | Elasticsearch                      | :octicons-x-circle-fill-12:{ .red-cross } |
| Database         | MongoDB                            | :octicons-x-circle-fill-12:{ .red-cross } |
| Database         | Opensearch                         | :octicons-x-circle-fill-12:{ .red-cross } |
| File             | CSV                                | :white_check_mark:                        |
| File             | Delta Lake                         | :white_check_mark:                        |
| File             | Iceberg                            | :white_check_mark:                        |
| File             | JSON                               | :white_check_mark:                        |
| File             | ORC                                | :white_check_mark:                        |
| File             | Parquet                            | :white_check_mark:                        |
| File             | Hudi                               | :octicons-x-circle-fill-12:{ .red-cross } |
| HTTP             | REST API                           | :white_check_mark:                        |
| Messaging        | Kafka                              | :white_check_mark:                        |
| Messaging        | RabbitMQ                           | :white_check_mark:                        |
| Messaging        | Solace                             | :white_check_mark:                        |
| Messaging        | ActiveMQ                           | :octicons-x-circle-fill-12:{ .red-cross } |
| Messaging        | Pulsar                             | :octicons-x-circle-fill-12:{ .red-cross } |
| Metadata         | Data Contract CLI                  | :white_check_mark:                        |
| Metadata         | Great Expectations                 | :white_check_mark:                        |
| Metadata         | JSON Schema                        | :white_check_mark:                        |
| Metadata         | Marquez                            | :white_check_mark:                        |
| Metadata         | OpenMetadata                       | :white_check_mark:                        |
| Metadata         | OpenAPI/Swagger                    | :white_check_mark:                        |
| Metadata         | Open Data Contract Standard (ODCS) | :white_check_mark:                        |
| Metadata         | Amundsen                           | :octicons-x-circle-fill-12:{ .red-cross } |
| Metadata         | Datahub                            | :octicons-x-circle-fill-12:{ .red-cross } |
| Metadata         | Solace Event Portal                | :octicons-x-circle-fill-12:{ .red-cross } |

### API

All connection details require a name. Depending on the data source, you can define additional options which may be used
by the driver or connector for connecting to the data source.

### Configuration file

All connection details follow the same pattern.

```
<connection format> {
    <connection name> {
        <key> = <value>
    }
}
```

!!! info "Overriding configuration"

    When defining a configuration value that can be defined by a system property or environment variable at runtime, you can
    define that via the following:

    ```
    url = "localhost"
    url = ${?POSTGRES_URL}
    ```

    The above defines that if there is a system property or environment variable named `POSTGRES_URL`, then that value will
    be used for the `url`, otherwise, it will default to `localhost`.

!!! tip "Connection Configuration Options"

    You have two options for configuring connections:

    **Option 1: Unified YAML File (Recommended)**

    Define connections inline within your unified YAML file:
    ```yaml
    name: "account_plan"

    dataSources:
      - name: "my_json_sink"
        connection:
          type: "json"
          options:
            path: "/data/output/accounts"
            saveMode: "overwrite"
        steps:
          - name: "account_step"
            count:
              records: 1000
            fields:
              - name: "account_id"
                type: "string"
    ```

    **Option 2: External application.conf**

    For advanced use cases or environment-specific configurations, you can define connections in `application.conf`:
    ```
    json {
      my_json_sink {
        path = "/data/output/accounts"
        saveMode = "overwrite"
      }
    }
    ```

    Then reference the connection in your unified YAML:
    ```yaml
    dataSources:
      - name: "my_json_sink"
        # Connection details loaded from application.conf
        steps:
          - name: "account_step"
            count:
              records: 1000
            options:
              saveMode: "append"  # Step options override connection defaults
            fields:
              - name: "account_id"
                type: "string"
    ```

    **Key Points:**
    - Inline connections in unified YAML are the simplest approach
    - Step-specific options take precedence over connection configuration
    - Use `application.conf` when you need environment variable substitution or complex configuration

## Data sources

To find examples of a task for each type of data source, please check out [this page](guide/index.md).

### File

Linked [**here**](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html) is a list of generic options
that can be included as part of your file data source configuration if required. Links to specific file type
configurations can be found below.

#### CSV

=== "Java"

    ```java
    csv("customer_transactions", "/data/customer/transaction")
    ```

=== "Scala"

    ```scala
    csv("customer_transactions", "/data/customer/transaction")
    ```

=== "YAML"

    In a unified YAML file:
    ```yaml
    dataSources:
      - name: "customer_transactions"
        connection:
          type: "csv"
          options:
            path: "/data/customer/transaction"
    ```

    Or in `application.conf`:
    ```
    csv {
      customer_transactions {
        path = "/data/customer/transaction"
        path = ${?CSV_PATH}
      }
    }
    ```

[Other available configuration for CSV can be found here](https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option)

#### JSON

=== "Java"

    ```java
    json("customer_transactions", "/data/customer/transaction")
    ```

=== "Scala"

    ```scala
    json("customer_transactions", "/data/customer/transaction")
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_transactions"
        connection:
          type: "json"
          options:
            path: "/data/customer/transaction"
    ```

[Other available configuration for JSON can be found here](https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option)

#### ORC

=== "Java"

    ```java
    orc("customer_transactions", "/data/customer/transaction")
    ```

=== "Scala"

    ```scala
    orc("customer_transactions", "/data/customer/transaction")
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_transactions"
        connection:
          type: "orc"
          options:
            path: "/data/customer/transaction"
    ```

[Other available configuration for ORC can be found here](https://spark.apache.org/docs/latest/sql-data-sources-orc.html#configuration)

#### Parquet

=== "Java"

    ```java
    parquet("customer_transactions", "/data/customer/transaction")
    ```

=== "Scala"

    ```scala
    parquet("customer_transactions", "/data/customer/transaction")
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_transactions"
        connection:
          type: "parquet"
          options:
            path: "/data/customer/transaction"
    ```

[Other available configuration for Parquet can be found here](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option)

#### Delta

=== "Java"

    ```java
    delta("customer_transactions", "/data/customer/transaction")
    ```

=== "Scala"

    ```scala
    delta("customer_transactions", "/data/customer/transaction")
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_transactions"
        connection:
          type: "delta"
          options:
            path: "/data/customer/transaction"
    ```

#### Iceberg

=== "Java"

    ```java
    iceberg(
      "customer_accounts",              //name
      "account.accounts",               //table name
      "/opt/app/data/customer/iceberg", //warehouse path
      "hadoop",                         //catalog type
      "",                               //catalogUri
      Map.of()                          //additional options
    );
    ```

=== "Scala"

    ```scala
    iceberg(
      "customer_accounts",              //name
      "account.accounts",               //table name
      "/opt/app/data/customer/iceberg", //warehouse path
      "hadoop",                         //catalog type
      "",                               //catalogUri
      Map()                             //additional options
    )
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_accounts"
        connection:
          type: "iceberg"
          options:
            path: "/opt/app/data/customer/iceberg"
            catalogType: "hadoop"
        steps:
          - name: "accounts"
            options:
              table: "account.accounts"
    ```

### RMDBS

Follows the same configuration used by Spark as
found [**here**](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option).  
Sample can be found below

=== "Java"

    ```java
    postgres(
        "customer_postgres",                            //name
        "jdbc:postgresql://localhost:5432/customer",    //url
        "postgres",                                     //username
        "postgres"                                      //password
    )
    ```

=== "Scala"

    ```scala
    postgres(
        "customer_postgres",                            //name
        "jdbc:postgresql://localhost:5432/customer",    //url
        "postgres",                                     //username
        "postgres"                                      //password
    )
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_postgres"
        connection:
          type: "postgres"
          options:
            url: "jdbc:postgresql://localhost:5432/customer"
            user: "postgres"
            password: "postgres"
    ```

Ensure that the user has write permission, so it is able to save the table to the target tables.

??? tip "SQL Permission Statements"

    ```sql
    GRANT INSERT ON <schema>.<table> TO <user>;
    ```

#### Postgres

Can see example API or Config definition for Postgres connection above.

##### Permissions

Following permissions are required when generating plan and tasks:

??? tip "SQL Permission Statements"

    ```sql
    GRANT SELECT ON information_schema.tables TO < user >;
    GRANT SELECT ON information_schema.columns TO < user >;
    GRANT SELECT ON information_schema.key_column_usage TO < user >;
    GRANT SELECT ON information_schema.table_constraints TO < user >;
    GRANT SELECT ON information_schema.constraint_column_usage TO < user >;
    ```

#### MySQL

=== "Java"

    ```java
    mysql(
        "customer_mysql",                       //name
        "jdbc:mysql://localhost:3306/customer", //url
        "root",                                 //username
        "root"                                  //password
    )
    ```

=== "Scala"

    ```scala
    mysql(
        "customer_mysql",                       //name
        "jdbc:mysql://localhost:3306/customer", //url
        "root",                                 //username
        "root"                                  //password
    )
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_mysql"
        connection:
          type: "mysql"
          options:
            url: "jdbc:mysql://localhost:3306/customer"
            user: "root"
            password: "root"
    ```

##### Permissions

Following permissions are required when generating plan and tasks:

??? tip "SQL Permission Statements"

    ```sql
    GRANT SELECT ON information_schema.columns TO < user >;
    GRANT SELECT ON information_schema.statistics TO < user >;
    GRANT SELECT ON information_schema.key_column_usage TO < user >;
    ```

### BigQuery

Follows same configuration as defined by the Spark BigQuery Connector as found 
[**here**](https://github.com/GoogleCloudDataproc/spark-bigquery-connector?tab=readme-ov-file#properties).

=== "Java"

    ```java
    bigquery(
        "customer_bigquery",   //name
        "gs://my-test-bucket", //temporaryGcsBucket
        Map.of()               //optional additional connection options
    )
    ```

=== "Scala"

    ```scala
    bigquery(
      "customer_bigquery",   //name
      "gs://my-test-bucket", //temporaryGcsBucket
      Map()                  //optional additional connection options
    )
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_bigquery"
        connection:
          type: "bigquery"
          options:
            temporaryGcsBucket: "gs://my-test-bucket"
    ```

### Cassandra

Follows same configuration as defined by the Spark Cassandra Connector as
found [**here**](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md)

=== "Java"

    ```java
    cassandra(
        "customer_cassandra",   //name
        "localhost:9042",       //url
        "cassandra",            //username
        "cassandra",            //password
        Map.of()                //optional additional connection options
    )
    ```

=== "Scala"

    ```scala
    cassandra(
        "customer_cassandra",   //name
        "localhost:9042",       //url
        "cassandra",            //username
        "cassandra",            //password
        Map()                   //optional additional connection options
    )
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_cassandra"
        connection:
          type: "cassandra"
          options:
            url: "localhost:9042"
            user: "cassandra"
            password: "cassandra"
    ```

##### Permissions

Ensure that the user has write permission, so it is able to save the table to the target tables.

??? tip "CQL Permission Statements"

    ```sql
    GRANT INSERT ON <schema>.<table> TO <user>;
    ```

Following permissions are required when enabling `configuration.enableGeneratePlanAndTasks(true)` as it will gather
metadata information about tables and fields from the below tables.

??? tip "CQL Permission Statements"

    ```sql
    GRANT SELECT ON system_schema.tables TO <user>;
    GRANT SELECT ON system_schema.columns TO <user>;
    ```

### Kafka

Define your Kafka bootstrap server to connect and send generated data to corresponding topics. Topic gets set at a step
level.  
Further details can be
found [**here**](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#writing-data-to-kafka)

=== "Java"

    ```java
    kafka(
        "customer_kafka",   //name
        "localhost:9092"    //url
    )
    ```

=== "Scala"

    ```scala
    kafka(
        "customer_kafka",   //name
        "localhost:9092"    //url
    )
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_kafka"
        connection:
          type: "kafka"
          options:
            url: "localhost:9092"
    ```

When defining your schema for pushing data to Kafka, it follows a specific top level schema.  
An example can be
found [**here**](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/kafka/kafka-account-task.yaml)
. You can define the key, value, headers, partition or topic by following the linked schema.

### JMS

Uses JNDI lookup to send messages to JMS queue. Ensure that the messaging system you are using has your queue/topic
registered
via JNDI otherwise a connection cannot be created.

#### Rabbitmq

=== "Java"

    ```java
    rabbitmq(
        "customer_rabbitmq",                            //name
        "amqp://localhost:5672",                        //url
        "guest",                                        //username
        "guest",                                        //password
        "/",                                            //virtual host
        "com.rabbitmq.jms.admin.RMQConnectionFactory",  //connection factory
    )
    ```

=== "Scala"

    ```scala
    rabbitmq(
      "customer_rabbitmq",                            //name
      "amqp://localhost:5672",                        //url
      "guest",                                        //username
      "guest",                                        //password
      "/",                                            //virtual host
      "com.rabbitmq.jms.admin.RMQConnectionFactory",  //connection factory
    )
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_rabbitmq"
        connection:
          type: "rabbitmq"
          options:
            url: "amqp://localhost:5672"
            user: "guest"
            password: "guest"
            virtualHost: "/"
    ```

#### Solace

=== "Java"

    ```java
    solace(
        "customer_solace",                                      //name
        "smf://localhost:55554",                                //url
        "admin",                                                //username
        "admin",                                                //password
        "default",                                              //vpn name
        "/jms/cf/default",                                      //connection factory
        "com.solacesystems.jndi.SolJNDIInitialContextFactory"   //initial context factory
    )
    ```

=== "Scala"

    ```scala
    solace(
        "customer_solace",                                      //name
        "smf://localhost:55554",                                //url
        "admin",                                                //username
        "admin",                                                //password
        "default",                                              //vpn name
        "/jms/cf/default",                                      //connection factory
        "com.solacesystems.jndi.SolJNDIInitialContextFactory"   //initial context factory
    )
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_solace"
        connection:
          type: "solace"
          options:
            url: "smf://localhost:55555"
            user: "admin"
            password: "admin"
            vpnName: "default"
            connectionFactory: "/jms/cf/default"
    ```

### HTTP

Define any username and/or password needed for the HTTP requests.  
The url is defined in the tasks to allow for generated data to be populated in the url.

=== "Java"

    ```java
    http(
        "customer_api", //name
        "admin",        //username
        "admin"         //password
    )
    ```

=== "Scala"

    ```scala
    http(
        "customer_api", //name
        "admin",        //username
        "admin"         //password
    )
    ```

=== "YAML"

    ```yaml
    dataSources:
      - name: "customer_api"
        connection:
          type: "http"
          options:
            user: "admin"
            password: "admin"
    ```
