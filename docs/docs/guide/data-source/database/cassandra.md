---
title: "Cassandra Test Data Management"
description: "Example of Cassandra data generation and testing tool that can automatically discover, generate and validate."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Cassandra

Creating a data generator for Cassandra. You will build a Docker image that will be able to populate data in Cassandra
for the tables you configure.

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/scala/io/github/datacatering/plan/CassandraPlanRun.scala) | [:material-coffee: Java Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/java/io/github/datacatering/plan/CassandraJavaPlanRun.java) | [:simple-yaml: Basic YAML](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/cassandra) | [:simple-yaml: Advanced YAML](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/cassandra/advanced-cassandra-task.yaml)

## Requirements

- 10 minutes
- Git
- Gradle
- Docker
- Cassandra

## Get Started

First, we will clone the data-caterer repo which will already have the base project setup required.

=== "Java"

    ```shell
    git clone git@github.com:data-catering/data-caterer.git
    cd data-caterer/example
    ```

=== "Scala"

    ```shell
    git clone git@github.com:data-catering/data-caterer.git
    cd data-caterer/example
    ```

=== "YAML"

    ```shell
    git clone git@github.com:data-catering/data-caterer.git
    cd data-caterer/example
    ```

=== "UI"

    [Run Data Caterer UI via the 'Quick Start' found here.](../../../../get-started/quick-start.md)

If you already have a Cassandra instance running, you can skip to [this step](#plan-setup).

### Cassandra Setup

Next, let's make sure you have an instance of Cassandra up and running in your local environment. This will make it
easy for us to iterate and check our changes.

```shell
cd docker
docker-compose up -d cassandra
```

#### Permissions

Let's make a new user that has the required permissions needed to push data into the Cassandra tables we want.

???+ tip "CQL Permission Statements"

    ```sql
    GRANT INSERT ON <schema>.<table> TO data_caterer_user;
    ```

Following permissions are required when enabling `configuration.enableGeneratePlanAndTasks(true)` as it will gather
metadata information about tables and fields from the below tables.

???+ tip "CQL Permission Statements"

    ```sql
    GRANT SELECT ON system_schema.tables TO data_caterer_user;
    GRANT SELECT ON system_schema.columns TO data_caterer_user;
    ```

### Plan Setup

Create a new Java or Scala class.

- Java: `src/main/java/io/github/datacatering/plan/MyAdvancedCassandraJavaPlan.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyAdvancedCassandraPlan.scala`

Make sure your class extends `PlanRun`.

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    
    public class MyAdvancedCassandraJavaPlan extends PlanRun {
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    
    class MyAdvancedCassandraPlan extends PlanRun {
    }
    ```

=== "YAML"

    In `docker/data/custom/unified/my-cassandra.yaml`:
    ```yaml
    name: "my_cassandra_plan"
    description: "Create account data via Cassandra"

    dataSources:
      - name: "my_cassandra"
        connection:
          type: "cassandra"
          options:
            url: "localhost:9042"
            user: "cassandra"
            password: "cassandra"
        steps:
          - name: "accounts"
    ```

=== "UI"

    1. Click on `Connection` towards the top of the screen
    1. For connection name, set to `my_cassandra`
    1. Click on `Select data source type..` and select `Cassandra`
    1. Set URL as `localhost:9042`
    1. Set username as `cassandra`
    1. Set password as `cassandra`
        1. Optionally, we could set a keyspace and table name but if you have more than keyspace or table, you would have to create new connection for each
    1. Click on `Create`
    1. You should see your connection `my_cassandra` show under `Existing connections`
    1. Click on `Home` towards the top of the screen
    1. Set plan name to `my_cassandra_plan`
    1. Set task name to `cassandra_task`
    1. Click on `Select data source..` and select `my_cassandra`

This class defines where we need to define all of our configurations for generating data. There are helper variables and
methods defined to make it simple and easy to use.

#### Connection Configuration

Within our class, we can start by defining the connection properties to connect to Cassandra.

=== "Java"

    ```java
    var accountTask = cassandra(
        "customer_cassandra",   //name
        "localhost:9042",       //url
        "cassandra",            //username
        "cassandra",            //password
        Map.of()                //optional additional connection options
    )
    ```
    
    Additional options such as SSL configuration, etc can be found [**here**](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md).

=== "Scala"

    ```scala
    val accountTask = cassandra(
        "customer_cassandra",   //name
        "localhost:9042",       //url
        "cassandra",            //username
        "cassandra",            //password
        Map()                   //optional additional connection options
    )
    ```
    
    Additional options such as SSL configuration, etc can be found [**here**](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md).

=== "YAML"

    In a unified YAML file:
    ```yaml
    dataSources:
      - name: "my_cassandra"
        connection:
          type: "cassandra"
          options:
            url: "localhost:9042"
            user: "cassandra"
            password: "cassandra"
    ```

=== "UI"

    1. We have already created the connection details in [this step](#plan-setup)

#### Schema

Let's create a task for inserting data into the `account.accounts` and `account.account_status_history` tables as
defined under`docker/data/cql/customer.cql`. This table should already be setup for you if you followed this
[step](#cassandra-setup). We can check if the table is set up already via the following command:

```shell
docker exec docker-cassandraserver-1 cqlsh -e 'describe account.accounts; describe account.account_status_history;'
```

Here we should see some output that looks like the below. This tells us what schema we need to follow when generating
data. We need to define that alongside any metadata that is useful to add constraints on what are possible values the
generated data should contain.

```shell
CREATE TABLE account.accounts (
    account_id text PRIMARY KEY,
    amount double,
    created_by text,
    name text,
    open_time timestamp,
    status text
)...
  
CREATE TABLE account.account_status_history (
    account_id text,
    eod_date date,
    status text,
    updated_by text,
    updated_time timestamp,
    PRIMARY KEY (account_id, eod_date)
)...
```

Trimming the connection details to work with the docker-compose Cassandra, we have a base Cassandra connection to define
the table and schema required. Let's define each field along with their corresponding data type. You will notice that
the `text` fields do not have a data type defined. This is because the default data type is `StringType` which
corresponds to `text` in Cassandra.

=== "Java"

    ```java
    {
        var accountTask = cassandra("customer_cassandra", "host.docker.internal:9042")
                .table("account", "accounts")
                .fields(
                        field().name("account_id"),
                        field().name("amount").type(DoubleType.instance()),
                        field().name("created_by"),
                        field().name("name"),
                        field().name("open_time").type(TimestampType.instance()),
                        field().name("status")
                );
    }
    ```

=== "Scala"

    ```scala
    val accountTask = cassandra("customer_cassandra", "host.docker.internal:9042")
      .table("account", "accounts")
      .fields(
        field.name("account_id"),
        field.name("amount").`type`(DoubleType),
        field.name("created_by"),
        field.name("name"),
        field.name("open_time").`type`(TimestampType),
        field.name("status")
      )
    ```

=== "YAML"

    In a unified YAML file:
    ```yaml
    dataSources:
      - name: "my_cassandra"
        connection:
          type: "cassandra"
          options:
            url: "host.docker.internal:9042"
            user: "cassandra"
            password: "cassandra"
        steps:
          - name: "accounts"
            options:
              keyspace: "account"
              table: "accounts"
            fields:
              - name: "account_number"
              - name: "amount"
                type: "double"
              - name: "created_by"
              - name: "open_time"
                type: "timestamp"
              - name: "status"
    ```

=== "UI"

    1. Click on `Generation` and tick the `Manual` checkbox
    1. Click on `+ Field`
    1. Add name as `account_number`
    1. Click on `Select data type` and select `string`
    1. Click on `+ Field` and add name as `amount`
    1. Click on `Select data type` and select `double`
    1. Click on `+ Field` and add name as `created_by`
    1. Click on `Select data type` and select `string`
    1. Click on `+ Field` and add name as `open_time`
    1. Click on `Select data type` and select `timestamp`
    1. Click on `+ Field` and add name as `status`
    1. Click on `Select data type` and select `string`

Depending on how you want to define the schema, follow the below:

- [Manual schema guide](../../scenario/data-generation.md#schema)
- Automatically detect schema from the data source, you can simply enable `configuration.enableGeneratePlanAndTasks(true)`
- [Automatically detect schema from a metadata source](../../index.md#metadata)

#### Additional Configurations

At the end of data generation, a report gets generated that summarises the actions it performed. We can control the
output folder of that report via configurations. We will also enable the unique check to ensure any unique fields will
have unique values generated.

=== "Java"

    ```java
    var config = configuration()
            .generatedReportsFolderPath("/opt/app/data/report")
            .enableUniqueCheck(true);
    ```

=== "Scala"

    ```scala
    val config = configuration
      .generatedReportsFolderPath("/opt/app/data/report")
      .enableUniqueCheck(true)
    ```

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    flags {
      enableUniqueCheck = true
    }
    folders {
      generatedReportsFolderPath = "/opt/app/data/report"
    }
    ```

=== "UI"

    1. Click on `Advanced Configuration` towards the bottom of the screen
    1. Click on `Flag` and click on `Unique Check`
    1. Click on `Folder` and enter `/tmp/data-caterer/report` for `Generated Reports Folder Path`

#### Execute

To tell Data Caterer that we want to run with the configurations along with the `accountTask`, we have to call `execute`
. So our full plan run will look like this.


=== "Java"

    ```java
    public class MyAdvancedCassandraJavaPlan extends PlanRun {
        {
            var accountTask = cassandra("customer_cassandra", "host.docker.internal:9042")
                    .table("account", "accounts")
                    .fields(
                            field().name("account_id").regex("ACC[0-9]{8}").primaryKey(true),
                            field().name("amount").type(DoubleType.instance()).min(1).max(1000),
                            field().name("created_by").sql("CASE WHEN status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
                            field().name("name").expression("#{Name.name}"),
                            field().name("open_time").type(TimestampType.instance()).min(java.sql.Date.valueOf("2022-01-01")),
                            field().name("status").oneOf("open", "closed", "suspended", "pending")
                    );
    
            var config = configuration()
                    .generatedReportsFolderPath("/opt/app/data/report")
                    .enableUniqueCheck(true);
            
            execute(config, accountTask);
        }
    }
    ```

=== "Scala"

    ```scala
    class MyAdvancedCassandraPlan extends PlanRun {
      val accountTask = cassandra("customer_cassandra", "host.docker.internal:9042")
        .table("account", "accounts")
        .fields(
          field.name("account_id").primaryKey(true).primaryKeyPosition(1),
          field.name("amount").`type`(DoubleType).min(1).max(1000),
          field.name("created_by").sql("CASE WHEN status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
          field.name("name").expression("#{Name.name}"),
          field.name("open_time").`type`(TimestampType).min(java.sql.Date.valueOf("2022-01-01")).clusteringPosition(1),
          field.name("status").oneOf("open", "closed", "suspended", "pending")
        )
    
      val config = configuration
        .generatedReportsFolderPath("/opt/app/data/report")
        .enableUniqueCheck(true)
      
      execute(config, accountTask)
    }
    ```

=== "YAML"

    No additional steps for YAML.

=== "UI"

    You can save your plan via the `Save` button at the top.

### Run

Now we can run via the script `./run.sh` that is in the top level directory of the `data-caterer/example` folder to run the class we just
created.

=== "Java"

    ```shell
    ./run.sh MyAdvancedCassandraJavaPlan
    docker exec docker-cassandraserver-1 cqlsh -e 'select count(1) from account.accounts;select * from account.accounts limit 10;'
    ```

=== "Scala"

    ```shell
    ./run.sh MyAdvancedCassandraPlan
    docker exec docker-cassandraserver-1 cqlsh -e 'select count(1) from account.accounts;select * from account.accounts limit 10;'
    ```

=== "YAML"

    ```shell
    ./run.sh my-cassandra.yaml
    docker exec docker-cassandraserver-1 cqlsh -e 'select count(1) from account.accounts;select * from account.accounts limit 10;'
    ```

=== "UI"

    1. Click the button `Execute` at the top
    1. Progress updates will show in the bottom right corner
    1. Click on `History` at the top
    1. Check for your plan name and see the result summary
    1. Click on `Report` on the right side to see more details of what was executed


Your output should look like this.

```shell
 count
-------
  1000

(1 rows)

Warnings :
Aggregation query used without partition key


 account_id  | amount    | created_by         | name                   | open_time                       | status
-------------+-----------+--------------------+------------------------+---------------------------------+-----------
 ACC13554145 | 917.00418 | zb CVvbBTTzitjo5fK |          Jan Sanford I | 2023-06-21 21:50:10.463000+0000 | suspended
 ACC19154140 |  46.99177 |             VH88H9 |       Clyde Bailey PhD | 2023-07-18 11:33:03.675000+0000 |      open
 ACC50587836 |  774.9872 |         GENANwPm t |           Sang Monahan | 2023-03-21 00:16:53.308000+0000 |    closed
 ACC67619387 | 452.86706 |       5msTpcBLStTH |         Jewell Gerlach | 2022-10-18 19:13:07.606000+0000 | suspended
 ACC69889784 |  14.69298 |           WDmOh7NT |          Dale Schulist | 2022-10-25 12:10:52.239000+0000 | suspended
 ACC41977254 |  51.26492 |          J8jAKzvj2 |           Norma Nienow | 2023-08-19 18:54:39.195000+0000 | suspended
 ACC40932912 | 349.68067 |   SLcJgKZdLp5ALMyg | Vincenzo Considine III | 2023-05-16 00:22:45.991000+0000 |    closed
 ACC20642011 | 658.40713 |          clyZRD4fI |  Lannie McLaughlin DDS | 2023-05-11 23:14:30.249000+0000 |      open
 ACC74962085 | 970.98218 |       ZLETTSnj4NpD |          Ima Jerde DVM | 2023-05-07 10:01:56.218000+0000 |   pending
 ACC72848439 | 481.64267 |                 cc |        Kyla Deckow DDS | 2023-08-16 13:28:23.362000+0000 | suspended

(10 rows)
```

Also check the HTML report, found at `docker/sample/report/index.html`, that gets generated to get an overview of what
was executed.

![Sample report](../../../../sample/report/report_screenshot.png)

### Validation

If you want to validate data from Cassandra,
[follow the validation documentation found here to help guide you](../../../validation.md).
