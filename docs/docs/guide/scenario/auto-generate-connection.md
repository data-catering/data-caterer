---
title: "Automated data generation and validation tool"
description: "Example of automatically discovering, generating and validating for Postgres."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Auto Generate From Data Connection

Creating a data generator based on only a data connection to Postgres.

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

- Java: `src/main/java/io/github/datacatering/plan/MyAdvancedAutomatedJavaPlanRun.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyAdvancedAutomatedPlanRun.scala`

Make sure your class extends `PlanRun`.

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    ...
    
    public class MyAdvancedAutomatedJavaPlanRun extends PlanRun {
        {
            var autoRun = configuration()
                    .postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")  (1)
                    .enableGeneratePlanAndTasks(true)                                                 (2)
                    .generatedPlanAndTaskFolderPath("/opt/app/data/generated")                        (3)
                    .enableUniqueCheck(true)                                                          (4)
                    .generatedReportsFolderPath("/opt/app/data/report");
    
            execute(autoRun);
        }
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    ...
    
    class MyAdvancedAutomatedPlanRun extends PlanRun {

      val autoRun = configuration
        .postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")  (1)
        .enableGeneratePlanAndTasks(true)                                                 (2)
        .generatedPlanAndTaskFolderPath("/opt/app/data/generated")                        (3)
        .enableUniqueCheck(true)                                                          (4)
        .generatedReportsFolderPath("/opt/app/data/report")
    
      execute(configuration = autoRun)
    }
    ```

=== "YAML"

    Create file `docker/data/custom/application.conf`:
    ```yaml
    flags {
      enableGeneratePlanAndTasks = true      # (2)
      enableUniqueCheck = true               # (4)
    }
    folders {
      generatedPlanAndTaskFolderPath = "/opt/app/data/generated"    # (3)
      generatedReportsFolderPath = "/opt/app/data/report"
    }
    connectionConfigurations {
      postgres {
        my_postgres {                        # (1)
          url = "jdbc:postgresql://host.docker.internal:5432/customer"
          user = "postgres"
          password = "password"
        }
      }
    }
    ```

In the above code, we note the following:

1. Data source configuration to a Postgres data source called `my_postgres`
2. We have enabled the flag `enableGeneratePlanAndTasks` which tells Data Caterer to go to `my_postgres` and generate
   data for all the tables found under the database `customer` (which is defined in the connection string).
3. The config `generatedPlanAndTaskFolderPath` defines where the metadata that is gathered from `my_postgres` should be
   saved at so that we could re-use it later.
4. `enableUniqueCheck` is set to true to ensure that generated data is unique based on primary key or foreign key
   definitions.

!!! note

    Unique check will only ensure generated data is unique. Any existing data in your data source is not taken into 
    account, so generated data may fail to insert depending on the data source restrictions

### Postgres Setup

If you don't have your own Postgres up and running, you can set up and run an instance configured in the `docker`
folder via.

```shell
cd docker
docker-compose up -d postgres
docker exec docker-postgresserver-1 psql -Upostgres -d customer -c '\dt+ account.*'
```

This will create the tables found under `docker/data/sql/postgres/customer.sql`. You can change this file to contain
your own tables. We can see there are 4 tables created for us, `accounts, balances, transactions and mapping`.

### Run

Let's try run.

```shell
cd ..
./run.sh
#input class MyAdvancedAutomatedJavaPlanRun or MyAdvancedAutomatedPlanRun
#after completing
docker exec docker-postgresserver-1 psql -Upostgres -d customer -c 'select * from account.accounts limit 1;'
```

It should look something like this.

```shell
   id   | account_number  | account_status | created_by | created_by_fixed_length | customer_id_int | customer_id_smallint | customer_id_bigint |   customer_id_decimal    | customer_id_real | customer_id_double | open_date  |     open_timestamp      | last_opened_time |                                                           payload_bytes
--------+-----------------+----------------+------------+-------------------------+-----------------+----------------------+--------------------+--------------------------+------------------+--------------------+------------+-------------------------+------------------+------------------------------------------------------------------------------------------------------------------------------------
 100414 | 5uROOVOUyQUbubN | h3H            | SfA0eZJcTm | CuRw                    |              13 |                   42 |               6041 | 76987.745612542900000000 |         91866.78 |  66400.37433202339 | 2023-03-05 | 2023-08-14 11:33:11.343 | 23:58:01.736     | \x604d315d4547616e6a233050415373317274736f5e682d516132524f3d23233c37463463322f342d34376d597e665d6b3d395b4238284028622b7d6d2b4f5042
(1 row)
```

The data that gets inserted will follow the foreign keys that are defined within Postgres and also ensure the insertion
order is correct.

Also check the HTML report that gets generated under `docker/sample/report/index.html`. You can see a summary of what
was generated along with other metadata.

You can now look to play around with other tables or data sources and auto generate for them.

### Additional Topics

#### Learn From Existing Data

If you have any existing data within your data source, Data Caterer will gather metadata about the existing data to
help guide it when generating new data. There are configurations that can help tune the metadata analysis found
[**here**](../../configuration.md#metadata).

#### Filter Out Schema/Tables

As part of your connection definition, you can define any schemas and/or tables your don't want to generate data for. In
the example below, it will not generate any data for any tables under the `history` and `audit` schemas. Also, any
table with the name `balances` or `transactions` in any schema will also not have data generated.

=== "Java"

    ```java
    var autoRun = configuration()
            .postgres(
                  "my_postgres", 
                  "jdbc:postgresql://host.docker.internal:5432/customer",
                  Map.of(
                      "filterOutSchema", "history, audit",
                      "filterOutTable", "balances, transactions")
                  )
            )
    ```

=== "Scala"

    ```scala
    val autoRun = configuration
      .postgres(
        "my_postgres",
        "jdbc:postgresql://host.docker.internal:5432/customer",
        Map(
          "filterOutSchema" -> "history, audit",
          "filterOutTable" -> "balances, transactions")
        )
      )
    ```

#### Define record count

You can control the record count per sub data source via `numRecordsPerStep`.

=== "Java"

      ```java
      var autoRun = configuration()
            ...
            .numRecordsPerStep(100)
      
      execute(autoRun)
      ```

=== "Scala"

      ```scala
      val autoRun = configuration
        ...
        .numRecordsPerStep(100)
         
      execute(configuration = autoRun)
      ```

