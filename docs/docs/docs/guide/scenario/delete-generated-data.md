---
title: "Delete Generated Data"
description: "Example of deleting generated data that is used for testing. Help clean up data in shared environments."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Delete Generated Data

Creating a data generator for Postgres and delete the generated data after using it.

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

- Java: `src/main/java/io/github/datacatering/plan/MyAdvancedDeleteJavaPlanRun.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyAdvancedDeletePlanRun.scala`

Make sure your class extends `PlanRun`.

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    ...
    
    public class MyAdvancedDeleteJavaPlanRun extends PlanRun {
        {
            var autoRun = configuration()
                    .postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")  (1)
                    .enableGeneratePlanAndTasks(true)                                                 (2)
                    .enableRecordTracking(true)                                                       (3)
                    .enableDeleteGeneratedRecords(false)                                              (4)
                    .enableUniqueCheck(true)
                    .generatedPlanAndTaskFolderPath("/opt/app/data/generated")                        (5)
                    .recordTrackingFolderPath("/opt/app/data/recordTracking")                         (6)
                    .generatedReportsFolderPath("/opt/app/data/report");
   
            execute(autoRun);
       }
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    ...
    
    class MyAdvancedDeletePlanRun extends PlanRun {

      val autoRun = configuration
        .postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")  (1)
        .enableGeneratePlanAndTasks(true)                                                 (2)
        .enableRecordTracking(true)                                                       (3)
        .enableDeleteGeneratedRecords(false)                                              (4)
        .enableUniqueCheck(true)
        .generatedPlanAndTaskFolderPath("/opt/app/data/generated")                        (5)
        .recordTrackingFolderPath("/opt/app/data/recordTracking")                         (6)
        .generatedReportsFolderPath("/opt/app/data/report")
      
      execute(configuration = autoRun)
    }
    ```

In the above code we note the following:

1. We have defined a Postgres connection called `my_postgres`
2. `enableGeneratePlanAndTasks` is enabled to auto generate data for all tables under `customer` database
3. `enableRecordTracking` is enabled to ensure that all generated records are tracked. This will get used when we want
   to delete data afterwards
4. `enableDeleteGeneratedRecords` is disabled for now. We want to see the generated data first and delete sometime after
5. `generatedPlanAndTaskFolderPath` is the folder path where we saved the metadata we have gathered from `my_postgres`
6. `recordTrackingFolderPath` is the folder path where record tracking is maintained. We need to persist this data to
   ensure it is still available when we want to delete data

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
#input class MyAdvancedDeleteJavaPlanRun or MyAdvancedDeletePlanRun
#after completing
docker exec docker-postgresserver-1 psql -Upostgres -d customer -c 'select * from account.accounts limit 1'
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

Check the number of records via:

```shell
docker exec docker-postgresserver-1 psql -Upostgres -d customer -c 'select count(1) from account.accounts'
#open report under docker/sample/report/index.html
```

### Delete

We are now at a stage where we want to delete the data that was generated. All we need to do is flip two flags.

```java
.enableDeleteGeneratedRecords(true)
.enableGenerateData(false)  //we need to explicitly disable generating data
```

Enable delete generated records and disable generating data. 

Before we run again, let us insert a record manually to see if that data will survive after running the job to delete
the generated data.

```shell
docker exec docker-postgresserver-1 psql -Upostgres -d customer -c "insert into account.accounts (account_number) values ('my_account_number')"
docker exec docker-postgresserver-1 psql -Upostgres -d customer -c "select count(1) from account.accounts"
```

We now should have 1001 records in our `account.accounts` table. Let's delete the generated data now.

```shell
./run.sh
#input class MyAdvancedDeleteJavaPlanRun or MyAdvancedDeletePlanRun
#after completing
docker exec docker-postgresserver-1 psql -Upostgres -d customer -c 'select * from account.accounts limit 1'
docker exec docker-postgresserver-1 psql -Upostgres -d customer -c 'select count(1) from account.accounts'
```

You should see that only 1 record is left, the one that we manually inserted. Great, now we can generate data reliably 
and also be able to clean it up.

### Additional Topics

#### One class for generating, another for deleting?

Yes, this is possible. There are two requirements:
- the connection names used need to be the same across both classes
- `recordTrackingFolderPath` needs to be set to the same value

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
