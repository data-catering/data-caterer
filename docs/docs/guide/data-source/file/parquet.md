---
title: "Parquet Test Data Management"
description: "Example of Parquet test data management tool that can automatically discover, generate and validate."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Parquet

Creating a data generator for Parquet. You will have the ability to generate and validate Parquet files via Docker.

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/scala/io/github/datacatering/plan/ParquetPlan.scala) | [:material-coffee: Java Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/java/io/github/datacatering/plan/ParquetJavaPlan.java) | [:material-file-yaml-outline: YAML Example](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/file/parquet)

## Requirements

- 10 minutes
- Git
- Gradle
- Docker

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

### Plan Setup

Create a file depending on which interface you want to use.

- Java: `src/main/java/io/github/datacatering/plan/MyParquetJavaPlan.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyParquetPlan.scala`
- YAML: `docker/data/custom/plan/my-parquet.yaml`

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    
    public class MyParquetJavaPlan extends PlanRun {
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    
    class MyParquetPlan extends PlanRun {
    }
    ```

=== "YAML"

    In `docker/data/custom/plan/my-parquet.yaml`:
    ```yaml
    name: "my_parquet_plan"
    description: "Create account data in Parquet format"
    tasks:
      - name: "parquet_task"
        dataSourceName: "my_parquet"
    ```

=== "UI"

    1. Click on `Connection` towards the top of the screen
    1. For connection name, set to `my_parquet`
    1. Click on `Select data source type..` and select `Parquet`
    1. Set `Path` as `/tmp/custom/parquet/accounts`
        1. Optionally, we could set the number of partitions and columns to partition by
    1. Click on `Create`
    1. You should see your connection `my_parquet` show under `Existing connections`
    1. Click on `Home` towards the top of the screen
    1. Set plan name to `my_parquet_plan`
    1. Set task name to `parquet_task`
    1. Click on `Select data source..` and select `my_parquet`

This class defines where we need to define all of our configurations for generating data. There are helper variables and
methods defined to make it simple and easy to use.

#### Connection Configuration

Within our class, we can start by defining the connection properties to read/write from/to Parquet.

=== "Java"

    ```java
    var accountTask = parquet(
        "customer_accounts",                      //name
        "/opt/app/data/customer/account_parquet", //path
        Map.of()                                  //additional options
    );
    ```
    
    Additional options can be found [**here**](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option).

=== "Scala"

    ```scala
    val accountTask = parquet(
      "customer_accounts",                      //name         
      "/opt/app/data/customer/account_parquet", //path
      Map()                                     //additional options
    )
    ```
    
    Additional options can be found [**here**](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option).

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    parquet {
        my_parquet {
            "spark.sql.parquet.mergeSchema": "true"
        }
    }
    ```

    Additional options can be found [**here**](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option).

=== "UI"

    1. We have already created the connection details in [this step](#plan-setup)

#### Schema

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

    execute(myPlan, config, accountTask, transactionTask);
    ```

=== "Scala"

    ```scala
    val config = configuration
      .generatedReportsFolderPath("/opt/app/data/report")
      .enableUniqueCheck(true)

    execute(myPlan, config, accountTask, transactionTask)
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

### Run

Now we can run via the script `./run.sh` that is in the top level directory of the `data-caterer/example` folder to run the class we just
created.

=== "Java"

    ```shell
    ./run.sh MyParquetJavaPlan
    ```

=== "Scala"

    ```shell
    ./run.sh MyParquetPlan
    ```

=== "YAML"

    ```shell
    ./run.sh my-parquet.yaml
    ```

=== "UI"

    1. Click the button `Execute` at the top
    1. Progress updates will show in the bottom right corner
    1. Click on `History` at the top
    1. Check for your plan name and see the result summary
    1. Click on `Report` on the right side to see more details of what was executed

Congratulations! You have now made a data generator that has simulated a real world data scenario. You can check the
`ParquetJavaPlan.java` or `ParquetPlan.scala` files as well to check that your plan is the same.

### Validation

If you want to validate data from a Parquet source, 
[follow the validation documentation found here to help guide you](../../../validation.md).
