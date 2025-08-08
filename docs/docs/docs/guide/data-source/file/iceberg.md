---
title: "Iceberg Test Data Management"
description: "Example of Iceberg test data management tool that can automatically discover, generate and validate."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Iceberg

Data testing for Iceberg. You will have the ability to generate and validate Iceberg tables.

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

    [Run Data Caterer UI via the 'Quick Start' found here.](../../../../get-started/quick-start.md)

### Plan Setup

Create a file depending on which interface you want to use.

- Java: `src/main/java/io/github/datacatering/plan/MyIcebergJavaPlan.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyIcebergPlan.scala`
- YAML: `docker/data/customer/plan/my-iceberg.yaml`

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    
    public class MyIcebergJavaPlan extends PlanRun {
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    
    class MyIcebergPlan extends PlanRun {
    }
    ```

=== "YAML"

    In `docker/data/custom/plan/my-iceberg.yaml`:
    ```yaml
    name: "my_iceberg_plan"
    description: "Create account data in Iceberg table"
    tasks:
      - name: "iceberg_task"
        dataSourceName: "my_iceberg"
    ```

=== "UI"

    1. Go to `Connection` tab in the top bar
    2. Select data source as `Iceberg`
        1. Enter in data source name `my_iceberg`
        2. Select catalog type `hadoop`
        3. Enter warehouse path as `/opt/app/data/customer/iceberg`

This class defines where we need to define all of our configurations for generating data. There are helper variables and
methods defined to make it simple and easy to use.

#### Connection Configuration

Within our class, we can start by defining the connection properties to read/write from/to Iceberg.

=== "Java"

    ```java
    var accountTask = iceberg(
            "customer_accounts",              //name
            "account.accounts",               //table name
            "/opt/app/data/customer/iceberg", //warehouse path
            "hadoop",                         //catalog type
            "",                               //catalog uri
            Map.of()                          //additional options
    );
    ```
    
    Additional options can be found [**here**](https://iceberg.apache.org/docs/1.5.0/spark-configuration/#catalog-configuration).

=== "Scala"

    ```scala
    val accountTask = iceberg(
      "customer_accounts",              //name
      "account.accounts",               //table name
      "/opt/app/data/customer/iceberg", //warehouse path
      "hadoop",                         //catalog type
      "",                               //catalog uri
      Map()                             //additional options
    )
    ```
    
    Additional options can be found [**here**](https://iceberg.apache.org/docs/1.5.0/spark-configuration/#catalog-configuration).

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    iceberg {
      my_iceberg {
        path = "/opt/app/data/customer/iceberg"
        path = ${?ICEBERG_WAREHOUSE_PATH}
        catalogType = "hadoop"
        catalogType = ${?ICEBERG_CATALOG_TYPE}
        catalogUri = ""
        catalogUri = ${?ICEBERG_CATALOG_URI}
      }
    }
    ```

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
    2. Click on `Flag` and click on `Unique Check`
    3. Click on `Folder` and enter `/tmp/data-caterer/report` for `Generated Reports Folder Path`

### Run

Now we can run via the script `./run.sh` that is in the top level directory of the `data-caterer-example` to run the class we just
created.

=== "Java"

    ```shell
    ./run.sh MyIcebergJavaPlan
    ```

=== "Scala"

    ```shell
    ./run.sh MyIcebergPlan
    ```

=== "YAML"

    ```shell
    ./run.sh my-iceberg.yaml
    ```

=== "UI"

    1. Click the button `Execute` at the top
    1. Progress updates will show in the bottom right corner
    1. Click on `History` at the top
    1. Check for your plan name and see the result summary
    1. Click on `Report` on the right side to see more details of what was executed

Congratulations! You have now made a data generator that has simulated a real world data scenario. You can check the
`IcebergJavaPlan.java` or `IcebergPlan.scala` files as well to check that your plan is the same.

### Validation

If you want to validate data from an Iceberg source, 
[follow the validation documentation found here to help guide you](../../../validation.md).
