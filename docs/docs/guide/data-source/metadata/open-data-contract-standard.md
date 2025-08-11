---
title: "Using Open Data Contract Standard (ODCS) with Data Caterer"
description: "Example of using Open Data Contract Standard (ODCS) for data generation and testing."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Open Data Contract Standard (ODCS) Source

![Data Caterer reading from ODCS file for schema metadata and data quality](../../../../diagrams/data-source/high_level_flow-run-config-basic-flow-odcs-support.svg)

Creating a data generator for a CSV file based on metadata stored
in [Open Data Contract Standard (ODCS)](https://github.com/bitol-io/open-data-contract-standard).

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer-example/blob/main/src/main/scala/io/github/datacatering/plan/ODCSPlanRun.scala)

## Requirements

- 10 minutes
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

### Open Data Contract Standard (ODCS) Setup

We will be using
[the following ODCS file](https://github.com/data-catering/data-caterer-example/blob/main/docker/mount/odcs/full-example.odcs.yaml)
for this example.

### Plan Setup

Create a new Java/Scala class or YAML file.

- Java: `src/main/java/io/github/datacatering/plan/MyAdvancedODCSJavaPlanRun.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyAdvancedODCSPlanRun.scala`
- YAML: `docker/data/customer/plan/my-odcs.yaml`

Make sure your class extends `PlanRun`.

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    ...
    
    public class MyAdvancedODCSJavaPlanRun extends PlanRun {
        {
            var conf = configuration().enableGeneratePlanAndTasks(true)
                .generatedReportsFolderPath("/opt/app/data/report");
        }
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    ...
    
    class MyAdvancedODCSPlanRun extends PlanRun {
      val conf = configuration.enableGeneratePlanAndTasks(true)
        .generatedReportsFolderPath("/opt/app/data/report")
    }
    ```

=== "YAML"

    In `docker/data/custom/plan/my-odcs.yaml`:
    ```yaml
    name: "my_odcs_plan"
    description: "Create account data in CSV via ODCS metadata"
    tasks:
      - name: "csv_account_file"
        dataSourceName: "customer_accounts"
    ```

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

We will enable generate plan and tasks so that we can read from external sources for metadata and save the reports
under a folder we can easily access.

#### Schema

We can point the schema of a data source to our Open Data Contract Standard (ODCS) file.

=== "Java"

    ```java
    var accountTask = csv("my_csv", "/opt/app/data/account-odcs", Map.of("header", "true"))
            .fields(metadataSource().openDataContractStandard("/opt/app/mount/odcs/full-example.yaml"))
            .count(count().records(100));
    ```

=== "Scala"

    ```scala
    val accountTask = csv("customer_accounts", "/opt/app/data/customer/account-odcs", Map("header" -> "true"))
      .fields(metadataSource.openDataContractStandard("/opt/app/mount/odcs/full-example.yaml"))
      .count(count.records(100))
    ```

=== "YAML"

    In `docker/data/custom/task/file/csv/csv-odcs-account-task.yaml`:
    ```yaml
    name: "csv_account_file"
    steps:
      - name: "accounts"
        type: "csv"
        options:
          path: "/opt/app/data/csv/account-odcs"
          metadataSourceType: "openDataContractStandard"
          dataContractFile: "/opt/app/mount/odcs/full-example.yaml"
        count:
          records: 100
    ```

=== "UI"

    1. Click on `Connection` tab at the top
    1. Select `ODCS` as the data source and enter `example-odcs`
    1. Copy [this file](https://github.com/data-catering/data-caterer-example/blob/main/docker/mount/odcs/full-example.yaml) into `/tmp/odcs/full-example.yaml`
    1. Enter `/tmp/odcs/full-example.yaml` as the `Contract File`

The above defines that the schema will come from Open Data Contract Standard (ODCS), which is a type of metadata source
that contains information about schemas.
[Specifically, it points to the schema provided here](https://github.com/data-catering/data-caterer-example/blob/main/docker/mount/odcs/full-example.yaml#L42)
in the `docker/mount/odcs` folder of data-caterer-example repo.

### Run

Let's try run and see what happens.

=== "Java"

    ```shell
    ./run.sh MyAdvancedODCSJavaPlanRun
    head docker/sample/customer/account-odcs/part-00000-*
    ```

=== "Scala"

    ```shell
    ./run.sh MyAdvancedODCSPlanRun
    head docker/sample/customer/account-odcs/part-00000-*
    ```

=== "YAML"

    ```shell
    ./run.sh my-odcs.yaml
    head docker/sample/customer/account-odcs/part-00000-*
    ```

=== "UI"

    1. Click on `Execute` at the top
    ```shell
    head /tmp/data-caterer/customer/account-odcs/part-00000*
    ```

It should look something like this.

```
txn_ref_dt,rcvr_id,rcvr_cntry_code
2023-07-11,PB0Wo dMx,nWlbRGIinpJfP
2024-05-01,5GtkNkHfwuxLKdM,1a
2024-05-01,OxuATCLAUIhHzr,gSxn2ct
2024-05-22,P4qe,y9htWZhyjW
```

Looks like we have some data now. But we can do better and add some enhancements to it.

### Custom metadata

We can see from the data generated, that it isn't quite what we want. Sometimes, the metadata is not sufficient for us
to produce production-like data yet, and we want to manually edit it. Let's try to add some enhancements to it.

Let's make the `rcvr_id` field follow the regex `RC[0-9]{8}` and the field `rcvr_cntry_code` should only be one of
either `AU, US or TW`. For the full guide on data generation options,
[check the following page](../../scenario/data-generation.md).

=== "Java"

    ```java
    var accountTask = csv("my_csv", "/opt/app/data/account-odcs", Map.of("header", "true"))
                .fields(metadata...)
                .fields(
                    field().name("rcvr_id").regex("RC[0-9]{8}"),
                    field().name("rcvr_cntry_code").oneOf("AU", "US", "TW")
                )
                .count(count().records(100));
    ```

=== "Scala"

    ```scala
    val accountTask = csv("customer_accounts", "/opt/app/data/customer/account-odcs", Map("header" -> "true"))
      .fields(metadata...)
      .fields(
        field.name("rcvr_id").regex("RC[0-9]{8}"),
        field.name("rcvr_cntry_code").oneOf("AU", "US", "TW")
      )
      .count(count.records(100))
    ```

=== "YAML"

    In `docker/data/custom/task/file/csv/csv-odcs-account-task.yaml`:
    ```yaml
    name: "csv_account_file"
    steps:
      - name: "accounts"
        type: "csv"
        options:
          path: "/opt/app/data/csv/account-odcs"
          metadataSourceType: "openDataContractStandard"
          dataContractFile: "/opt/app/mount/odcs/full-example.yaml"
        count:
          records: 100
        fields:
          - name: "rcvr_id"
            options:
              regex: "RC[0-9]{8}"
          - name: "rcvr_cntry_code"
            options:
              oneOf:
                - "AU"
                - "US"
                - "TW"
    ```

=== "UI"

    1. Click on `Generation` and tick the `Manual` checkbox
    1. Click on `+ Field`
        1. Go to `rcvr_id` field
        1. Click on `+` dropdown next to `string` data type
        1. Click `Regex` and enter `RC[0-9]{8}`
    1. Click on `+ Field`
        1. Go to `rcvr_cntry_code` field
        1. Click on `+` dropdown next to `string` data type
        1. Click `One Of` and enter `AU,US,TW`

Let's test it out by running it again

=== "Java"

    ```shell
    ./run.sh MyAdvancedODCSJavaPlanRun
    head docker/sample/customer/account-odcs/part-00000-*
    ```

=== "Scala"

    ```shell
    ./run.sh MyAdvancedODCSPlanRun
    head docker/sample/customer/account-odcs/part-00000-*
    ```

=== "YAML"

    ```shell
    ./run.sh my-odcs.yaml
    head docker/sample/customer/account-odcs/part-00000-*
    ```

=== "UI"

    1. Click on `Execute` at the top
    ```shell
    head /tmp/data-caterer/customer/account-odcs/part-00000*
    ```

```
txn_ref_dt,rcvr_id,rcvr_cntry_code
2024-02-15,RC02579393,US
2023-08-18,RC14320425,AU
2023-07-07,RC17915355,TW
2024-06-07,RC47347046,TW
```

Great! Now we have the ability to get schema information from an external source, add our own metadata and generate
data.

### Data validation

[To find out what data validation options are available, check this link.](../../../validation.md)

Another aspect of Open Data Contract Standard (ODCS) that can be leveraged is the definition of data quality rules.
Once the latest version of ODCS is released (version 3.x), there should be a vendor neutral definition of data quality
rules that Data Caterer can use. Once available, it will be as easy as enabling data validations
via `enableGenerateValidations` in `configuration`.

=== "Java"

    ```java
    var conf = configuration().enableGeneratePlanAndTasks(true)
        .enableGenerateValidations(true)
        .generatedReportsFolderPath("/opt/app/data/report");

    execute(conf, accountTask);
    ```

=== "Scala"

    ```scala
    val conf = configuration.enableGeneratePlanAndTasks(true)
      .enableGenerateValidations(true)
      .generatedReportsFolderPath("/opt/app/data/report")
    
    execute(conf, accountTask)
    ```

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    flags {
      enableGenerateValidations = true
    }
    ```

=== "UI"

    1. Click on `Advanced Configuration` towards the bottom of the screen
    2. Click on `Flag` and click on `Generate Validations`

Check out the full example under `ODCSSourcePlanRun` in the example repo.
