---
title: "Using Data Contract CLI with Data Caterer"
description: "Example of using Data Contract CLI for data generation and testing."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Data Contract CLI Source

![Data Caterer reading from Data Contract CLI file for schema metadata and data quality](../../../../diagrams/data-source/high_level_flow-run-config-basic-flow-odcs-support.svg)

Creating a data generator for a CSV file based on metadata stored
in [Data Contract CLI](https://github.com/datacontract/datacontract-cli).

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

### Data Contract CLI Setup

We will be using
[the following Data Contract CLI file](https://github.com/data-catering/data-caterer/blob/main/example/docker/mount/datacontract-cli/datacontract.yaml)
for this example.

### Plan Setup

Create a file depending on which interface you want to use.

- Java: `src/main/java/io/github/datacatering/plan/MyAdvancedDataContractCliJavaPlanRun.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyAdvancedDataContractCliPlanRun.scala`
- YAML: `docker/data/customer/plan/my-datacontract-cli.yaml`

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    ...
    
    public class MyAdvancedDataContractCliJavaPlanRun extends PlanRun {
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
    
    class MyAdvancedDataContractCliPlanRun extends PlanRun {
      val conf = configuration.enableGeneratePlanAndTasks(true)
        .generatedReportsFolderPath("/opt/app/data/report")
    }
    ```

=== "YAML"

    In `docker/data/custom/plan/my-datacontract-cli.yaml`:
    ```yaml
    name: "my_datacontract_cli_plan"
    description: "Create account data in CSV via Data Contract CLI metadata"
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

We can point the schema of a data source to our Data Contract CLI file.

=== "Java"

    ```java
    var accountTask = csv("my_csv", "/opt/app/data/account-datacontract-cli", Map.of("header", "true"))
            .fields(metadataSource().dataContractCli("/opt/app/mount/datacontract-cli/datacontract.yaml"))
            .count(count().records(100));
    ```

=== "Scala"

    ```scala
    val accountTask = csv("customer_accounts", "/opt/app/data/customer/account-datacontract-cli", Map("header" -> "true"))
      .fields(metadataSource.dataContractCli("/opt/app/mount/datacontract-cli/datacontract.yaml"))
      .count(count.records(100))
    ```

=== "YAML"

    In `docker/data/custom/task/file/csv/csv-datacontract-cli-account-task.yaml`:
    ```yaml
    name: "csv_account_file"
    steps:
      - name: "accounts"
        type: "csv"
        options:
          path: "/opt/app/data/csv/account-datacontract-cli"
          metadataSourceType: "dataContractCli"
          dataContractFile: "/opt/app/mount/datacontract-cli/datacontract.yaml"
        count:
          records: 100
    ```

=== "UI"

    1. Click on `Connection` tab at the top
    1. Select `Data Contract CLI` as the data source and enter `example-datacontract-cli`
    1. Copy [this file](https://github.com/data-catering/data-caterer/blob/main/example/docker/mount/datacontract-cli/datacontract.yaml) into `/tmp/datacontract-cli/datacontract.yaml`
    1. Enter `/tmp/datacontract-cli/datacontract.yaml` as the `Contract File`

The above defines that the schema will come from Data Contract CLI, which is a type of metadata source
that contains information about schemas.
[Specifically, it points to the schema provided here](https://github.com/data-catering/data-caterer/blob/main/example/docker/mount/datacontract-cli/datacontract.yaml)
in the `docker/mount/datacontract-cli` folder of data-caterer repo.

### Run

Let's try run and see what happens.

=== "Java"

    ```shell
    ./run.sh MyAdvancedDataContractCliJavaPlanRun
    head docker/sample/customer/account-datacontract-cli/part-00000-*
    ```

=== "Scala"

    ```shell
    ./run.sh MyAdvancedDataContractCliPlanRun
    head docker/sample/customer/account-datacontract-cli/part-00000-*
    ```

=== "YAML"

    ```shell
    ./run.sh my-datacontract-cli.yaml
    head docker/sample/customer/account-datacontract-cli/part-00000-*
    ```

=== "UI"

    1. Click on `Execute` at the top
    ```shell
    head /tmp/data-caterer/customer/account-datacontract-cli/part-00000*
    ```

It should look something like this.

```
province_state,latitude,confirmed,fips,longitude,country_region,last_update,combined_key,admin2
fwFaFV F73BAIfFd,69977.84296117249,17533,ln9 CRbGkQ9IEyuW,793.3222856184141,87YVVqgS1podHa S,2024-02-10T10:25:39.176Z,sAnv74T9xOyA6MZI,06iRhvBBy40WBlVf
W9N6z1 s7CYyc4L3,54580.231575916325,96761,4mxWLbwArVKOhg6E,58977.422371028944,TkCABcFIYJf87okg,2024-09-07T17:45:27.641Z,9GDm6MGk3WfPdorc,TQdRvrCSgCXg ioP
dp2E6zXwoSKJ5 J2,13368.961196453121,18606,wGJ3iQNg5SdaN4ad,22482.40836235147,r4 Ka6J9ZNKQVEHK,2024-01-25T14:01:09.224Z,RYh6Kl5 46QvOZFR,eEad607OtQX15Vlw
sfQG0neaO5hS7PlV,17461.556283773938,40155,DeSwWCpYwa4WFx5F,81371.85361585379,F2 tzIJS9JsTlhuE,2024-06-13T08:44:55.555Z,JnnGplRjkjo6SgOX,8B5h7UuV2r965wD4
rAISjVikM0ScAsRX,65831.49716656232,36392,vKhuncOokeDgia7e,67677.50911541228,zZVJkymK09ef5oFC,2024-01-01T14:32:02.881Z,lLdHa4JExfuN2FXv,ebcPhXgYJMYTAla1
```

Looks like we have some data now. But we can do better and add some enhancements to it.

### Custom metadata

We can see from the data generated, that it isn't quite what we want. Sometimes, the metadata is not sufficient for us
to produce production-like data yet, and we want to manually edit it. Let's try to add some enhancements to it.

Let's make the `latitude` and `longitude` fields make sense. `latitude` is meant to be between -90 to 90 whilst 
`longitude` is between -180 to 180. `country_region` should also represent a state name. 
For the full guide on data generation options,
[check the following page](../../scenario/data-generation.md).

=== "Java"

    ```java
    var accountTask = csv("my_csv", "/opt/app/data/account-datacontract-cli", Map.of("header", "true"))
                .fields(metadata...)
                .fields(
                    field().name("latitude").min(-90).max(90),
                    field().name("longitude").min(-180).max(180),
                    field().name("country_region").expression("#{Address.state}")
                )
                .count(count().records(100));
    ```

=== "Scala"

    ```scala
    val accountTask = csv("customer_accounts", "/opt/app/data/customer/account-datacontract-cli", Map("header" -> "true"))
      .fields(metadata...)
      .fields(
        field.name("latitude").min(-90).max(90),
        field.name("longitude").min(-180).max(180),
        field.name("country_region").expression("#{Address.state}")
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
          path: "/opt/app/data/csv/account-datacontract-cli"
          metadataSourceType: "dataContractCli"
          dataContractFile: "/opt/app/mount/datacontract-cli/datacontract.yaml"
        count:
          records: 100
        fields:
          - name: "latitude"
            options:
              min: -90
              max: 90
          - name: "longitude"
            options:
              min: -180
              max: 180
          - name: "country_region"
            options:
              expression: "#{Address.state}"
    ```

=== "UI"

    1. Click on `Generation` and tick the `Manual` checkbox
    1. Click on `+ Field`
        1. Go to `latitude` field
        1. Select data type as `double`
        1. Click on `+` dropdown next to `double` data type
        1. Click `Min` and enter `-90`
        1. Click `Max` and enter `90`
    1. Click on `+ Field`
        1. Go to `longitude` field
        1. Select data type as `double`
        1. Click on `+` dropdown next to `double` data type
        1. Click `Min` and enter `-180`
        1. Click `Max` and enter `180`
    1. Click on `+ Field`
        1. Go to `country_region` field
        1. Click on `+` dropdown next to `string` data type
        1. Click `Faker Expression` and enter `#{Address.state}`

Let's test it out by running it again

=== "Java"

    ```shell
    ./run.sh MyAdvancedDataContractCliJavaPlanRun
    head docker/sample/customer/account-datacontract-cli/part-00000-*
    ```

=== "Scala"

    ```shell
    ./run.sh MyAdvancedDataContractCliPlanRun
    head docker/sample/customer/account-datacontract-cli/part-00000-*
    ```

=== "YAML"

    ```shell
    ./run.sh my-datacontract-cli.yaml
    head docker/sample/customer/account-datacontract-cli/part-00000-*
    ```

=== "UI"

    1. Click on `Execute` at the top
    ```shell
    head /tmp/data-caterer/customer/account-datacontract-cli/part-00000*
    ```

```
province_state,latitude,confirmed,fips,longitude,country_region,last_update,combined_key,admin2
HY5GstfIPnXT0em,35.73941132584518,63652,6YS4JJvZ8N9JsqT,27.037747952451554,Connecticut,2023-12-24T12:42:08.798Z,qIPco7WUo5jXA D,ODADv25VyKsf6Qn
vnkQrkwgf9oj xR,81.87829759208316,73064,cPgrOuPwBVnxK2b,-146.20644012308924,Illinois,2024-03-14T10:24:52.327Z,7NYzdyaM87VjlfH,KUpbi4msmXWZYS4
jnSwW Pk6zj1LsC,82.87970774482852,72341,rL5XqKZtM5unS9x,-153.1279291007243,Mississippi,2024-08-29T15:30:56.338Z,NouXv6EXlWY1Ihe,mirpEgTno0OEDH8
ZmNNb9C5g t8CgJ,43.58312642271184,73116,NFlRmB8p0egkFqG,179.56650534615852,Indiana,2024-01-22T17:05:51.968Z,Fkxf0l3CC a42o5,JznmesYH8ReGhg3
Uf5QH6luS4u5SnO,-75.64320251178277,6232,yRQLBU2OQvm5uqC,-31.025626492871083,New Jersey,2024-09-25T02:35:03.477Z,7IXVfeL6BEpkRbf,f7wUqnigV8WU4B
```

Great! Now we have the ability to get schema information from an external source, add our own metadata and generate
data.

### Data validation

[To find out what data validation options are available, check this link.](../../../validation.md)

Another aspect of Data Contract CLI that can be leveraged is the definition of data quality rules.
In a later version of Data Caterer, the data quality rules could be later imported and all run within Data Caterer. 
Once available, it will be as easy as enabling data validations via `enableGenerateValidations` in `configuration`.

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

Check out the full example under `DataContractCliSourcePlanRun` in the example repo.
