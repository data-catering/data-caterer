---
title: "JSON Test Data Management"
description: "Example of JSON test data management tool that can automatically discover, generate and validate."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# JSON

Creating a data generator for JSON. You will have the ability to generate and validate JSON files via Docker.

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer-example/blob/main/src/main/scala/io/github/datacatering/plan/JsonPlan.scala) | [:material-coffee: Java Example](https://github.com/data-catering/data-caterer-example/blob/main/src/main/java/io/github/datacatering/plan/JsonJavaPlan.java) | [:material-file-yaml-outline: Basic YAML](https://github.com/data-catering/data-caterer-example/blob/main/docker/data/custom/task/file/json) | [:material-file-yaml-outline: Advanced YAML](https://github.com/data-catering/data-caterer-example/blob/main/docker/data/custom/task/file/json/advanced-json-task.yaml)

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

### Plan Setup

Create a file depending on which interface you want to use.

- Java: `src/main/java/io/github/datacatering/plan/MyJSONJavaPlan.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyJSONPlan.scala`
- YAML: `docker/data/custom/plan/my-json.yaml`

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    
    public class MyJSONJavaPlan extends PlanRun {
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    
    class MyJSONPlan extends PlanRun {
    }
    ```

=== "YAML"

    In `docker/data/custom/plan/my-json.yaml`:
    ```yaml
    name: "my_json_plan"
    description: "Create account data in JSON"
    tasks:
      - name: "json_task"
        dataSourceName: "my_json"
    ```

=== "UI"

    1. Click on `Connection` towards the top of the screen
    1. For connection name, set to `my_json`
    1. Click on `Select data source type..` and select `JSON`
    1. Set `Path` as `/tmp/custom/json/accounts`
        1. Optionally, we could set the number of partitions and columns to partition by
    1. Click on `Create`
    1. You should see your connection `my_json` show under `Existing connections`
    1. Click on `Home` towards the top of the screen
    1. Set plan name to `my_json_plan`
    1. Set task name to `json_task`
    1. Click on `Select data source..` and select `my_json`

This class defines where we need to define all of our configurations for generating data. There are helper variables and
methods defined to make it simple and easy to use.

#### Connection Configuration

Within our class, we can start by defining the connection properties to read/write from/to JSON.

=== "Java"

    ```java
    var accountTask = json(
        "customer_accounts",                    //name
        "/opt/app/data/customer/account_json",  //path
        Map.of()                                //additional options
    );
    ```
    
    Additional options can be found [**here**](https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option).

=== "Scala"

    ```scala
    val accountTask = json(
      "customer_accounts",                    //name         
      "/opt/app/data/customer/account_json",  //path
      Map()                                   //additional options
    )
    ```
    
    Additional options can be found [**here**](https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option).

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    json {
        my_json {
            "dateFormat": "dd-MM-yyyy"
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

Let's create a task for generating data as `accounts` and then generate data for `transactions`, which will be related 
to the accounts generated.

=== "Java"

    ```java
    var accountTask = json("customer_accounts", "/opt/app/data/customer/account_json")
            .fields(
                field().name("account_id"),
                field().name("balance").type(new DecimalType(5, 2)),
                field().name("created_by"),
                field().name("open_time").type(TimestampType.instance()),
                field().name("status"),
                field().name("customer_details")
                    .fields(
                        field().name("name"),
                        field().name("age").type(IntegerType.instance()),
                        field().name("city")
                    )
            );
    ```

=== "Scala"

    ```scala
    val accountTask = json("customer_accounts", "/opt/app/data/customer/account_json")
      .fields(
        field.name("account_id"),
        field.name("balance").`type`(new DecimalType(5, 2)),
        field.name("created_by"),
        field.name("open_time").`type`(TimestampType),
        field.name("status"),
        field.name("customer_details")
          .fields(
            field.name("name"),
            field.name("age").`type`(IntegerType),
            field.name("city")
          )
      )
    ```

=== "YAML"

    In `docker/data/custom/task/json/json-task.yaml`:
    ```yaml
    name: "json_task"
    steps:
      - name: "accounts"
        type: "json"
        options:
          path: "/opt/app/data/customer/account_json"
        fields:
          - name: "account_id"
          - name: "balance"
            type: "double"
          - name: "created_by"
          - name: "open_time"
            type: "timestamp"
          - name: "status"
          - name: "customer_details"
            type: "struct"
            fields:
              - name: "name"
              - name: "age"
                type: "integer"
              - name: "city"
    ```

=== "UI"

    1. Click on `Generation` and tick the `Manual` checkbox
    1. Click on `+ Field`
    1. Add name as `account_id`
    1. Click on `Select data type` and select `string`
    1. Click on `+ Field` and add name as `balance`
    1. Click on `Select data type` and select `double`
    1. Click on `+ Field` and add name as `created_by`
    1. Click on `Select data type` and select `string`
    1. Click on `+ Field` and add name as `open_time`
    1. Click on `Select data type` and select `timestamp`
    1. Click on `+ Field` and add name as `status`
    1. Click on `Select data type` and select `string`
    1. Click on `+ Field` and add name as `customer_details`
    1. Click on `Select data type` and select `struct`
        1. Under `customer_details`, click on `+ Field` and add name `name`
        1. Under `customer_details`, click on `+ Field` and add name `age`, set data type as `integer`
        1. Under `customer_details`, click on `+ Field` and add name `city`

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

#### JSON output options

If your JSON sink should output a bare top-level array when there is a single top-level array field, enable unwrap.

[:material-run-fast: Example](https://github.com/data-catering/data-caterer-example/blob/main/src/main/scala/io/github/datacatering/plan/FastGenerationAndReferencePlanRun.scala)

=== "Java"

    ```java
    json("json_array", "/opt/app/data/json-array")
      .fields(
        field().name("items").type(ArrayType.instance()).unwrapTopLevelArray(true)
          .fields(
            field().name("id"),
            field().name("score").type(DoubleType.instance()).min(0).max(100)
          )
      );
    ```

=== "Scala"

    ```scala
    json("json_array", "/opt/app/data/json-array")
      .fields(
        field.name("items").`type`(ArrayType).unwrapTopLevelArray(true)
          .fields(
            field.name("id"),
            field.name("score").`type`(DoubleType).min(0).max(100)
          )
      )
    ```

=== "YAML"

    ```yaml
    name: "json_array_task"
    steps:
      - name: "json_array"
        type: "json"
        options:
          path: "/opt/app/data/json-array"
        fields:
          - name: "items"
            type: "array"
            options:
              unwrapTopLevelArray: true
            fields:
              - name: "id"
              - name: "score"
                type: "double"
                options:
                  min: 0
                  max: 100
    ```

=== "UI"

    1. Click on `+ Field`
    1. Add name as `items`
    1. Click on `Select data type` and select `array`
    1. Click on `+ Field` and add name as `unwrapTopLevelArray`
    1. Click on `Select data type` and select `boolean`
    1. Set value to `true`

### Run

Now we can run via the script `./run.sh` that is in the top level directory of the `data-caterer-example` to run the class we just
created.

=== "Java"

    ```shell
    ./run.sh MyJSONJavaPlan
    account=$(head -1 docker/sample/customer/account_json/part-00000-* | sed -nr 's/.*account_id":"(.+)","balance.*/\1/p')
    echo "Head account record:"
    head -1 docker/sample/customer/account_json/part-00000-*
    echo $account
    echo "Transaction records:"
    cat docker/sample/customer/transaction_json/part-0000* | grep $account
    ```

=== "Scala"

    ```shell
    ./run.sh MyJSONPlan
    account=$(head -1 docker/sample/customer/account_json/part-00000-* | sed -nr 's/.*account_id":"(.+)","balance.*/\1/p')
    echo "Head account record:"
    head -1 docker/sample/customer/account_json/part-00000-*
    echo $account
    echo "Transaction records:"
    cat docker/sample/customer/transaction_json/part-0000* | grep $account
    ```

=== "YAML"

    ```shell
    ./run.sh my-json.yaml
    account=$(head -1 docker/sample/customer/account_json/part-00000-* | sed -nr 's/.*account_id":"(.+)","balance.*/\1/p')
    echo "Head account record:"
    head -1 docker/sample/customer/account_json/part-00000-*
    echo $account
    echo "Transaction records:"
    cat docker/sample/customer/transaction_json/part-0000* | grep $account
    ```

=== "UI"

    1. Click the button `Execute` at the top
    1. Progress updates will show in the bottom right corner
    1. Click on `History` at the top
    1. Check for your plan name and see the result summary
    1. Click on `Report` on the right side to see more details of what was executed


It should look something like this.

```shell
Head account record:
{"account_id":"ACC00047541","balance":445.62,"created_by":"event","open_time":"2024-03-13T00:31:38.836Z","status":"suspended","customer_details":{"name":"Joey Gaylord","age":44,"city":"Lake Jose"}}
ACC00047541
Transaction records:
{"account_id":"ACC00047541","full_name":"Joey Gaylord","amount":31.485424217447527,"time":"2023-11-07T04:50:20.875Z","date":"2023-11-07"}
{"account_id":"ACC00047541","full_name":"Joey Gaylord","amount":79.22177964401857,"time":"2024-02-01T15:15:38.289Z","date":"2024-02-01"}
{"account_id":"ACC00047541","full_name":"Joey Gaylord","amount":56.06230355456882,"time":"2024-02-29T21:42:42.473Z","date":"2024-02-29"}
```

Congratulations! You have now made a data generator that has simulated a real world data scenario. You can check the
`JsonJavaPlan.java` or `JsonPlan.scala` files as well to check that your plan is the same.

### Validation

If you want to validate data from a JSON source, 
[follow the validation documentation found here to help guide you](../../../validation.md).
