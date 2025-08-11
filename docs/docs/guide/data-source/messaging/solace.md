---
title: "Solace Test Data Management"
description: "Example of Solace for data generation and validation tool for queue/topic."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Solace

Creating a data generator for Solace. You will build a Docker image that will be able to populate data in Solace
for the queues/topics you configure.

![Generate Solace messages](../../../../diagrams/data-source/solace_generation_run.gif)

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

If you already have a Solace instance running, you can skip to [this step](#plan-setup).

### Solace Setup

Next, let's make sure you have an instance of Solace up and running in your local environment. This will make it
easy for us to iterate and check our changes.

```shell
cd docker
docker-compose up -d solace
```

Open up [localhost:8080](http://localhost:8080) and login with `admin:admin` and check there is the `default` VPN like
below. Notice there is 2 queues/topics created. If you do not see 2 created, try to run the script found under
`docker/data/solace/setup_solace.sh` and change the `host` to `localhost`.

![Solace dashboard](../../../../diagrams/data-source/solace_dashboard.png)

### Plan Setup

Create a file depending on which interface you want to use.

- Java: `src/main/java/io/github/datacatering/plan/MyAdvancedSolaceJavaPlan.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyAdvancedSolacePlan.scala`
- YAML: `docker/data/custom/plan/my-solace.yaml`

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    
    public class MyAdvancedSolaceJavaPlan extends PlanRun {
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    
    class MyAdvancedSolacePlan extends PlanRun {
    }
    ```

=== "YAML"

    In `docker/data/custom/plan/my-solace.yaml`:
    ```yaml
    name: "my_solace_plan"
    description: "Create account data in Solace"
    tasks:
      - name: "solace_task"
        dataSourceName: "my_solace"
    ```

=== "UI"

    1. Click on `Connection` towards the top of the screen
    1. For connection name, set to `my_solace`
    1. Click on `Select data source type..` and select `Solace`
    1. Set `URL` as `smf://host.docker.internal:55554`
        1. Optionally, we could set the JNDI destination (queue or topic) but we would have to create a new connection for each queue or topic
    1. Click on `Create`
    1. You should see your connection `my_solace` show under `Existing connections`
    1. Click on `Home` towards the top of the screen
    1. Set plan name to `my_solace_plan`
    1. Set task name to `solace_task`
    1. Click on `Select data source..` and select `my_solace`

This class defines where we need to define all of our configurations for generating data. There are helper variables and
methods defined to make it simple and easy to use.

#### Connection Configuration

Within our class, we can start by defining the connection properties to connect to Solace.

=== "Java"

    ```java
    var accountTask = solace(
        "my_solace",                        //name
        "smf://host.docker.internal:55554", //url
        Map.of()                            //optional additional connection options
    );
    ```
    
    Additional connection options can be found [**here**](../../../connection.md#jms).

=== "Scala"

    ```scala
    val accountTask = solace(
        "my_solace",                        //name
        "smf://host.docker.internal:55554", //url
        Map()                               //optional additional connection options
    )
    ```
    
    Additional connection options can be found [**here**](../../../connection.md#jms).

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    jms {
        solace {
            initialContextFactory = "com.solacesystems.jndi.SolJNDIInitialContextFactory"
            initialContextFactory = ${?SOLACE_INITIAL_CONTEXT_FACTORY}
            connectionFactory = "/jms/cf/default"
            connectionFactory = ${?SOLACE_CONNECTION_FACTORY}
            url = "smf://solaceserver:55555"
            url = ${?SOLACE_URL}
            user = "admin"
            user = ${?SOLACE_USER}
            password = "admin"
            password = ${?SOLACE_PASSWORD}
            vpnName = "default"
            vpnName = ${?SOLACE_VPN}
        }
    }
    ```

=== "UI"

    1. We have already created the connection details in [this step](#plan-setup)

#### Schema

Let's create a task for inserting data into the `rest_test_queue` or `rest_test_topic` that is already created for us
from this [step](#solace-setup).

Trimming the connection details to work with the docker-compose Solace, we have a base Solace connection to define
the JNDI destination we will publish to. Let's define each field along with their corresponding data type. You will
notice
that the `text` fields do not have a data type defined. This is because the default data type is `StringType`.

=== "Java"

    ```java
    {
        var solaceTask = solace("my_solace", "smf://host.docker.internal:55554")
                .destination("/JNDI/Q/rest_test_queue")
                .fields(
                        //field().name("partition").type(IntegerType.instance()),   can define JMS priority here
                        field().messageHeaders(   //set message properties via headers field
                                field().messageHeader("account-id", "body.account_id"),
                                field().messageHeader("updated", "body.details.updated_by-time")
                        )
                ).fields(
                        field().messageBody(
                                field().name("account_id").regex("ACC[0-9]{8}"),
                                field().name("year").type(IntegerType.instance()).min(2021).max(2023),
                                field().name("amount").type(DoubleType.instance()),
                                field().name("details")
                                        .fields(
                                                field().name("name").expression("#{Name.name}"),
                                                field().name("first_txn_date").type(DateType.instance()).sql("ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"),
                                                field().name("updated_by")
                                                        .fields(
                                                                field().name("user"),
                                                                field().name("time").type(TimestampType.instance())
                                                        )
                                        ),
                                field().name("transactions").type(ArrayType.instance())
                                        .fields(
                                                field().name("txn_date").type(DateType.instance()).min(Date.valueOf("2021-01-01")).max("2021-12-31"),
                                                field().name("amount").type(DoubleType.instance())
                                        )
                        )
                )
                .count(count().records(10));
    }
    ```

=== "Scala"

    ```scala
    val solaceTask = solace("my_solace", "smf://host.docker.internal:55554")
      .destination("/JNDI/Q/rest_test_queue")
      .fields(
        //field.name("partition").type(IntegerType),  can define JMS priority here
        field.messageHeaders(                         //set message properties via headers field
          field.messageHeader("account-id", "body.account_id"),
          field.messageHeader("updated", "body.details.updated_by.time"),
        )
      )
      .fields(
        field.messageBody(
          field.name("account_id").regex("ACC[0-9]{8}"),
          field.name("year").`type`(IntegerType).min(2021).max(2023),
          field.name("account_status").oneOf("open", "closed", "suspended", "pending"),
          field.name("amount").`type`(DoubleType),
          field.name("details").`type`(StructType)
            .fields(
              field.name("name").expression("#{Name.name}"),
              field.name("first_txn_date").`type`(DateType).sql("ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"),
              field.name("updated_by").`type`(StructType)
                .fields(
                  field.name("user"),
                  field.name("time").`type`(TimestampType),
                ),
            ),
          field.name("transactions").`type`(ArrayType)
            .fields(
              field.name("txn_date").`type`(DateType).min(Date.valueOf("2021-01-01")).max("2021-12-31"),
              field.name("amount").`type`(DoubleType),
            )
        )
      )
      .count(count.records(10))
    ```

=== "YAML"

    In `docker/data/custom/task/solace/solace-task.yaml`:
    ```yaml
    name: "solace_task"
    steps:
      - name: "solace_account"
        options:
          destinationName: "/JNDI/Q/rest_test_queue"
        fields:
          - name: "messageBody"
            fields:
              - name: "account_id"
              - name: "year"
                type: "int"
                options:
                  min: "2021"
                  max: "2022"
              - name: "amount"
                type: "double"
                options:
                  min: "10.0"
                  max: "100.0"
              - name: "details"
                fields:
                  - name: "name"
                  - name: "first_txn_date"
                    type: "date"
                    options:
                      sql: "ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"
                  - name: "updated_by"
                    fields:
                      - name: "user"
                      - name: "time"
                        type: "timestamp"
              - name: "transactions"
                type: "array"
                fields:
                  - name: "txn_date"
                    type: "date"
                  - name: "amount"
                    type: "double"
          - name: "messageHeaders"
            fields:
              - name: "account-id"
                options:
                  sql: "body.account_id"
              - name: "updated"
                options:
                  sql: "body.details.update_by.time"
    ```

=== "UI"

    1. Click on `Generation` and tick the `Manual` checkbox
    1. Click on `+ Field`
        1. Add name as `key`
        1. Click on `Select data type` and select `string`
        1. Click `+` next to data type and select `Sql`. Then enter `body.account_id`
        1. Click on `+ Field` and add name as `messageBody`
        1. Click on `Select data type` and select `struct`
        1. Click on `+ Field` under `messageBody` and add name as `account_id`
        1. Add additional fields under `messageBody` with your own metadata
        1. Click on `+ Field` and add name as `messageHeaders`
        1. Click on `Select data type` and select `struct`
        1. Click on `+ Field` under `messageHeaders` and add name as `account_id`
        1. Add additional fields under `messageHeaders` with your own metadata

#### Fields

The schema defined for Solace has a format that needs to be followed as noted above. Specifically, the required fields
are:
- `messageBody`

Whilst, the other fields are optional:

- `partition` - refers to JMS priority of the message
- `messageHeaders` - refers to JMS message properties

##### Message Headers

If your messages contain headers, you can follow the details below on generating header values. These can be based off
values contained within you message body or could be static values, just like any other generated field. The main
restriction imposed here is that the `key` of the message header is static and the `value` has to be a valid SQL 
expression.

=== "Java"

    ```java
    field().messageHeaders(
            field().messageHeader("account-id", "body.account_id"),
            field().messageHeader("updated", "body.details.updated_by-time")
    )
    ```

=== "Scala"

    ```scala
    field.messageHeaders(
      field.messageHeader("account-id", "body.account_id"),
      field.messageHeader("updated", "body.details.updated_by.time"),
    )
    ```

=== "YAML"

    In `docker/data/custom/task/solace/solace-task.yaml`:
    ```yaml
    name: "solace_task"
    steps:
      - name: "solace_account"
        options:
          destinationName: "/JNDI/Q/rest_test_queue"
        fields:
          - name: "messageHeaders"
            fields:
              - name: "account-id"
                options:
                  sql: "body.account_id"
              - name: "updated"
                options:
                  sql: "body.details.update_by.time"
    ```

=== "UI"

    1. Click on `+ Field` and add name as `messageHeaders`
    1. Click on `Select data type` and select `struct`
    1. Click on `+ Field` under `messageHeaders` and add name as `account_id`
    1. Add additional fields under `messageHeaders` with your own metadata

##### transactions

`transactions` is an array that contains an inner structure of `txn_date` and `amount`. The size of the array generated
can be controlled via `arrayMinLength` and `arrayMaxLength`.

=== "Java"

    ```java
    field().name("transactions").type(ArrayType.instance())
            .fields(
                    field().name("txn_date").type(DateType.instance()).min(Date.valueOf("2021-01-01")).max("2021-12-31"),
                    field().name("amount").type(DoubleType.instance())
            )
    ```

=== "Scala"

    ```scala
    field.name("transactions").`type`(ArrayType)
      .fields(
        field.name("txn_date").`type`(DateType).min(Date.valueOf("2021-01-01")).max("2021-12-31"),
        field.name("amount").`type`(DoubleType),
      )
    ```

=== "YAML"

    In `docker/data/custom/task/solace/solace-task.yaml`:
    ```yaml
    name: "solace_task"
    steps:
      - name: "solace_account"
        options:
          destinationName: "/JNDI/Q/rest_test_queue"
        fields:
          - name: "messageBody"
            fields:
              - name: "transactions"
                type: "array"
                fields:
                  - name: "txn_date"
                    type: "date"
                  - name: "amount"
                    type: "double"
    ```

=== "UI"

    !!! warning "Warning"
        Inner field definition for array type is currently not supported from the UI. Will be added in the near future!

##### details

`details` is another example of a nested schema structure where it also has a nested structure itself in `updated_by`.
One thing to note here is the `first_txn_date` field has a reference to the `content.transactions` array where it will
sort the array by `txn_date` and get the first element.

=== "Java"

    ```java
    field().name("details")
            .fields(
                    field().name("name").expression("#{Name.name}"),
                    field().name("first_txn_date").type(DateType.instance()).sql("ELEMENT_AT(SORT_ARRAY(content.transactions.txn_date), 1)"),
                    field().name("updated_by")
                            .fields(
                                    field().name("user"),
                                    field().name("time").type(TimestampType.instance())
                            )
            )
    ```

=== "Scala"

    ```scala
    field.name("details")
      .fields(
        field.name("name").expression("#{Name.name}"),
        field.name("first_txn_date").`type`(DateType).sql("ELEMENT_AT(SORT_ARRAY(content.transactions.txn_date), 1)"),
        field.name("updated_by")
          .fields(
            field.name("user"),
            field.name("time").`type`(TimestampType),
          ),
      )
    ```

=== "YAML"

    In `docker/data/custom/task/solace/solace-task.yaml`:
    ```yaml
    name: "solace_task"
    steps:
      - name: "solace_account"
        options:
          destinationName: "/JNDI/Q/rest_test_queue"
        fields:
          - name: "messageBody"
            fields:
              - name: "details"
                fields:
                  - name: "name"
                  - name: "first_txn_date"
                    type: "date"
                    options:
                      sql: "ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"
                  - name: "updated_by"
                    fields:
                      - name: "user"
                      - name: "time"
                        type: "timestamp"
    ```

=== "UI"

    1. Click on `+ Field` and add name as `messageBody`
    1. Click on `Select data type` and select `struct`
    1. Click on `+ Field` under `messageBody` and add name as `details`
    1. Add additional fields under `details` with your own metadata

#### Additional Configurations

At the end of data generation, a report gets generated that summarises the actions it performed. We can control the
output folder of that report via configurations.

=== "Java"

    ```java
    var config = configuration()
            .generatedReportsFolderPath("/opt/app/data/report");
    ```

=== "Scala"

    ```scala
    val config = configuration
      .generatedReportsFolderPath("/opt/app/data/report")
    ```

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    folders {
      generatedReportsFolderPath = "/opt/app/data/report"
    }
    ```

=== "UI"

    1. Click on `Advanced Configuration` towards the bottom of the screen
    1. Click on `Folder` and enter `/tmp/data-caterer/report` for `Generated Reports Folder Path`

#### Execute

To tell Data Caterer that we want to run with the configurations along with the `kafkaTask`, we have to call `execute`.

### Run

Now we can run via the script `./run.sh` that is in the top level directory of the `data-caterer/example` folder to run the
class we just created.

=== "Java"

    ```shell
    ./run.sh AdvancedSolaceJavaPlanRun
    #after completing, check http://localhost:8080 from browser
    ```

=== "Scala"

    ```shell
    ./run.sh AdvancedSolacePlanRun
    #after completing, check http://localhost:8080 from browser
    ```

=== "YAML"

    ```shell
    ./run.sh my-solace.yaml
    #after completing, check http://localhost:8080 from browser
    ```

=== "UI"

    1. Click the button `Execute` at the top
    1. Progress updates will show in the bottom right corner
    1. Click on `History` at the top
    1. Check for your plan name and see the result summary
    1. Click on `Report` on the right side to see more details of what was executed

Your output should look like this.

![Solace messages queued](../../../../diagrams/data-source/solace_messages_queued.png)

Unfortunately, there is no easy way to see the message content. You can check the message content from your application
or service that consumes these messages.

Also check the HTML report, found at `docker/sample/report/index.html`, that gets generated to get an overview of what
was executed. Or view the sample report found [here](../../../../sample/report/html/index.html).
