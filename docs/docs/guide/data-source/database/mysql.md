---
title: "MySQL Test Data Management"
description: "Example of MySQL data generation and testing tool that can automatically discover, generate and validate."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# MySQL

Creating a data generator for MySQL. You will build a Docker image that will be able to populate data in MySQL
for the tables you configure.

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/scala/io/github/datacatering/plan/MySqlPlanRun.scala) | [:material-coffee: Java Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/java/io/github/datacatering/plan/MySqlJavaPlanRun.java) | [:simple-yaml: YAML Example](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/jdbc/mysql)

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

If you already have a MySQL instance running, you can skip to [this step](#plan-setup).

### MySQL Setup

Next, let's make sure you have an instance of MySQL up and running in your local environment. This will make it
easy for us to iterate and check our changes.

```shell
cd docker
docker-compose up -d mysql
```

#### Permissions

Let's make a new user that has the required permissions needed to push data into the MySQL tables we want.

???+ tip "SQL Permission Statements"

    ```sql
    GRANT INSERT ON <schema>.<table> TO data_caterer_user;
    ```

Following permissions are required when enabling `configuration.enableGeneratePlanAndTasks(true)` as it will gather
metadata information about tables and fields from the below tables.

???+ tip "SQL Permission Statements"

    ```sql
    GRANT SELECT ON information_schema.columns TO < user >;
    GRANT SELECT ON information_schema.statistics TO < user >;
    GRANT SELECT ON information_schema.key_column_usage TO < user >;
    ```

### Plan Setup

Create a file depending on which interface you want to use.

- Java: `src/main/java/io/github/datacatering/plan/MySQLJavaPlan.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MySQLPlan.scala`
- YAML: `docker/data/custom/plan/my-mysql.yaml`

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;
    
    public class MySQLJavaPlan extends PlanRun {
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    
    class MySQLPlan extends PlanRun {
    }
    ```

=== "YAML"

    In `docker/data/custom/plan/my-mysql.yaml`:
    ```yaml
    name: "my_mysql_plan"
    description: "Create account data via MySQL"
    tasks:
      - name: "mysql_task"
        dataSourceName: "my_mysql"
    ```

=== "UI"

    1. Click on `Connection` towards the top of the screen
    1. For connection name, set to `my_mysql`
    1. Click on `Select data source type..` and select `MySQL`
    1. Set URL as `jdbc:mysql://localhost:3306/customer`
    1. Set username as `root`
    1. Set password as `root`
        1. Optionally, we could set a schema and table name but if you have more than schema or table, you would have to create new connection for each
    1. Click on `Create`
    1. You should see your connection `my_mysql` show under `Existing connections`
    1. Click on `Home` towards the top of the screen
    1. Set plan name to `my_mysql_plan`
    1. Set task name to `mysql_task`
    1. Click on `Select data source..` and select `my_mysql`

This class defines where we need to define all of our configurations for generating data. There are helper variables and
methods defined to make it simple and easy to use.

#### Connection Configuration

Within our class, we can start by defining the connection properties to connect to MySQL.

=== "Java"

    ```java
    var accountTask = mysql(
        "customer_mysql",                                   //name
        "jdbc:mysql://host.docker.internal:3306/customer",  //url
        "root",                                             //username
        "root",                                             //password
        Map.of()                                            //optional additional connection options
    )
    ```
    
    Additional options such as SSL configuration, etc can be found [**here**](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option).

=== "Scala"

    ```scala
    val accountTask = mysql(
        "customer_mysql",                                   //name
        "jdbc:mysql://host.docker.internal:3306/customer",  //url
        "root",                                             //username
        "root",                                             //password
        Map()                                               //optional additional connection options
    )
    ```
    
    Additional options such as SSL configuration, etc can be found [**here**](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option).

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    jdbc {
        customer_mysql {
            url = "jdbc:mysql://jdbc:mysql://host.docker.internal:3306/customer/customer"
            user = "root"
            password = "root"
            driver = "com.mysql.cj.jdbc.Driver"
        }
    }
    ```

=== "UI"

    1. We have already created the connection details in [this step](#plan-setup)

#### Schema

Let's create a task for inserting data into the `customer.accounts` and `customer.balances` tables as
defined under`docker/data/sql/mysql/customer.cql`. This table should already be setup for you if you followed this
[step](#mysql-setup).

Trimming the connection details to work with the docker-compose MySQL, we have a base MySQL connection to define
the table and schema required. Let's define each field along with their corresponding data type. You will notice that
the `text` fields do not have a data type defined. This is because the default data type is `StringType` which
corresponds to `text` in MySQL.

=== "Java"

    ```java
    {
        var accountTask = mysql("customer_mysql", "jdbc:mysql://host.docker.internal:3306/customer")
                .table("customer", "accounts")
                .fields(
                        field().name("account_number"),
                        field().name("amount").type(DoubleType.instance()),
                        field().name("created_by"),
                        field().name("created_by_fixed_length"),
                        field().name("open_timestamp").type(TimestampType.instance()),
                        field().name("account_status")
                );
    }
    ```

=== "Scala"

    ```scala
    val accountTask = mysql("customer_mysql", "jdbc:mysql://host.docker.internal:3306/customer")
      .table("customer", "accounts")
      .fields(
        field.name("account_number"),
        field.name("amount").`type`(DoubleType),
        field.name("created_by"),
        field.name("created_by_fixed_length"),
        field.name("open_timestamp").`type`(TimestampType),
        field.name("account_status")
      )
    ```

=== "YAML"

    In `docker/data/custom/task/mysql/mysql-task.yaml`:
    ```yaml
    name: "mysql_task"
    steps:
      - name: "accounts"
        type: "mysql"
        options:
          dbtable: "customer.accounts"
        fields:
        - name: "account_number"
        - name: "amount"
          type: "double"
        - name: "created_by"
        - name: "created_by_fixed_length"
        - name: "open_timestamp"
          type: "timestamp"
        - name: "account_status"
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
    1. Click on `+ Field` and add name as `created_by_fixed_length`
    1. Click on `Select data type` and select `string`
    1. Click on `+ Field` and add name as `open_timestamp`
    1. Click on `Select data type` and select `timestamp`
    1. Click on `+ Field` and add name as `account_status`
    1. Click on `Select data type` and select `string`

Depending on how you want to define the schema, follow the below:

- [Manual schema guide](../../scenario/data-generation.md#schema)
- Automatically detect schema from the data source, you can simply
  enable `configuration.enableGeneratePlanAndTasks(true)`
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
    public class MySQLJavaPlan extends PlanRun {
        {
            var accountTask = mysql("customer_mysql", "jdbc:mysql://host.docker.internal:3306/customer")
                    .table("customer", "accounts")
                    .fields(
                            field().name("account_number").regex("ACC[0-9]{8}").primaryKey(true),
                            field().name("amount").type(DoubleType.instance()).min(1).max(1000),
                            field().name("created_by").expression("#{Name.name}"),
                            field().name("created_by_fixed_length").sql("CASE WHEN account_status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
                            field().name("open_timestamp").type(TimestampType.instance()).min(java.sql.Date.valueOf("2022-01-01")),
                            field().name("account_status").oneOf("open", "closed", "suspended", "pending")
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
    class MySQLPlan extends PlanRun {
      val accountTask = mysql("customer_mysql", "jdbc:mysql://host.docker.internal:3306/customer")
        .table("customer", "accounts")
        .fields(
          field.name("account_number").primaryKey(true),
          field.name("amount").`type`(DoubleType).min(1).max(1000),
          field.name("created_by").expression("#{Name.name}"),
          field.name("created_by_fixed_length").sql("CASE WHEN account_status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
          field.name("open_timestamp").`type`(TimestampType).min(java.sql.Date.valueOf("2022-01-01")),
          field.name("account_status").oneOf("open", "closed", "suspended", "pending")
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

Now we can run via the script `./run.sh` that is in the top level directory of the `data-caterer/example` folder to run the
class we just
created.

=== "Java"

    ```shell
    ./run.sh MySQLJavaPlan
    docker exec docker-mysql-1  mysql -u root "-proot" "customer" -e "SELECT COUNT(1) FROM customer.accounts; SELECT * FROM customer.accounts LIMIT 10;"
    ```

=== "Scala"

    ```shell
    ./run.sh MySQLPlan
    docker exec docker-mysql-1  mysql -u root "-proot" "customer" -e "SELECT COUNT(1) FROM customer.accounts; SELECT * FROM customer.accounts LIMIT 10;"
    ```

=== "YAML"

    ```shell
    ./run.sh my-mysql.yaml
    docker exec docker-mysql-1  mysql -u root "-proot" "customer" -e "SELECT COUNT(1) FROM customer.accounts; SELECT * FROM customer.accounts LIMIT 10;"
    ```

=== "UI"

    1. Click the button `Execute` at the top
    1. Progress updates will show in the bottom right corner
    1. Click on `History` at the top
    1. Check for your plan name and see the result summary
    1. Click on `Report` on the right side to see more details of what was executed

Your output should look like this.

```shell
COUNT(1)
10
id	account_number	account_status	created_by	created_by_fixed_length	customer_id_int	customer_id_smallint	customer_id_bigint	customer_id_decimal	customer_id_real	customer_id_double	open_date	open_timestamp	last_opened_time	payload_bytes
1	0507581306	suspended	Pete Pouros	event	510	NULL	NULL	NULL	NULL	NULL	NULL	2023-08-08 06:55:09	NULL	NULL
2	0998204877	pending	Kraig Balistreri	event	987	NULL	NULL	NULL	NULL	NULL	NULL	2023-09-04 10:09:27	NULL	NULL
3	1491488574	pending	Mrs. Ali DuBuque	event	43	NULL	NULL	NULL	NULL	NULL	NULL	2024-05-11 06:43:07	NULL	NULL
4	5209805789	suspended	Lorilee Gislason	event	975	NULL	NULL	NULL	NULL	NULL	NULL	2023-08-30 20:54:40	NULL	NULL
5	5901422604	closed	Elbert Johnston	eod	369	NULL	NULL	NULL	NULL	NULL	NULL	2023-08-07 13:46:58	NULL	NULL
6	7930880350	pending	Charlie McCullough	event	797	NULL	NULL	NULL	NULL	NULL	NULL	2024-02-05 15:16:46	NULL	NULL
7	8248715689	pending	Mila Becker	event	236	NULL	NULL	NULL	NULL	NULL	NULL	2024-06-07 08:51:29	NULL	NULL
8	8709384015	closed	Dinah Zemlak	eod	965	NULL	NULL	NULL	NULL	NULL	NULL	2024-01-14 20:58:51	NULL	NULL
9	9463221576	closed	Les Hettinger	eod	83	NULL	NULL	NULL	NULL	NULL	NULL	2023-06-27 16:05:22	NULL	NULL
10	9903967165	closed	Ms. Napoleon Walker	eod	795	NULL	NULL	NULL	NULL	NULL	NULL	2023-10-07 13:42:24	NULL	NULL
```

Also check the HTML report, found at `docker/sample/report/index.html`, that gets generated to get an overview of what
was executed.

![Sample report](../../../../sample/report/report_screenshot.png)

### Validation

If you want to validate data from MySQL,
[follow the validation documentation found here to help guide you](../../../validation.md).
