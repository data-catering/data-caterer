# Advanced use cases

## Special data formats

There are many options available for you to use when you have a scenario when data has to be a certain format.

1. Create expression [datafaker](https://www.datafaker.net/documentation/expressions/)
    1. Can be used to create names, addresses, or anything that can be found
       under [**here**](../sample/datafaker/expressions.txt)
2. Create regex

## Foreign keys across data sets

![Multiple data source foreign key example](../diagrams/foreign_keys.drawio.png "Multiple data source foreign keys")

Details for how you can configure foreign keys can be found [**here**](generator/foreign-key.md).

## Edge cases

For each given data type, there are edge cases which can cause issues when your application processes the data.
This can be controlled at a field level by including the following flag in the generator options:

=== "Java"

    ```java
    field()
      .name("amount")
      .type(DoubleType.instance())
      .enableEdgeCases(true)
      .edgeCaseProbability(0.1)
    ```

=== "Scala"

    ```scala
    field
      .name("amount")
      .`type`(DoubleType)
      .enableEdgeCases(true)
      .edgeCaseProbability(0.1)
    ```

=== "YAML"

    ```yaml
    fields:
      - name: "amount"
        type: "double"
        generator:
          type: "random"
          options:
            enableEdgeCases: "true"
            edgeCaseProb: 0.1
    ```

If you want to know all the possible edge cases for each data
type, [can check the documentation here](generator/data-generator.md).

## Scenario testing

You can create specific scenarios by adjusting the metadata found in the plan and tasks to your liking.  
For example, if you had two data sources, a Postgres database and a parquet file, and you wanted to save account data
into Postgres and transactions related to those accounts into a parquet file.
You can alter the `status` field in the account data to only generate `open` accounts
and define a foreign key between Postgres and parquet to ensure the same `account_id` is being used.  
Then in the parquet task, define 1 to 10 transactions per `account_id` to be generated.

[Postgres account generation example task](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/jdbc/postgres/postgres-account-task.yaml)  
[Parquet transaction generation example task](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/file/parquet/parquet-transaction-task.yaml)  
[Plan](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/plan/scenario-based.yaml)

## Cloud storage

### Data source

If you want to save the file types CSV, JSON, Parquet or ORC into cloud storage, you can do so via adding extra
configurations. Below is an example for S3.


=== "Java"

    ```java
    var csvTask = csv("my_csv", "s3a://my-bucket/csv/accounts")
      .fields(
        field().name("account_id"),
        ...
      );

    var s3Configuration = configuration()
      .runtimeConfig(Map.of(
        "spark.hadoop.fs.s3a.directory.marker.retention", "keep",
        "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true",
        "spark.hadoop.fs.defaultFS", "s3a://my-bucket",
        //can change to other credential providers as shown here
        //https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers
        "spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.access.key", "access_key",
        "spark.hadoop.fs.s3a.secret.key", "secret_key"
      ));
    
    execute(s3Configuration, csvTask);
    ```

=== "Scala"

    ```scala
    val csvTask = csv("my_csv", "s3a://my-bucket/csv/accounts")
      .fields(
        field.name("account_id"),
        ...
      )

    val s3Configuration = configuration
      .runtimeConfig(Map(
        "spark.hadoop.fs.s3a.directory.marker.retention" -> "keep",
        "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled" -> "true",
        "spark.hadoop.fs.defaultFS" -> "s3a://my-bucket",
        //can change to other credential providers as shown here
        //https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers
        "spark.hadoop.fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.access.key" -> "access_key",
        "spark.hadoop.fs.s3a.secret.key" -> "secret_key"
      ))

    execute(s3Configuration, csvTask)
    ```

=== "YAML"

    ```yaml
    folders {
       generatedPlanAndTaskFolderPath = "s3a://my-bucket/data-caterer/generated"
       planFilePath = "s3a://my-bucket/data-caterer/generated/plan/customer-create-plan.yaml"
       taskFolderPath = "s3a://my-bucket/data-caterer/generated/task"
    }
       
    runtime {
        config {
            ...
            #S3
            "spark.hadoop.fs.s3a.directory.marker.retention" = "keep"
            "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled" = "true"
            "spark.hadoop.fs.defaultFS" = "s3a://my-bucket"
            #can change to other credential providers as shown here
            #https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers
            "spark.hadoop.fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            "spark.hadoop.fs.s3a.access.key" = "access_key"
            "spark.hadoop.fs.s3a.secret.key" = "secret_key"
       }
    }
    ```

### Storing plan/task(s)

You can generate and store the plan/task files inside either AWS S3, Azure Blob Storage or Google GCS.
This can be controlled via configuration set in the `application.conf` file where you can set something like the below:

=== "Java"

    ```java
    configuration()
      .generatedReportsFolderPath("s3a://my-bucket/data-caterer/generated")
      .planFilePath("s3a://my-bucket/data-caterer/generated/plan/customer-create-plan.yaml")
      .taskFolderPath("s3a://my-bucket/data-caterer/generated/task")
      .runtimeConfig(Map.of(
        "spark.hadoop.fs.s3a.directory.marker.retention", "keep",
        "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true",
        "spark.hadoop.fs.defaultFS", "s3a://my-bucket",
        //can change to other credential providers as shown here
        //https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers
        "spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.access.key", "access_key",
        "spark.hadoop.fs.s3a.secret.key", "secret_key"
      ));
    ```

=== "Scala"

    ```scala
    configuration
      .generatedReportsFolderPath("s3a://my-bucket/data-caterer/generated")
      .planFilePath("s3a://my-bucket/data-caterer/generated/plan/customer-create-plan.yaml")
      .taskFolderPath("s3a://my-bucket/data-caterer/generated/task")
      .runtimeConfig(Map(
        "spark.hadoop.fs.s3a.directory.marker.retention" -> "keep",
        "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled" -> "true",
        "spark.hadoop.fs.defaultFS" -> "s3a://my-bucket",
        //can change to other credential providers as shown here
        //https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers
        "spark.hadoop.fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.access.key" -> "access_key",
        "spark.hadoop.fs.s3a.secret.key" -> "secret_key"
      ))
    ```

=== "YAML"

    ```yaml
    folders {
       generatedPlanAndTaskFolderPath = "s3a://my-bucket/data-caterer/generated"
       planFilePath = "s3a://my-bucket/data-caterer/generated/plan/customer-create-plan.yaml"
       taskFolderPath = "s3a://my-bucket/data-caterer/generated/task"
    }
       
    runtime {
        config {
            ...
            #S3
            "spark.hadoop.fs.s3a.directory.marker.retention" = "keep"
            "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled" = "true"
            "spark.hadoop.fs.defaultFS" = "s3a://my-bucket"
            #can change to other credential providers as shown here
            #https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers
            "spark.hadoop.fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            "spark.hadoop.fs.s3a.access.key" = "access_key"
            "spark.hadoop.fs.s3a.secret.key" = "secret_key"
       }
    }
    ```
