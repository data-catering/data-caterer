package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.{ArrayType, DateType, DoubleType, IntegerType}

import java.sql.Date

class ExamplePlanRun extends PlanRun {

  val planBuilder = plan.name("sample plan")

  val tasksBuilder = tasks.
    addTask("account_json", "fs_json",
      step.name("account")
        .option(("path", "app/src/test/resources/sample/json/account"))
        .schema(schema.addFields(
          field.name("account_id"),
          field.name("year").`type`(IntegerType).min(2022),
          field.name("name").static("peter")
        ))
    )

  execute(List(tasksBuilder), planBuilder)
}

class MinimalPlanRun extends PlanRun {
  execute(configuration =
    configuration
      .enableGeneratePlanAndTasks(true)
      .addConnectionConfig("account_json", "json", Map("path" -> "app/src/test/resources/sample/json/account"))
  )
}

class MinimalPlanWithManualTaskRun extends PlanRun {
  val tasksBuilder = tasks.addTask("my_task", "mininal_json",
    step
      .option(("path", "app/src/test/resources/sample/json/minimal"))
      .schema(schema.addFields(field.name("account_id")))
  )
  execute(tasksBuilder)
}


class LargeCountRun extends PlanRun {
  val jsonTask = json("mininal_json", "app/src/test/resources/sample/json/large")
    .schema(schema.addFields(
      field.name("account_id"),
      field.name("year").`type`(IntegerType).min(2022),
      field.name("name").expression("#{Name.name}"),
      field.name("amount").`type`(DoubleType).max(1000.0),
      field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
      field.name("status").oneOf("open", "closed"),
      field.name("txn_list")
        .`type`(ArrayType)
        .schema(schema.addFields(
          field.name("id"),
          field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
          field.name("amount").`type`(DoubleType)
        ))
    ))
    .count(count
      .records(10000)
      .recordsPerColumn(100, "account_id")
    )

  val conf = configuration
    .enableCount(true)
    .generatedReportsFolderPath("app/src/test/resources/sample/report")
  execute(conf, jsonTask)
}

class DocsPlanRun extends PlanRun {

  validationConfig
    .name("account_checks")
    .description("Check account related fields have gone through system correctly")
    .addValidations(
      "accountJson",
      Map("path" -> "sample/json/txn-gen"),
      validation.expr("amount < 100"),
      validation.expr("year == 2021").errorThreshold(0.1),
      validation.expr("regexp_like(name, 'Peter .*')").errorThreshold(200).description("Should be lots of Peters")
    )
  val t = task
    .name("csv_file")
    .steps(
      step
        .name("transactions")
        .`type`("csv")
        .option("path", "app/src/test/resources/sample/csv/transactions")
        .count(
          count
            .records(1000)
            .recordsPerColumnGenerator(
              generator.min(1).max(2),
              "account_id"
            )
        )
        .schema(schema.addField("account_id"))
    )
}

class FullExamplePlanRun extends PlanRun {

  val startDate = Date.valueOf("2022-01-01")
  val accountIdField = field.name("account_id").regex("ACC[0-9]{8}")
  val nameField = field.name("name").expression("#{Name.name}")

  val postgresTask = task.name("postgres_account_details")
    .steps(
      step
        .name("transaction")
        .jdbcTable("account.transaction")
        .schema(schema.addFields(
          accountIdField,
          field.name("txn_id").regex("txn_[0-9]{5}"),
          field.name("year").`type`(IntegerType).sql("YEAR(date)"),
          nameField,
          field.name("date").`type`(DateType).min(startDate),
          field.name("amount").`type`(DoubleType).max(10000),
          field.name("credit_debit").sql("CASE WHEN amount < 0 THEN 'C' ELSE 'D' END")
        )),
      step
        .name("account")
        .jdbcTable("account.account")
        .schema(schema.addFields(
          accountIdField,
          nameField,
          field.name("open_date").`type`(DateType).min(startDate),
          field.name("status").oneOf("open", "closed", "pending")
        ))
    )

  val jsonTask = task.name("json_account_details")
    .steps(
      step
        .name("account_info")
        .path("/tmp/src/main/resources/sample/json")
        .schema(schema.addFields(
          accountIdField,
          nameField,
          field.name("txn_list")
            .`type`(ArrayType)
            .schema(schema.addFields(
              field.name("id"),
              field.name("date").`type`(DateType).min(startDate),
              field.name("amount").`type`(DoubleType),
            ))
        ))
    )

  val conf = configuration
    .postgres("customer_postgres")
    .json("account_json")

  val p = plan.taskSummaries(
    taskSummary.dataSource("customer_postgres").task(postgresTask),
    taskSummary.dataSource("account_json").task(jsonTask),
  ).addForeignKeyRelationship(
    foreignField("customer_postgres", "account", "account_id"),
    foreignField("customer_postgres", "transaction", "account_id")
  ).addForeignKeyRelationship(
    foreignField("customer_postgres", "account", "account_id"),
    foreignField("account_json", "account_info", "account_id")
  )

  execute(p, conf)
}

class ConnectionBasedApiPlanRun extends PlanRun {

  val csvGenerate = csv("my_csv", "app/src/test/resources/sample/connection-api/csv")
    .schema(
      field.name("account_id"),
      field.name("year").`type`(IntegerType).min(2022)
    )
    .count(count.records(100))

  val jsonGenerate = json("my_json", "app/src/test/resources/sample/connection-api/json")
    .partitionBy("age")
    .schema(
      field.name("name").expression("#{Name.name}"),
      field.name("age").`type`(IntegerType).min(18).max(20),
    )
    .count(count.records(100))

  val x = json("account_info", "/tmp/data-caterer/json")
    .schema(
      field.name("account_id"),
      field.name("year").`type`(IntegerType).min(2022),
      field.name("name").expression("#{Name.name}"),
      field.name("amount").`type`(DoubleType).max(1000.0),
      field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
      field.name("status").oneOf("open", "closed"),
      field.name("txn_list")
        .`type`(ArrayType)
        .schema(schema.addFields(
          field.name("id"),
          field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
          field.name("amount").`type`(DoubleType),
        ))
    )
    .count(count.records(100))

  val postgresGenerate = postgres("my_postgres")
    .task(
      step
        .jdbcTable("public.accounts")
        .schema(
          field.name("account_id"),
          field.name("name").expression("#{Name.name}"),
        ),
      step
        .jdbcTable("public.transactions")
        .schema(
          field.name("account_id"),
          field.name("amount").`type`(DoubleType).max(1000)
        )
        .count(count.recordsPerColumn(10, "account_id"))
    )

  val postgresAcc = postgres("my_postgres")
    .table("public.accounts")
    .schema(
      field.name("account_id")
    )
  var jsonTask = json("my_json", "/tmp/json")
    .schema(
      field.name("account_id"),
      field.name("customer_details")
        .schema(
          field.name("name").sql("_join_txn_name").`type`(DoubleType).enableEdgeCases(true).edgeCaseProbability(0.1)
        ),
      field.name("_join_txn_name").omit(true)
    )
  plan.addForeignKeyRelationship(
    postgresAcc, List("account_id", ""),
    List(jsonTask -> List("account_id", ""))
  )
  val csvTask = csv("my_csv", "s3a://my-bucket/csv/accounts")
    .schema(
      field.name("account_id"),
  )
  val conf = configuration
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
  execute(conf, csvGenerate, jsonGenerate)
}

class DocumentationPlanRun extends PlanRun {
  val jsonTask = json("account_info", "/opt/app/data/json")
    .schema(
      field.name("account_id").regex("ACC[0-9]{8}"),
      field.name("year").`type`(IntegerType).sql("YEAR(date)"),
      field.name("name").expression("#{Name.name}"),
      field.name("amount").`type`(DoubleType).min(10).max(1000),
      field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
      field.name("status").oneOf("open", "closed"),
      field.name("txn_list")
        .`type`(ArrayType)
        .schema(schema.addFields(
          field.name("id").sql("_join_txn_id"),
          field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
          field.name("amount").`type`(DoubleType)
        )),
      field.name("_join_txn_id").omit(true)
    )
    .count(count.records(100))

  val csvTxns = csv("transactions", "/opt/app/data/csv")
    .schema(
      field.name("account_id"),
      field.name("txn_id"),
      field.name("amount"),
      field.name("merchant").expression("#{Company.name}"),
    )
    .count(count.recordsPerColumnGenerator(generator.min(1).max(5), "account_id"))

  val foreignKeySetup = plan
    .addForeignKeyRelationship(jsonTask, "account_id", List((csvTxns, "account_id")))
    .addForeignKeyRelationship(jsonTask, "_join_txn_id", List((csvTxns, "txn_id")))
    .addForeignKeyRelationship(jsonTask, "amount", List((csvTxns, "amount")))

  execute(foreignKeySetup, configuration, jsonTask, csvTxns)
}