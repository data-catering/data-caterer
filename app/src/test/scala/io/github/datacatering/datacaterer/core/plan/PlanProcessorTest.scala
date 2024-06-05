package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.SAVE_MODE
import io.github.datacatering.datacaterer.api.model.{ArrayType, DateType, DoubleType, IntegerType, TimestampType}
import io.github.datacatering.datacaterer.core.util.{ObjectMapperUtil, SparkSuite}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.{Files, Path}
import java.sql.{Date, Timestamp}

@RunWith(classOf[JUnitRunner])
class PlanProcessorTest extends SparkSuite {

  private val scalaBaseFolder = "src/test/resources/sample/documentation"
  private val javaBaseFolder = "src/test/resources/sample/java/documentation"

  class DocumentationPlanRun extends PlanRun {
    {
      val accountStatus = List("open", "closed", "pending", "suspended")
      val jsonTask = json("account_info", s"$scalaBaseFolder/json", Map(SAVE_MODE -> "overwrite"))
        .schema(
          field.name("account_id").regex("ACC[0-9]{8}"),
          field.name("year").`type`(IntegerType).sql("YEAR(date)"),
          field.name("balance").`type`(DoubleType).min(10).max(1000),
          field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
          field.name("status").oneOf(accountStatus: _*),
          field.name("update_history")
            .`type`(ArrayType)
            .schema(
              field.name("updated_time").`type`(TimestampType).min(Timestamp.valueOf("2022-01-01 00:00:00")),
              field.name("prev_status").oneOf(accountStatus: _*),
              field.name("new_status").oneOf(accountStatus: _*)
            ),
          field.name("customer_details")
            .schema(
              field.name("name").sql("_join_txn_name"),
              field.name("age").`type`(IntegerType).min(18).max(90),
              field.name("city").expression("#{Address.city}")
            ),
          field.name("_join_txn_name").expression("#{Name.name}").omit(true)
        )
        .count(count.records(100))

      val csvTxns = csv("transactions", s"$scalaBaseFolder/csv", Map(SAVE_MODE -> "overwrite", "header" -> "true"))
        .schema(
          field.name("account_id"),
          field.name("txn_id"),
          field.name("name"),
          field.name("amount").`type`(DoubleType).min(10).max(100),
          field.name("merchant").expression("#{Company.name}"),
          field.name("time").`type`(TimestampType),
          field.name("date").`type`(DateType).sql("DATE(time)"),
        )
        .count(
          count
            .records(100)
            .recordsPerColumnGenerator(generator.min(1).max(2), "account_id", "name")
        )
        .validationWait(waitCondition.pause(1))
        .validations(
          validation.expr("amount > 0").errorThreshold(0.01),
          validation.expr("LENGTH(name) > 3").errorThreshold(5),
          validation.expr("LENGTH(merchant) > 0").description("Non-empty merchant name"),
          validation.preFilter(columnPreFilter("name").isEqual("peter"))
            .col("amount").greaterThan(50),
          validation.preFilter(
            preFilterBuilder(columnPreFilter("name").startsWith("john"))
              .and(columnPreFilter("amount").greaterThan(10))
          ).col("account_id").isNotNull,
        )

      val foreignKeySetup = plan
        .addForeignKeyRelationship(
          jsonTask, List("account_id", "_join_txn_name"),
          List((csvTxns, List("account_id", "name")), (csvTxns, List("account_id", "name")))
        )
        .addForeignKeyRelationship(jsonTask, List("account_id"), List(), List((csvTxns, List("account_id"))))
      val conf = configuration
        .generatedReportsFolderPath(s"$scalaBaseFolder/report")
        .enableValidation(true)
        .enableSinkMetadata(true)

      execute(foreignKeySetup, conf, jsonTask, csvTxns)
    }
  }

  test("Can run documentation plan run") {
    PlanProcessor.determineAndExecutePlan(Some(new DocumentationPlanRun()))
    verifyGeneratedData(scalaBaseFolder)
  }

  ignore("Can run Java plan run") {
    PlanProcessor.determineAndExecutePlanJava(new ExampleJavaPlanRun(javaBaseFolder))
    verifyGeneratedData(javaBaseFolder)
  }

  private def verifyGeneratedData(folder: String) = {
    val jsonData = sparkSession.read.json(s"$folder/json").selectExpr("*", "customer_details.name AS name").collect()
    val csvData = sparkSession.read.option("header", "true").csv(s"$folder/csv").collect()
    val csvCount = csvData.length
    assert(jsonData.length == 100)
    assert(csvCount >= 100 && csvCount <= 200)
    val jsonRecord = jsonData.head
    val jsonAccountId = jsonRecord.getString(0)
    val csvMatchAccount = csvData.filter(r => r.getString(0).equalsIgnoreCase(jsonAccountId))
    val csvMatchCount = csvMatchAccount.length
    assert(csvMatchCount >= 1 && csvMatchCount <= 2)
    assert(csvMatchAccount.forall(r => r.getAs[String]("name").equalsIgnoreCase(jsonRecord.getAs[String]("name"))))
    assert(csvData.forall(r => r.getAs[String]("time").substring(0, 10) == r.getAs[String]("date")))
  }

  ignore("Write YAML for plan") {
    val docPlanRun = new DocumentationPlanRun()
    val planWrite = ObjectMapperUtil.yamlObjectMapper.writeValueAsString(docPlanRun._plan)
    val validWrite = ObjectMapperUtil.yamlObjectMapper.writeValueAsString(docPlanRun._validations)
    Files.writeString(Path.of("/tmp/my-plan.yaml"), planWrite)
    Files.writeString(Path.of("/tmp/my-validation.yaml"), validWrite)
  }

  ignore("Can run Postgres plan run") {
    PlanProcessor.determineAndExecutePlan(Some(new TestOtherFileFormats))
  }

  class TestPostgres extends PlanRun {
    val jsonTask = json("my_json", "/tmp/data/json", Map("saveMode" -> "overwrite"))
      .schema(
        field.name("account_id").regex("ACC[0-9]{8}"),
        field.name("name").expression("#{Name.name}"),
        field.name("amount").`type`(DoubleType).max(10),
      )
      .count(count.recordsPerColumn(2, "account_id", "name"))
      .validations(
        validation.groupBy("account_id", "name").max("amount").lessThan(100),
        validation.unique("account_id", "name"),
      )
    val csvTask = json("my_csv", "/tmp/data/csv", Map("saveMode" -> "overwrite"))
      .schema(
        field.name("account_number").regex("[0-9]{8}"),
        field.name("name").expression("#{Name.name}"),
        field.name("amount").`type`(DoubleType).max(10),
      )
      .validations(
        validation.col("account_number").isNotNull.description("account_number is a primary key"),
        validation.col("name").matches("[A-Z][a-z]+ [A-Z][a-z]+").errorThreshold(0.3).description("Some names follow a different pattern"),
      )

    val conf = configuration
      .generatedReportsFolderPath("/tmp/report")
      .enableSinkMetadata(true)

    execute(conf, jsonTask, csvTask)
  }

  class TestCsvPostgres extends PlanRun {
    val csvTask = csv("my_csv", "/tmp/data/csv", Map("saveMode" -> "overwrite", "header" -> "true"))
      .numPartitions(1)
      .schema(metadataSource.marquez("http://localhost:5001", "food_delivery", "public.delivery_7_days"))
      .count(count.records(10))

    val postgresTask = postgres("my_postgres", "jdbc:postgresql://localhost:5432/food_delivery", "postgres", "password")
      .schema(metadataSource.marquez("http://localhost:5001", "food_delivery"))
      .count(count.records(10))

    val foreignCols = List("order_id", "order_placed_on", "order_dispatched_on", "order_delivered_on", "customer_email",
      "customer_address", "menu_id", "restaurant_id", "restaurant_address", "menu_item_id", "category_id", "discount_id",
      "city_id", "driver_id")

    val myPlan = plan.addForeignKeyRelationships(
      csvTask, foreignCols,
      List(foreignField(postgresTask, "food_delivery_public.delivery_7_days", foreignCols))
    )

    val conf = configuration.enableGeneratePlanAndTasks(true)
      .generatedReportsFolderPath("/tmp/report")

    execute(myPlan, conf, csvTask, postgresTask)
  }

  class TestJson extends PlanRun {
    val jsonTask = json("my_json", "/tmp/data/json", Map("saveMode" -> "overwrite"))
      .schema(
        field.name("account_id").regex("ACC[0-9]{8}"),
        field.name("year").`type`(IntegerType).sql("YEAR(date)"),
        field.name("balance").`type`(DoubleType).min(10).max(1000),
        field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
        field.name("status").sql("element_at(sort_array(update_history, false), 1).status"),
        field.name("update_history")
          .`type`(ArrayType)
          .arrayMinLength(1)
          .schema(
            field.name("updated_time").`type`(TimestampType).min(Timestamp.valueOf("2022-01-01 00:00:00")),
            field.name("status").oneOf("open", "closed")
          ),
        field.name("customer_details")
          .schema(
            field.name("name").expression("#{Name.name}"),
            field.name("age").`type`(IntegerType).min(18).max(90),
            field.name("city").expression("#{Address.city}")
          )
      )

    execute(jsonTask)
  }

  class TestValidation extends PlanRun {
    val firstJsonTask = json("my_first_json", "/tmp/data/first_json", Map("saveMode" -> "overwrite"))
      .schema(
        field.name("account_id").regex("ACC[0-9]{8}"),
        field.name("year").`type`(IntegerType).sql("YEAR(date)"),
        field.name("balance").`type`(DoubleType).min(10).max(1000),
        field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
        field.name("status").oneOf("open", "closed"),
        field.name("update_history")
          .`type`(ArrayType)
          .schema(
            field.name("updated_time").`type`(TimestampType).min(Timestamp.valueOf("2022-01-01 00:00:00")),
            field.name("prev_status").oneOf("open", "closed"),
            field.name("new_status").oneOf("open", "closed")
          ),
        field.name("customer_details")
          .schema(
            field.name("name").expression("#{Name.name}"),
            field.name("age").`type`(IntegerType).min(18).max(90),
            field.name("city").expression("#{Address.city}")
          ),
      )
      .count(count.records(10))

    val thirdJsonTask = json("my_thrid_json", "/tmp/data/third_json", Map("saveMode" -> "overwrite"))
      .schema(
        field.name("account_id"),
        field.name("amount").`type`(IntegerType).min(1).max(100),
        field.name("name").expression("#{Name.name}"),
      )
      .count(count.records(10))

    val secondJsonTask = json("my_json", "/tmp/data/second_json", Map("saveMode" -> "overwrite"))
      .schema(
        field.name("account_id"),
        field.name("amount").`type`(IntegerType).min(1).max(100),
        field.name("name").expression("#{Name.name}"),
      )
      .count(count.records(10).recordsPerColumn(3, "account_id"))
      .validations(
        validation.preFilter(columnPreFilter("name").isEqual("peter"))
          .col("amount").greaterThan(50),
        validation.preFilter(
          preFilterBuilder(columnPreFilter( "name").startsWith("john"))
            .and(columnPreFilter("amount").greaterThan(10))
        ).col("account_id").isNotNull,
        validation.columnNames.countEqual(3),
        validation.columnNames.countBetween(1, 2),
        validation.columnNames.matchOrder("account_id", "amount", "name"),
        validation.columnNames.matchSet("account_id", "my_name"),
        validation.upstreamData(firstJsonTask).joinColumns("account_id")
          .withValidation(validation.col("my_first_json_customer_details.name").isEqualCol("name")),
        validation.upstreamData(firstJsonTask).joinColumns("account_id")
          .withValidation(validation.col("amount").isNotEqualCol("my_first_json_balance")),
        validation.upstreamData(firstJsonTask).joinExpr("account_id == my_first_json_account_id")
          .withValidation(validation.groupBy("account_id", "my_first_json_balance").sum("amount").betweenCol("my_first_json_balance * 0.8", "my_first_json_balance * 1.2")),
        validation.upstreamData(firstJsonTask).joinColumns("account_id")
          .withValidation(validation.groupBy("account_id", "my_first_json_balance").sum("amount").betweenCol("my_first_json_balance * 0.8", "my_first_json_balance * 1.2")),
        validation.upstreamData(firstJsonTask).joinColumns("account_id").joinType("anti").withValidation(validation.count().isEqual(0)),
        validation.upstreamData(firstJsonTask).joinColumns("account_id").withValidation(validation.count().isEqual(30)),
        validation.upstreamData(firstJsonTask)
          .joinColumns("account_id")
          .withValidation(
            validation.upstreamData(thirdJsonTask)
              .joinColumns("account_id")
              .withValidation(validation.count().isEqual(30))
          )
      )

    val config = configuration
      .generatedReportsFolderPath("/tmp/report")
      .recordTrackingForValidationFolderPath("/tmp/record-tracking-validation")
      .enableValidation(true)
      .slackAlertToken(System.getenv("SLACK_TOKEN"))
      .slackAlertChannels("#data-testing")

    val foreignPlan = plan
      .addForeignKeyRelationship(firstJsonTask, "account_id", List(secondJsonTask -> "account_id", thirdJsonTask -> "account_id"))

    execute(foreignPlan, config, firstJsonTask, secondJsonTask, thirdJsonTask)
  }

  class TestOtherFileFormats extends PlanRun {
    val basicSchema = List(
      field.name("account_id").regex("ACC[0-9]{8}"),
      field.name("amount").`type`(IntegerType).min(1).max(1),
      field.name("name").expression("#{Name.name}"),
    )

//    val hudiTask = hudi("my_hudi", "/tmp/data/hudi", "accounts", Map("saveMode" -> "overwrite"))
//      .schema(basicSchema: _*)
//
    val deltaTask = delta("my_delta", "/tmp/data/delta", Map("saveMode" -> "overwrite"))
      .schema(basicSchema: _*)
//
//    val icebergTask = iceberg("my_iceberg", "/tmp/data/iceberg", "account.accounts", options = Map("saveMode" -> "overwrite"))
//      .schema(basicSchema: _*)

    execute(deltaTask)
  }

  class TestUniqueFields extends PlanRun {
    val jsonTask = json("my_first_json", "/tmp/data/unique_json", Map("saveMode" -> "overwrite"))
      .schema(
        field.name("account_id").regex("ACC[0-9]{8}").unique(true)
      )
    execute(jsonTask)
  }
}
