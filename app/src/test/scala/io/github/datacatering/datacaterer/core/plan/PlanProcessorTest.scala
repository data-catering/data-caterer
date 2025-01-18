package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.Constants.{OPEN_METADATA_AUTH_TYPE_OPEN_METADATA, OPEN_METADATA_JWT_TOKEN, OPEN_METADATA_TABLE_FQN, PARTITIONS, ROWS_PER_SECOND, SAVE_MODE, VALIDATION_IDENTIFIER}
import io.github.datacatering.datacaterer.api.model.{ArrayType, DateType, DoubleType, HeaderType, IntegerType, MapType, TimestampType}
import io.github.datacatering.datacaterer.api.{HttpMethodEnum, PlanRun}
import io.github.datacatering.datacaterer.core.model.Constants.{DATA_CATERER_API_TOKEN, DATA_CATERER_API_USER, METADATA_FILTER_OUT_SCHEMA}
import io.github.datacatering.datacaterer.core.util.{ObjectMapperUtil, SparkSuite}
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import org.asynchttpclient.Dsl.asyncHttpClient
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import scala.compat.java8.FutureConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class PlanProcessorTest extends SparkSuite {

  private val scalaBaseFolder = "src/test/resources/sample/documentation"
  private val javaBaseFolder = "src/test/resources/sample/java/documentation"

  class DocumentationPlanRun extends PlanRun {
    {
      val accountStatus = List("open", "closed", "pending", "suspended")
      val jsonTask = json("account_info", s"$scalaBaseFolder/json", Map(SAVE_MODE -> "overwrite"))
        .fields(
          field.name("account_id").regex("ACC[0-9]{8}"),
          field.name("year").`type`(IntegerType).sql("YEAR(date)"),
          field.name("balance").`type`(DoubleType).min(10).max(1000).round(2),
          field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
          field.name("status").oneOf(accountStatus: _*),
          field.name("rand_map").`type`(MapType),
          field.name("update_history")
            .`type`(ArrayType)
            .fields(
              field.name("updated_time").`type`(TimestampType).min(Timestamp.valueOf("2022-01-01 00:00:00")),
              field.name("prev_status").oneOf(accountStatus: _*),
              field.name("new_status").oneOf(accountStatus: _*)
            ),
          field.name("customer_details")
            .fields(
              field.name("name").sql("_join_txn_name"),
              field.name("age").`type`(IntegerType).min(18).max(90),
              field.name("city").expression("#{Address.city}")
            ),
          field.name("_join_txn_name").expression("#{Name.name}").omit(true)
        )
        .count(count.records(100))

      val csvTxns = csv("transactions", s"$scalaBaseFolder/csv", Map(SAVE_MODE -> "overwrite", "header" -> "true"))
        .fields(
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
            .recordsPerFieldGenerator(generator.min(1).max(2), "account_id", "name")
        )
        .validationWait(waitCondition.pause(1))
        .validations(
          validation.expr("amount > 0").errorThreshold(0.01),
          validation.expr("LENGTH(name) > 3").errorThreshold(5),
          validation.expr("LENGTH(merchant) > 0").description("Non-empty merchant name"),
        )

      val foreignKeySetup = plan
        .addForeignKeyRelationship(
          jsonTask, List("account_id", "_join_txn_name"),
          List((csvTxns, List("account_id", "name")))
        )
      val conf = configuration
        .generatedReportsFolderPath(s"$scalaBaseFolder/report")
        .recordTrackingForValidationFolderPath(s"$scalaBaseFolder/record-tracking-validation")
        .enableValidation(true)
        .enableSinkMetadata(true)

      execute(foreignKeySetup, conf, jsonTask, csvTxns)
    }
  }

  test("Can run documentation plan run") {
    PlanProcessor.determineAndExecutePlan(Some(new DocumentationPlanRun()), apiCheck = false)
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
    assertResult(100)(jsonData.length)
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
    val docPlanRun = new TestValidation()
    val planWrite = ObjectMapperUtil.yamlObjectMapper.writeValueAsString(docPlanRun._validations)
    println(planWrite)
  }

  ignore("Can run Postgres plan run") {
    PlanProcessor.determineAndExecutePlan(Some(new TestRelationshipTask), apiCheck = false)
  }

  class TestPostgres extends PlanRun {
    val accountTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
      .table("account", "accounts")
      .fields(
        field.name("account_number").regex("[0-9]{10}").unique(true),
        field.name("customer_id_int").`type`(IntegerType).min(1).max(1000),
        field.name("created_by").expression("#{Name.name}"),
        field.name("created_by_fixed_length").sql("CASE WHEN account_status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
        field.name("open_timestamp").`type`(TimestampType).min(Date.valueOf(LocalDate.now())),
        field.name("account_status").oneOf("open", "closed", "suspended", "pending")
      )
      .count(count.records(100))

    val jsonTask = json("my_json", "/tmp/data/json", Map("saveMode" -> "overwrite"))
      .fields(
        field.name("account_id").regex("ACC[0-9]{8}"),
        field.name("name").expression("#{Name.name}"),
        field.name("amount").`type`(DoubleType).max(10),
      )
      .count(count.recordsPerField(2, "account_id", "name"))
      .validations(
        validation.groupBy("account_id", "name").max("amount").lessThan(100),
        validation.unique("account_id", "name"),
      )
    val csvTask = json("my_csv", "/tmp/data/csv", Map("saveMode" -> "overwrite"))
      .fields(
        field.name("account_number").regex("[0-9]{8}"),
        field.name("name").expression("#{Name.name}"),
        field.name("amount").`type`(DoubleType).max(10),
      )
      .validations(
        validation.field("account_number").isNull(true).description("account_number is a primary key"),
        validation.field("name").matches("[A-Z][a-z]+ [A-Z][a-z]+").errorThreshold(0.3).description("Some names follow a different pattern"),
      )

    val conf = configuration
      .generatedReportsFolderPath("/tmp/report")
      .enableSinkMetadata(true)

    execute(conf, accountTask)
  }

  class TestCsvPostgres extends PlanRun {
    val csvTask = csv("my_csv", "/tmp/data/csv", Map("saveMode" -> "overwrite", "header" -> "true"))
      .numPartitions(1)
      .fields(metadataSource.marquez("http://localhost:5001", "food_delivery", "public.delivery_7_days"))
      .count(count.records(10))

    val postgresTask = postgres("my_postgres", "jdbc:postgresql://localhost:5432/food_delivery", "postgres", "password")
      .fields(metadataSource.marquez("http://localhost:5001", "food_delivery"))
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

  class TestOpenMetadata extends PlanRun {
    val jsonTask = json("my_json", "/tmp/data/json", Map("saveMode" -> "overwrite"))
      .fields(metadataSource.openMetadata("http://localhost:8585/api", OPEN_METADATA_AUTH_TYPE_OPEN_METADATA,
        Map(
          OPEN_METADATA_JWT_TOKEN -> "abc123",
          OPEN_METADATA_TABLE_FQN -> "sample_data.ecommerce_db.shopify.dim_address"
        )))
      .fields(field.name("customer").fields(field.name("sex").oneOf("M", "F")))
      .count(count.records(10))

    val conf = configuration.enableGeneratePlanAndTasks(true)
      .enableGenerateValidations(true)
      .generatedReportsFolderPath("/tmp/report")

    execute(conf, jsonTask)
  }

  class TestOpenAPI extends PlanRun {
    val httpTask = http("my_http", options = Map(ROWS_PER_SECOND -> "5"))
      .fields(metadataSource.openApi("app/src/test/resources/sample/http/openapi/petstore.json"))
      .count(count.records(10))

    val httpGetTask = http("get_http", options = Map(VALIDATION_IDENTIFIER -> "GET/pets/{id}"))
      .validations(
        validation.field("request.method").isEqual("GET"),
        validation.field("request.method").isEqualField("response.statusText"),
        validation.field("response.statusCode").isEqual(200),
        validation.field("response.headers.Content-Length").greaterThan(0),
        validation.field("response.headers.Content-Type").isEqual("application/json"),
        validation.selectExpr("PERCENTILE(response.timeTakenMs, 0.2) AS pct20").expr("pct20 < 10"),
      )

    val conf = configuration.enableGeneratePlanAndTasks(true)
      .enableDeleteRecordTrackingFiles(false)
      .generatedReportsFolderPath("/tmp/report")
      .recordTrackingForValidationFolderPath("/tmp/record-tracking-validation")

    val myPlan = plan.addForeignKeyRelationship(
      foreignField("my_http", "POST/pets", "bodyContent.id"),
      foreignField("my_http", "DELETE/pets/{id}", "pathParamid"),
      foreignField("my_http", "GET/pets/{id}", "pathParamid"),
    )

    execute(myPlan, conf, httpTask, httpGetTask)
  }

  class TestSolace extends PlanRun {
    val solaceTask = solace("my_solace", "smf://localhost:55554")
      .destination("/JNDI/T/rest_test_topic")
      .fields(
        field.name("value").sql("TO_JSON(content)"),
        field.name("headers") //set message properties via headers field
          .`type`(HeaderType.getType)
          .sql(
            """ARRAY(
              |  NAMED_STRUCT('key', 'account-id', 'value', TO_BINARY(content.account_id, 'utf-8')),
              |  NAMED_STRUCT('key', 'name', 'value', TO_BINARY(content.name, 'utf-8'))
              |)""".stripMargin
          ),
        field.name("content").fields(
          field.name("account_id"),
          field.name("name").expression("#{Name.name}"),
          field.name("age").`type`(IntegerType),
        )
      )
      .count(count.records(10))

    execute(solaceTask)
  }

  class TestHttp extends PlanRun {
    val httpTask = http("my_http")
      .fields(metadataSource.openApi("app/src/test/resources/sample/http/openapi/petstore.json"))
      .fields(field.name("bodyContent").fields(field.name("id").regex("ID[0-9]{8}")))
      .count(count.records(20))

    val myPlan = plan.addForeignKeyRelationship(
      foreignField("my_http", "POST/pets", "bodyContent.id"),
      foreignField("my_http", "DELETE/pets/{id}", "pathParamid"),
      foreignField("my_http", "GET/pets/{id}", "pathParamid"),
    )

    val conf = configuration.enableGeneratePlanAndTasks(true)
      .recordTrackingForValidationFolderPath("/tmp/record-tracking-validation")
      .generatedReportsFolderPath("/tmp/report")

    execute(myPlan, conf, httpTask)
  }

  class TestBasicHttp extends PlanRun {
    val urlField = field.httpUrl(
      "http://localhost:80/anything/user/{id}",
      HttpMethodEnum.GET,
      List(field.httpPathParam("id").regex("ACC[0-9]{8}")),
      List(field.httpQueryParam("name").expression("#{Name.name}"))
    )
    val httpTask = http("my_http")
      .fields(urlField: _*)
      .fields(field.httpHeader("Content-Type").static("json"))
      .count(count.records(5))

    val conf = configuration.generatedReportsFolderPath("/tmp/report")

    execute(conf, httpTask)
  }

  class TestRelationshipTask extends PlanRun {
    val httpPostTask = http("post_http")
      .fields(field.httpHeader("Content-Type").static("application/json"))
      .fields(field.httpUrl(
        "http://localhost:80/anything/pets", //url
        HttpMethodEnum.POST //method
      ): _*)
      .fields(field.httpBody(
        field.name("id").regex("[0-9]{8}"),
        field.name("name").expression("#{Name.name}")
      ): _*)
      .count(count.records(20))
      .validations(
        validation.field("request.method").isEqual("POST"),
        validation.field("request.method").isEqualField("response.statusText"),
        validation.field("response.statusCode").isEqual(200),
        validation.field("response.headers.Content-Length").greaterThan(0),
        validation.field("response.headers.Content-Type").isEqual("application/json"),
      )

    val httpGetTask = http("get_http")
      .fields(
        field.httpHeader("Content-Type").static("application/json"),
      )
      .fields(field.httpUrl(
        "http://localhost:80/anything/pets/{id}", //url
        HttpMethodEnum.GET, //method
        List(
          field.name("id") //path parameters
        ),
        List(
          field.name("limit").`type`(IntegerType).min(1).max(10) //query parameters
        )
      ): _*)
      .validations(
        validation.field("request.method").isEqual("GET"),
        validation.field("request.method").isEqualField("response.statusText"),
        validation.field("response.statusCode").isEqual(200),
        validation.field("response.headers.Content-Length").greaterThan(0),
        validation.field("response.headers.Content-Type").isEqual("application/json"),
      )

    val myPlan = plan.addForeignKeyRelationship(
      foreignField(httpPostTask, "bodyContent.id"),
      foreignField(httpGetTask, "id")
    )

    val conf = configuration.enableGeneratePlanAndTasks(true)
      .enableDeleteRecordTrackingFiles(false)
      .generatedReportsFolderPath("/tmp/report")
      .recordTrackingForValidationFolderPath("/tmp/valid-track")

    execute(myPlan, conf, httpPostTask, httpGetTask)
  }

  class TestJson extends PlanRun {
    val jsonTask = json("my_json", "/tmp/data/json", Map("saveMode" -> "overwrite"))
      .fields(
        field.name("account_id").regex("ACC[0-9]{8}"),
        field.name("year").`type`(IntegerType).sql("YEAR(dates)"),
        field.name("balance").`type`(DoubleType).min(10).max(1000),
        field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
        field.name("status").sql("element_at(sort_array(update_history, false), 1).status"),
        field.name("update_history")
          .`type`(ArrayType)
          .arrayMinLength(1)
          .fields(
            field.name("updated_time").`type`(TimestampType).min(Timestamp.valueOf("2022-01-01 00:00:00")),
            field.name("status").oneOf("open", "closed")
          ),
        field.name("customer_details")
          .fields(
            field.name("name").expression("#{Name.name}"),
            field.name("age").`type`(IntegerType).min(18).max(90),
            field.name("city").expression("#{Address.city}")
          )
      )

    execute(jsonTask)
  }

  class TestValidationAndInnerSchemaFromMetadataSource extends PlanRun {
    val csvTask = json("my_big_json", "/tmp/data/big_json", Map("saveMode" -> "overwrite"))
      .fields(
        field.name("content").fields(metadataSource.openMetadata("http://localhost:8585/api", OPEN_METADATA_AUTH_TYPE_OPEN_METADATA,
          Map(
            OPEN_METADATA_JWT_TOKEN -> "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE2OTcxNzc0MzgsImV4cCI6bnVsbH0.jnO65SJZG9GQuVlJvpyKrrBZejPpjV71crJEvWOMPyeozZkoEyYy-kcb8UkVenidDcoAdie4Zhl4saNyaLudiAO2MKhSU1Rf3yT2M3BQBf37kQ3Ma4pjrx-lXVk2SmCaHsgLFETksSHZTwgPrtx5L3d2FOCfF92dANI_tldTg5Jog51tjHyYWYV4y4_eU4AfC7gXjIhvU35vTJmzUWH7BUkDGfcwHnIVa0AOqLzwZUQT1S717yNoenj2CUTBNS4fxWlATWBQIMG9JaBmQAAYNWOFPKnVWfWGv7Ya1OEW5wtb7A69hyPAT1lS-_FIxOOMkGbdg2u3sFuu9rD1d2JMdg",
            OPEN_METADATA_TABLE_FQN -> "sample_data.ecommerce_db.shopify.dim_address"
          ))),
        field.name("account_id"),
        field.name("year").`type`(IntegerType).min(2020).max(2023),
      )
      .count(count.records(10))

    val jsonTask = json("my_json", "/tmp/data/json")
      .validations(
        validation.field("customer_details.name").matches("[A-Z][a-z]+ [A-Z][a-z]+").errorThreshold(0.1).description("Names generally follow the same pattern"),
        validation.field("date").isNull(true).errorThreshold(10),
        validation.field("balance").greaterThan(500),
        validation.expr("YEAR(date) == year"),
        validation.field("status").in("open", "closed", "pending").errorThreshold(0.2).description("Could be new status introduced"),
        validation.field("customer_details.age").greaterThan(18),
        validation.expr("FORALL(update_history, x -> x.updated_time > TIMESTAMP('2022-01-01 00:00:00'))"),
        validation.unique("account_id"),
        validation.groupBy().count().isEqual(1000),
        validation.groupBy("account_id").max("balance").lessThan(900),
        validation.upstreamData(csvTask).validations(validation.field("amount").isEqualField("balance")),
      )
      .enableDataGeneration(false)

    val config = configuration
      .generatedReportsFolderPath("/tmp/report")
      .recordTrackingForValidationFolderPath("/tmp/record-tracking")
      .enableValidation(true)
      .enableGenerateValidations(true)
      .enableGeneratePlanAndTasks(true)

    execute(config, csvTask, jsonTask)
  }

  class TestValidation extends PlanRun {
    val firstJsonTask = json("my_first_json", "/tmp/data/first_json", Map("saveMode" -> "overwrite"))
      .fields(
        field.name("account_id").regex("ACC[0-9]{8}"),
        field.name("year").`type`(IntegerType).sql("YEAR(date)"),
        field.name("balance").`type`(DoubleType).min(10).max(1000),
        field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
        field.name("status").oneOf("open", "closed"),
        field.name("update_history")
          .`type`(ArrayType)
          .fields(
            field.name("updated_time").`type`(TimestampType).min(Timestamp.valueOf("2022-01-01 00:00:00")),
            field.name("prev_status").oneOf("open", "closed"),
            field.name("new_status").oneOf("open", "closed")
          ),
        field.name("customer_details")
          .fields(
            field.name("name").expression("#{Name.name}"),
            field.name("age").`type`(IntegerType).min(18).max(90),
            field.name("city").expression("#{Address.city}")
          ),
      )
      .count(count.records(10))

    val thirdJsonTask = json("my_thrid_json", "/tmp/data/third_json", Map("saveMode" -> "overwrite"))
      .fields(
        field.name("account_id"),
        field.name("amount").`type`(IntegerType).min(1).max(100),
        field.name("name").expression("#{Name.name}"),
      )
      .count(count.records(10))

    val secondJsonTask = json("my_json", "/tmp/data/second_json", Map("saveMode" -> "overwrite"))
      .fields(
        field.name("account_id"),
        field.name("amount").`type`(IntegerType).min(1).max(100),
        field.name("name").expression("#{Name.name}"),
      )
      .count(count.records(10).recordsPerField(3, "account_id"))
      .validations(
        validation.field("account_id").isNull(true),
        validation.field("amount").quantileValuesBetween(Map(0.1 -> (1.0 -> 10.0))),
        validation.groupBy("account_id").count().isEqual(1),
        validation.fieldNames.countEqual(3),
        validation.fieldNames.countBetween(1, 2),
        validation.fieldNames.matchOrder("account_id", "amount", "name"),
        validation.fieldNames.matchSet("account_id", "my_name"),
        validation.upstreamData(firstJsonTask).joinFields("account_id")
          .validations(
            validation.field("my_first_json_customer_details.name").isEqualField("name"),
            validation.field("amount").isEqualField("my_first_json_balance", true),
            validation.groupBy("account_id", "my_first_json_balance").sum("amount").betweenFields("my_first_json_balance * 0.8", "my_first_json_balance * 1.2"),
            validation.count().isEqual(30),
            validation.upstreamData(thirdJsonTask)
              .joinFields("account_id")
              .validations(validation.count().isEqual(30))
          ),
        validation.upstreamData(firstJsonTask).joinExpr("account_id == my_first_json_account_id")
          .validations(validation.groupBy("account_id", "my_first_json_balance").sum("amount").betweenFields("my_first_json_balance * 0.8", "my_first_json_balance * 1.2")),
        validation.upstreamData(firstJsonTask).joinFields("account_id").joinType("anti").validations(validation.count().isEqual(0)),
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

  class TestGreatExpectations extends PlanRun {
    val myJson = json("my_json", "/tmp/data/ge_json")
      .fields(
        field.name("vendor_id"),
        field.name("pickup_datetime").`type`(TimestampType),
        field.name("dropoff_datetime").`type`(TimestampType),
        field.name("passenger_count").`type`(IntegerType),
        field.name("trip_distance").`type`(DoubleType),
        field.name("rate_code_id"),
        field.name("store_and_fwd_flag"),
        field.name("pickup_location_id"),
        field.name("dropoff_location_id"),
        field.name("payment_type"),
        field.name("fare_amount").`type`(DoubleType),
        field.name("extra"),
        field.name("mta_tax").`type`(DoubleType),
        field.name("tip_amount").`type`(DoubleType),
        field.name("tolls_amount").`type`(DoubleType),
        field.name("improvement_surcharge").`type`(DoubleType),
        field.name("total_amount").`type`(DoubleType),
        field.name("congestion_surcharge").`type`(DoubleType),
      )
      .validations(metadataSource.greatExpectations("app/src/test/resources/sample/validation/great-expectations/taxi-expectations.json"))

    val config = configuration.enableGenerateValidations(true)
      .generatedReportsFolderPath("/tmp/report")
      .generatedReportsFolderPath("/tmp/app/data/report")
      .recordTrackingFolderPath("/tmp/record-tracking")
      .recordTrackingForValidationFolderPath("/tmp/record-tracking-validation")

    execute(config, myJson)
  }

  class AdvancedMySqlPlanRun extends PlanRun {

    val accountTask = mysql(
      "customer_mysql",
      "jdbc:mysql://localhost:3306/customer",
      "root", "root",
      Map(METADATA_FILTER_OUT_SCHEMA -> "datahub")
    )
      .fields(field.name("account_number").regex("[0-9]{10}"))
      .count(count.records(10))

    val config = configuration
      .generatedReportsFolderPath("/tmp/app/data/report")
      .recordTrackingFolderPath("/tmp/record-tracking")
      .recordTrackingForValidationFolderPath("/tmp/record-tracking-validation")
      .enableGeneratePlanAndTasks(true)
      .enableRecordTracking(true)
      .enableDeleteGeneratedRecords(false)
      .enableGenerateData(true)

    execute(config, accountTask)
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
    //    val deltaTask = delta("my_delta", "/tmp/data/delta", Map("saveMode" -> "overwrite"))
    //      .schema(basicSchema: _*)
    //
    val icebergTask = iceberg("my_iceberg", "/tmp/data/iceberg", "account.accounts", options = Map("saveMode" -> "overwrite"))
      .fields(basicSchema: _*)

    execute(icebergTask)
  }

  class TestUniqueFields extends PlanRun {
    val jsonTask = json("my_first_json", "/tmp/data/unique_json", Map("saveMode" -> "overwrite"))
      .fields(
        field.name("account_id").regex("ACC[0-9]{8}").unique(true)
      )
    execute(jsonTask)
  }

  class TestDeleteViaForeignKey extends PlanRun {

    val accountTask = json("customer_json", "/tmp/data/generate-account-json", Map(PARTITIONS -> "1", SAVE_MODE -> "overwrite"))
      .fields(
        field.name("account_number").regex("[0-9]{10}"),
        field.name("year").`type`(IntegerType).min(2020).max(2024),
        field.name("name").expression("#{Name.name}"),
      )
      .count(count.records(10))

    val accountEvents = json("customer_event_json", "/tmp/data/generate-event-json", Map(PARTITIONS -> "1", SAVE_MODE -> "overwrite"))
      .fields(
        field.name("account_number").omit(true),
        field.name("account_id").sql("CONCAT('ACC', account_number)"),
        field.name("year").`type`(IntegerType),
        field.name("name"),
        field.name("status").oneOf("open", "closed"),
      )
      .count(count.records(10))

    val generateConfig = configuration
      .generatedReportsFolderPath("/tmp/app/data/report")
      .recordTrackingFolderPath("/tmp/record-tracking")
      .recordTrackingForValidationFolderPath("/tmp/record-tracking-validation")
      .enableRecordTracking(true)
      .enableDeleteGeneratedRecords(false)
      .enableGenerateData(true)
    val deleteConfig = configuration
      .generatedReportsFolderPath("/tmp/app/data/report")
      .recordTrackingFolderPath("/tmp/record-tracking")
      .recordTrackingForValidationFolderPath("/tmp/record-tracking-validation")
      .enableRecordTracking(true)
      .enableDeleteGeneratedRecords(true)
      .enableGenerateData(false)

    /*
     * 1. Run with `generatePlan` with `generateConfig`
     * 2. Run rm -rf /tmp/record-tracking/json/customer_event_json
     * 2. Run with `deletePlan` with `deleteConfig`
     */
    val generatePlan = plan.addForeignKeyRelationship(accountTask, List("account_number"), List((accountEvents, List("account_number"))))
    val deletePlan = plan.addForeignKeyRelationship(accountTask, List("account_number"), List(), List((accountEvents, List("CONCAT('ACC', account_number) AS account_id"))))

    //    execute(generatePlan, generateConfig, accountTask, accountEvents)
    execute(deletePlan, deleteConfig, accountTask, accountEvents)
  }

  class TestSchemaFromODCS extends PlanRun {
    val accounts = csv("customer_csv", "/tmp/data/odcs-csv", Map("header" -> "true"))
      .fields(metadataSource.openDataContractStandard("app/src/test/resources/sample/metadata/odcs/full-example.odcs.yaml"))
      .fields(
        field.name("rcvr_id").regex("RC[0-9]{8}"),
        field.name("rcvr_cntry_code").oneOf("AU", "US", "TW")
      )
      .count(count.records(100))

    val conf = configuration.enableGeneratePlanAndTasks(true).generatedPlanAndTaskFolderPath("/tmp/data-caterer-gen")

    execute(conf, accounts)
  }

  class TestSchemaFromDataContractCli extends PlanRun {
    val accounts = csv("customer_csv", "/tmp/data/datacontract-cli-csv", Map("header" -> "true"))
      .fields(metadataSource.dataContractCli("app/src/test/resources/sample/metadata/datacontractcli/datacontract.yaml"))
      .fields(
        field.name("rcvr_id").regex("RC[0-9]{8}"),
        field.name("rcvr_cntry_code").oneOf("AU", "US", "TW")
      )
      .count(count.records(100))

    val conf = configuration.enableGeneratePlanAndTasks(true).generatedPlanAndTaskFolderPath("/tmp/data-caterer-gen")

    execute(conf, accounts)
  }

  class TestSchemaFromConfluentSchemaRegistry extends PlanRun {
    val accounts = kafka("customer_kafka", "localhost:9092")
      .topic("accounts")
      .fields(metadataSource.confluentSchemaRegistry("http://localhost:8081", 1))
      .count(count.records(3))

    val conf = configuration.enableGeneratePlanAndTasks(true).generatedPlanAndTaskFolderPath("/tmp/data-caterer-gen")

    execute(conf, accounts)
  }

  ignore("Check fail status") {
    System.setProperty(DATA_CATERER_API_USER, "")
    System.setProperty(DATA_CATERER_API_TOKEN, "")
    PlanProcessor.determineAndExecutePlan(Some(new TestFailedValidation))
  }

  class TestFailedGeneration extends PlanRun {
    val accounts = json("customer_json", "/tmp/failed_gen")
      .fields(field.name("name"), field.name("age").sql("invalid_field_name"))
      .count(count.records(1))

    execute(accounts)
  }

  class TestFailedValidation extends PlanRun {
    val accounts = postgres("customer_json", "/tmp/failed_gen", "username", "password")
      .fields(field.name("name"))
      .validations(validation.unique("name2"))
      .count(count.records(1))

    val conf = configuration
      .enableGenerateData(false)
      .enableValidation(true)
    execute(conf, accounts)
  }

  ignore("Timing of http calls") {
    val config = new DefaultAsyncHttpClientConfig.Builder()
      .setRequestTimeout(5000).build()
    val client = asyncHttpClient(config)
    (0 to 10).foreach(i => {
      val req = client.prepare("GET", s"http://localhost:80/anything/pets/LRCnF8780Ie563kPzOj/$i").build()
      val startTime = Timestamp.from(Instant.now())
      val futureResp = client.executeRequest(req)
        .toCompletableFuture
        .toScala
        .map(r => {
          val endTime = Timestamp.from(Instant.now())
          println("Time taken: " + {
            endTime.getTime - startTime.getTime
          } + "ms")
          r.getStatusCode
        })
      Await.result(futureResp, Duration.Inf)
    })
  }
}
