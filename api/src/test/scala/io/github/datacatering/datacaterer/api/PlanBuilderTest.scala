package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.Constants.{ALL_COMBINATIONS, FOREIGN_KEY_DELIMITER}
import io.github.datacatering.datacaterer.api.connection.FileBuilder
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, ExpressionValidation, ForeignKeyRelation, PauseWaitCondition}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PlanBuilderTest extends AnyFunSuite {

  test("Can create Plan") {
    val planBuilder = PlanBuilder()
    val name = "basic plan"
    val desc = "basic desc"
    val taskSummaries = TaskSummaryBuilder()
      .name("account_json_task")
      .dataSource("account_json")

    val result = planBuilder.name(name)
      .description(desc)
      .taskSummaries(taskSummaries)

    assert(result.plan.name == name)
    assert(result.plan.description == desc)
    assert(result.plan.tasks.size == 1)
    assert(result.plan.tasks.head == taskSummaries.taskSummary)
  }

  test("Can implement PlanRun") {
    val result: PlanRun = new PlanRun {
      val dataSourceName = "account_json"
      val t = tasks.addTask(
        "my task",
        dataSourceName,
        step.schema(schema.addFields(field.name("account_id")))
      )

      val p = plan.name("my plan")
        .seed(1)
        .locale("en")
        .addForeignKeyRelationship(
          new ForeignKeyRelation("account_json", "default_step", "account_id"),
          new ForeignKeyRelation("txn_db", "txn_step", "account_number")
        )
        .addForeignKeyRelationship(
          new ForeignKeyRelation("account_json", "default_step", "customer_number"),
          new ForeignKeyRelation("acc_db", "acc_step", "customer_number")
        )

      val c = configuration
        .addRuntimeConfig("spark.sql.shuffle.partitions" -> "2")
        .enableGeneratePlanAndTasks(true)
        .enableValidation(true)
        .addConnectionConfig(dataSourceName, "json", Map())
        .addConnectionConfig("txn_db", "postgres", Map())

      val v = validationConfig
        .name("account_validation")
        .description("account checks")
        .addDataSourceValidation(
          dataSourceName,
          dataSourceValidation
            .validations(
              validation
                .description("name is equal to Peter")
                .errorThreshold(0.1)
                .expr("name == 'Peter'")
            ).option(("path", "test/path/json"))
        )

      execute(List(t), p, c, List(v))
    }

    assert(result._tasks.size == 1)
    assert(result._tasks.head.name == "my task")
    assert(result._tasks.head.steps.head.schema.fields.get.head.name == "account_id")

    assert(result._plan.name == "my plan")
    assert(result._plan.tasks.size == 1)
    assert(result._plan.tasks.head.name == "my task")
    assert(result._plan.tasks.head.dataSourceName == "account_json")
    assert(result._plan.tasks.head.enabled)
    assert(result._plan.sinkOptions.get.seed.contains("1"))
    assert(result._plan.sinkOptions.get.locale.contains("en"))
    val fk = result._plan.sinkOptions.get.foreignKeys
    assert(fk.exists(f => f._1.equalsIgnoreCase(s"account_json${FOREIGN_KEY_DELIMITER}default_step${FOREIGN_KEY_DELIMITER}account_id")))
    assert(
      fk.find(f => f._1.equalsIgnoreCase(s"account_json${FOREIGN_KEY_DELIMITER}default_step${FOREIGN_KEY_DELIMITER}account_id")).get._2 ==
        List(s"txn_db${FOREIGN_KEY_DELIMITER}txn_step${FOREIGN_KEY_DELIMITER}account_number")
    )
    assert(fk.exists(f => f._1.equalsIgnoreCase(s"account_json${FOREIGN_KEY_DELIMITER}default_step${FOREIGN_KEY_DELIMITER}customer_number")))
    assert(
      fk.find(f => f._1.equalsIgnoreCase(s"account_json${FOREIGN_KEY_DELIMITER}default_step${FOREIGN_KEY_DELIMITER}customer_number")).get._2 ==
        List(s"acc_db${FOREIGN_KEY_DELIMITER}acc_step${FOREIGN_KEY_DELIMITER}customer_number")
    )

    assert(result._configuration.flagsConfig.enableCount)
    assert(result._configuration.flagsConfig.enableGenerateData)
    assert(!result._configuration.flagsConfig.enableRecordTracking)
    assert(!result._configuration.flagsConfig.enableDeleteGeneratedRecords)
    assert(result._configuration.flagsConfig.enableGeneratePlanAndTasks)
    assert(result._configuration.flagsConfig.enableFailOnError)
    assert(!result._configuration.flagsConfig.enableUniqueCheck)
    assert(!result._configuration.flagsConfig.enableSinkMetadata)
    assert(result._configuration.flagsConfig.enableSaveReports)
    assert(result._configuration.flagsConfig.enableValidation)
    assert(result._configuration.connectionConfigByName.size == 2)
    assert(result._configuration.connectionConfigByName.contains("account_json"))
    assert(result._configuration.connectionConfigByName("account_json") == Map("format" -> "json"))
    assert(result._configuration.connectionConfigByName.contains("txn_db"))
    assert(result._configuration.connectionConfigByName("txn_db") == Map("format" -> "postgres"))
    assert(result._configuration.runtimeConfig == DataCatererConfiguration().runtimeConfig ++ Map("spark.sql.shuffle.partitions" -> "2"))

    assert(result._validations.size == 1)
    assert(result._validations.head.dataSources.size == 1)
    val dataSourceHead = result._validations.head.dataSources.head
    assert(dataSourceHead._1 == "account_json")
    assert(dataSourceHead._2.size == 1)
    assert(dataSourceHead._2.head.validations.size == 1)
    val validationHead = dataSourceHead._2.head.validations.head.validation
    assert(validationHead.description.contains("name is equal to Peter"))
    assert(validationHead.errorThreshold.contains(0.1))
    assert(validationHead.isInstanceOf[ExpressionValidation])
    assert(validationHead.asInstanceOf[ExpressionValidation].expr == "name == 'Peter'")
    assert(dataSourceHead._2.head.options == Map("path" -> "test/path/json"))
    assert(dataSourceHead._2.head.waitCondition == PauseWaitCondition())
  }

  test("Can define random seed and locale that get used across all data generators") {
    val result = PlanBuilder().sinkOptions(SinkOptionsBuilder().locale("es").seed(1)).plan

    assert(result.sinkOptions.isDefined)
    assert(result.sinkOptions.get.locale.contains("es"))
    assert(result.sinkOptions.get.seed.contains("1"))
  }

  test("Can define foreign key via connection task builder") {
    val jsonTask = ConnectionConfigWithTaskBuilder().file("my_json", "json")
      .schema(FieldBuilder().name("account_id"))
    val csvTask = ConnectionConfigWithTaskBuilder().file("my_csv", "csv")
      .schema(FieldBuilder().name("account_id"))
    val result = PlanBuilder().addForeignKeyRelationship(
      jsonTask, List("account_id"),
      List(csvTask -> List("account_id"))
    ).plan

    assert(result.sinkOptions.isDefined)
    val fk = result.sinkOptions.get.foreignKeys
    assert(fk.nonEmpty)
    assert(fk.size == 1)
    assert(fk.exists(f => f._1.startsWith("my_json") && f._1.endsWith("account_id") &&
      f._2.size == 1 && f._2.head.startsWith("my_csv") && f._2.head.endsWith("account_id")
    ))

    val result2 = PlanBuilder().addForeignKeyRelationship(
      jsonTask, "account_id",
      List(csvTask -> "account_id")
    ).plan

    assert(result2.sinkOptions.isDefined)
    val fk2 = result2.sinkOptions.get.foreignKeys
    assert(fk2.nonEmpty)
    assert(fk2.size == 1)
  }

  test("Throw runtime exception when foreign key column is not defined in data sources") {
    val jsonTask = ConnectionConfigWithTaskBuilder().file("my_json", "json")
    val csvTask = ConnectionConfigWithTaskBuilder().file("my_csv", "csv")

    assertThrows[RuntimeException](PlanBuilder().addForeignKeyRelationship(
      jsonTask, List("account_id"),
      List(csvTask -> List("account_id"))
    ).plan)
  }

  test("Throw runtime exception when foreign key column is not defined in data sources with other columns") {
    val jsonTask = ConnectionConfigWithTaskBuilder().file("my_json", "json").schema(FieldBuilder().name("account_number"))
    val csvTask = ConnectionConfigWithTaskBuilder().file("my_csv", "csv").schema(FieldBuilder().name("account_type"))

    assertThrows[RuntimeException](PlanBuilder().addForeignKeyRelationship(
      jsonTask, List("account_id"),
      List(csvTask -> List("account_id"))
    ).plan)
  }

  test("Don't throw runtime exception when data source schema is defined from metadata source") {
    val jsonTask = ConnectionConfigWithTaskBuilder().file("my_json", "json").schema(MetadataSourceBuilder().openApi("localhost:8080"))
    val csvTask = ConnectionConfigWithTaskBuilder().file("my_csv", "csv").schema(MetadataSourceBuilder().openApi("localhost:8080"))
    val result = PlanBuilder().addForeignKeyRelationship(
      jsonTask, List("account_id"),
      List(csvTask -> List("account_id"))
    ).plan

    assert(result.sinkOptions.isDefined)
    val fk = result.sinkOptions.get.foreignKeys
    assert(fk.nonEmpty)
    assert(fk.size == 1)
  }

  test("Don't throw runtime exception when delete foreign key column, defined by SQL, is not defined in data sources") {
    val jsonTask = ConnectionConfigWithTaskBuilder().file("my_json", "json").schema(FieldBuilder().name("account_id"))
    val csvTask = ConnectionConfigWithTaskBuilder().file("my_csv", "csv").schema(FieldBuilder().name("account_number"))
    val result = PlanBuilder().addForeignKeyRelationship(
      jsonTask, List("account_id"),
      List(),
      List(csvTask -> List("account_id AS account_number"))
    ).plan

    assert(result.sinkOptions.isDefined)
    val fk = result.sinkOptions.get.foreignKeys
    assert(fk.nonEmpty)
    assert(fk.size == 1)
    assert(fk.head._2.isEmpty)
    assert(fk.head._3.size == 1)
  }

  test("Can create a step that will generate records for all combinations") {
    val jsonTask = ConnectionConfigWithTaskBuilder().file("my_json", "json")
      .allCombinations(true)

    assert(jsonTask.step.isDefined)
    assert(jsonTask.step.get.step.options.nonEmpty)
    assert(jsonTask.step.get.step.options.contains(ALL_COMBINATIONS))
    assert(jsonTask.step.get.step.options(ALL_COMBINATIONS).equalsIgnoreCase("true"))
  }
}
