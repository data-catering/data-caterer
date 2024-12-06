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

    assertResult(name)(result.plan.name)
    assertResult(desc)(result.plan.description)
    assertResult(1)(result.plan.tasks.size)
    assertResult(taskSummaries.taskSummary)(result.plan.tasks.head)
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

    assertResult(1)(result._tasks.size)
    assertResult("my task")(result._tasks.head.name)
    assertResult("account_id")(result._tasks.head.steps.head.schema.fields.get.head.name)

    assertResult("my plan")(result._plan.name)
    assertResult(1)(result._plan.tasks.size)
    assertResult("my task")(result._plan.tasks.head.name)
    assertResult("account_json")(result._plan.tasks.head.dataSourceName)
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
    assertResult(2)(result._configuration.connectionConfigByName.size)
    assert(result._configuration.connectionConfigByName.contains("account_json"))
    assertResult(Map("format" -> "json"))(result._configuration.connectionConfigByName("account_json"))
    assert(result._configuration.connectionConfigByName.contains("txn_db"))
    assertResult(Map("format" -> "postgres"))(result._configuration.connectionConfigByName("txn_db"))
    assertResult(DataCatererConfiguration().runtimeConfig ++ Map("spark.sql.shuffle.partitions" -> "2"))(result._configuration.runtimeConfig)

    assertResult(1)(result._validations.size)
    assertResult(1)(result._validations.head.dataSources.size)
    val dataSourceHead = result._validations.head.dataSources.head
    assertResult("account_json")(dataSourceHead._1)
    assertResult(1)(dataSourceHead._2.size)
    assertResult(1)(dataSourceHead._2.head.validations.size)
    val validationHead = dataSourceHead._2.head.validations.head.validation
    assert(validationHead.description.contains("name is equal to Peter"))
    assert(validationHead.errorThreshold.contains(0.1))
    assert(validationHead.isInstanceOf[ExpressionValidation])
    assertResult("name == 'Peter'")(validationHead.asInstanceOf[ExpressionValidation].expr)
    assertResult(Map("path" -> "test/path/json"))(dataSourceHead._2.head.options)
    assertResult(PauseWaitCondition())(dataSourceHead._2.head.waitCondition)
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
    assertResult(1)(fk.size)
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
    assertResult(1)(fk2.size)
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
    assertResult(1)(fk.size)
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
    assertResult(1)(fk.size)
    assert(fk.head._2.isEmpty)
    assertResult(1)(fk.head._3.size)
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
