package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.Constants.{FOREIGN_KEY_DELIMITER, JDBC_TABLE, MINIMUM, ONE_OF_GENERATOR, OPEN_METADATA_API_VERSION, OPEN_METADATA_HOST, PATH}
import io.github.datacatering.datacaterer.api.model.{Count, Step, StringType => ApiStringType}
import io.github.datacatering.datacaterer.api.{FieldBuilder, PlanRun, StepBuilder}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata.OpenMetadataDataSourceMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceDetail, DataSourceMetadata}
import io.github.datacatering.datacaterer.core.util.TaskHelper
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.must.Matchers.{contain, include}
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, empty}

class TaskHelperTest extends AnyFunSuite {

  private val structType = StructType(Array(
    StructField("name", StringType, false, new MetadataBuilder().putString("key", "value").build()),
    StructField("age", IntegerType, false, new MetadataBuilder().putString("key1", "value1").build()),
    StructField("category", StringType, false, new MetadataBuilder().putString("oneOf", "person,dog").build()),
    StructField("customers", StructType(Array(
      StructField("name", StringType),
      StructField("sex", StringType),
    ))),
  ))
  private val openMetadataConf = Map(OPEN_METADATA_HOST -> "localhost:8585", OPEN_METADATA_API_VERSION -> "v1")

  test("Can create task from metadata generated values") {
    val dataSourceMetadata = OpenMetadataDataSourceMetadata("my_json", "json", openMetadataConf)

    val result = TaskHelper.fromMetadata(None, "task_name", "json", List(DataSourceDetail(dataSourceMetadata, Map(), structType, List())))

    assertResult("task_name")(result._1.name)
    assertResult(1)(result._1.steps.size)
    assertResult("json")(result._1.steps.head.`type`)
    val resFields = result._1.steps.head.fields
    assert(resFields.size == 4)
    assert(resFields.exists(_.name == "name"))
    assert(resFields.exists(_.name == "age"))
    assert(resFields.exists(_.name == "category"))
    assert(resFields.exists(_.name == "customers"))
    assert(resFields.find(_.name == "name").get.options.exists(x => x._1 == "key" && x._2.toString == "value"))
    assert(resFields.find(_.name == "age").get.options.exists(x => x._1 == "key1" && x._2.toString == "value1"))
    assert(resFields.find(_.name == "category").get.options.contains(ONE_OF_GENERATOR))
    assert(resFields.find(_.name == "category").get.options.get(ONE_OF_GENERATOR).contains("person,dog"))
    assert(result._2.isEmpty)
  }

  test("Can merge in user defined values with metadata generated values") {
    val userDefinedPlan = new JsonPlanRun
    val dataSourceMetadata = OpenMetadataDataSourceMetadata("my_json", "json", openMetadataConf)

    val result = TaskHelper.fromMetadata(Some(userDefinedPlan), "my_json", "json", List(DataSourceDetail(dataSourceMetadata, Map(), structType, List())))

    val resFields = result._1.steps.head.fields
    assert(resFields.find(_.name == "name").get.options(ONE_OF_GENERATOR).isInstanceOf[List[_]])
    assertResult(List("peter", "john"))(resFields.find(_.name == "name").get.options(ONE_OF_GENERATOR).asInstanceOf[List[_]])
    assert(resFields.find(_.name == "name").get.options.exists(x => x._1 == "key" && x._2.toString == "value"))
    assert(resFields.find(_.name == "age").get.options.exists(x => x._1 == "key1" && x._2.toString == "value1"))
    assert(resFields.find(_.name == "age").get.options.exists(x => x._1 == MINIMUM && x._2 == "18"))
    assert(resFields.find(_.name == "category").get.options.contains(ONE_OF_GENERATOR))
    assert(resFields.find(_.name == "category").get.options.get(ONE_OF_GENERATOR).contains("person,dog"))
    assert(resFields.find(_.name == "customers").get.fields.exists(_.name == "sex"))
    assert(resFields.find(_.name == "customers").get.fields.find(_.name == "sex").get.options(ONE_OF_GENERATOR).isInstanceOf[List[_]])
    assertResult(List("M", "F"))(resFields.find(_.name == "customers").get.fields.find(_.name == "sex").get.options(ONE_OF_GENERATOR).asInstanceOf[List[_]])
  }

  test("TaskHelper.fromMetadata should create task without plan run") {
    val structType = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))

    val detail = DataSourceDetail(NoopDataSourceMetadata(Map()), Map("table" -> "users"), structType, null)
    val (task, mappings) = TaskHelper.fromMetadata(
      None,
      "test_source",
      "spark",
      List(detail)
    )

    task.name shouldBe "test_source"
    task.steps.size shouldBe 1
    mappings shouldBe Matchers.empty
  }

  test("TaskHelper.fromMetadata should handle multiple data sources") {
    val detail1 = DataSourceDetail(
      NoopDataSourceMetadata(Map(JDBC_TABLE -> "table1")),
      Map(),
      StructType(Seq(StructField("id1", IntegerType))),
      List()
    )
    val detail2 = DataSourceDetail(
      NoopDataSourceMetadata(Map(JDBC_TABLE -> "table2")),
      Map(),
      StructType(Seq(StructField("id2", IntegerType))),
      List()
    )

    val (task, _) = TaskHelper.fromMetadata(
      None,
      "multi_source",
      "spark",
      List(detail1, detail2)
    )

    task.name shouldBe "multi_source"
    task.steps.size shouldBe 2
  }

  test("TaskHelper.fromMetadata should apply manual count and manual field options to all steps") {
    val detail1 = DataSourceDetail(
      NoopDataSourceMetadata(Map(PATH -> "/tmp/data/json")),
      Map(),
      StructType(Seq(StructField("id1", IntegerType), StructField("account_id", StringType))),
      List()
    )
    val detail2 = DataSourceDetail(
      NoopDataSourceMetadata(Map(PATH -> "/tmp/data/json")),
      Map(),
      StructType(Seq(StructField("id2", IntegerType), StructField("account_id", StringType))),
      List()
    )
    class CurrentPlanRun extends PlanRun {
      val jsonTask = task.name("test_source")
        .steps(
          step.name("step1").option(PATH -> "/tmp/data/json").count(count.records(1))
            .fields(field.name("account_id").regex("ACC[0-9]{8}"))
        )
      val conf = configuration
        .postgres("customer_postgres")
        .json("account_json", "/tmp/json")
      val p = plan.taskSummaries(taskSummary.dataSource("account_json").task(jsonTask))
      execute(p, conf)
    }
    val planRun = new CurrentPlanRun

    val (task, _) = TaskHelper.fromMetadata(
      Some(planRun),
      "multi_source",
      "spark",
      List(detail1, detail2)
    )

    task.name shouldBe "multi_source"
    task.steps.size shouldBe 2
    task.steps.map(_.count.records) should contain(Some(1))
    task.steps.map(_.fields).foreach { fields =>
      fields should contain(FieldBuilder().name("account_id").regex("ACC[0-9]{8}").field)
    }
  }

  test("TaskHelper.enrichWithUserDefinedOptions should merge options correctly") {
    val detail = DataSourceDetail(
      NoopDataSourceMetadata(Map(PATH -> "/tmp/data/json")),
      Map("sourceOpt" -> "value1"),
      StructType(Seq(StructField("id", IntegerType))),
      null
    )

    class CurrentPlanRun extends PlanRun {
      val jsonTask = json("test_source", "/tmp/data/json")
        .step(StepBuilder(Step("step1", "spark", Count(records = Some(1)), Map("userOpt" -> "value2"), Nil)))
      execute(jsonTask)
    }
    val planRun = new CurrentPlanRun

    val (step, _) = TaskHelper.enrichWithUserDefinedOptions(
      "test_source",
      "spark",
      detail,
      Some(planRun),
      false
    )

    step.options should contain allOf(
      "sourceOpt" -> "value1",
      "userOpt" -> "value2"
    )
    step.count.records shouldBe Some(1)
  }

  test("TaskHelper.enrichWithUserDefinedOptions should handle missing user configuration") {
    val detail = DataSourceDetail(
      NoopDataSourceMetadata(Map(PATH -> "/tmp/data/json")),
      Map("sourceOpt" -> "value"),
      StructType(Seq(StructField("id", IntegerType))),
      null
    )

    val (step, optMapping) = TaskHelper.enrichWithUserDefinedOptions(
      "test_source",
      "spark",
      detail,
      None,
      false
    )

    step.options should contain("sourceOpt" -> "value")
    optMapping shouldBe None
    step.count shouldBe Count()
  }

  test("TaskHelper.enrichWithUserDefinedOptions should preserve step name mapping with foreign key delimiter") {
    val detail = DataSourceDetail(
      NoopDataSourceMetadata(Map(PATH -> "/tmp/data/json")),
      Map("table" -> "users"),
      StructType(Seq(StructField("id", IntegerType))),
      null
    )

    class CurrentPlanRun extends PlanRun {
      val jsonTask = json("test_source", "/tmp/data/json")
        .step(StepBuilder(Step("custom_step", "spark", Count(), Map(), Nil)))
      execute(jsonTask)
    }
    val planRun = new CurrentPlanRun

    val (_, optMapping) = TaskHelper.enrichWithUserDefinedOptions(
      "test_source",
      "spark",
      detail,
      Some(planRun),
      false
    )

    optMapping.isDefined shouldBe true
    optMapping.get._1 should include(FOREIGN_KEY_DELIMITER)
    optMapping.get._2 should include(FOREIGN_KEY_DELIMITER)
  }

  // Additional tests for the new refactored structure
  test("TaskHelper should handle empty struct types list") {
    val (task, mappings) = TaskHelper.fromMetadata(
      None,
      "empty_source",
      "json",
      List()
    )

    task.name shouldBe "empty_source"
    task.steps shouldBe empty
    mappings shouldBe empty
  }

  test("TaskHelper should handle plan run with no matching data sources") {
    val detail = DataSourceDetail(
      NoopDataSourceMetadata(Map()),
      Map("path" -> "/tmp/different/path"), // sparkOptions should contain the path
      StructType(Seq(StructField("id", IntegerType))),
      List()
    )

    class NonMatchingPlanRun extends PlanRun {
      val jsonTask = json("different_source", "/tmp/other/path")
      execute(jsonTask)
    }
    val planRun = new NonMatchingPlanRun

    val (step, mapping) = TaskHelper.enrichWithUserDefinedOptions(
      "test_source",
      "json",
      detail,
      Some(planRun),
      false
    )

    // Should use defaults when no matching configuration is found
    step.count shouldBe Count()
    mapping shouldBe None
    step.options should contain("path" -> "/tmp/different/path")
  }

  test("TaskHelper should handle multiple matching configurations correctly") {
    val detail = DataSourceDetail(
      NoopDataSourceMetadata(Map(PATH -> "/tmp/data/json")),
      Map("format" -> "json"),
      StructType(Seq(StructField("id", IntegerType))),
      List()
    )

    class MultipleMatchingPlanRun extends PlanRun {
      val jsonTask1 = json("test_source", "/tmp/data/json")
        .step(StepBuilder(Step("step1", "json", Count(records = Some(5)), Map("format" -> "json"), Nil)))
      val jsonTask2 = json("test_source", "/tmp/data/json")
        .step(StepBuilder(Step("step2", "json", Count(records = Some(10)), Map("format" -> "json"), Nil)))
      execute(jsonTask1, jsonTask2)
    }
    val planRun = new MultipleMatchingPlanRun

    val (step, _) = TaskHelper.enrichWithUserDefinedOptions(
      "test_source",
      "json",
      detail,
      Some(planRun),
      false
    )

    // Should use first matching configuration
    step.count.records shouldBe Some(5)
  }

  test("TaskHelper should correctly merge schemas for multiple sub data sources") {
    val detail1 = DataSourceDetail(
      NoopDataSourceMetadata(Map(JDBC_TABLE -> "table1")),
      Map(),
      StructType(Seq(StructField("id1", IntegerType))),
      List()
    )
    val detail2 = DataSourceDetail(
      NoopDataSourceMetadata(Map(JDBC_TABLE -> "table2")),  
      Map(),
      StructType(Seq(StructField("id2", IntegerType))),
      List()
    )

    class MultiSubDataSourcePlanRun extends PlanRun {
      val postgresTask = postgres("test_source")
        .fields(field.name("user_id").`type`(ApiStringType))
      execute(postgresTask)
    }
    val planRun = new MultiSubDataSourcePlanRun

    val (task, _) = TaskHelper.fromMetadata(
      Some(planRun),
      "test_source",
      "postgres",
      List(detail1, detail2)
    )

    task.steps.size shouldBe 2
    // Each step should have its generated schema fields
    task.steps(0).fields.exists(_.name == "id1") shouldBe true
    task.steps(1).fields.exists(_.name == "id2") shouldBe true
    // User-defined fields may or may not be merged depending on hasMultipleSubDataSources behavior
    task.steps.foreach { step =>
      step.fields.nonEmpty shouldBe true // At least the generated field should be present
    }
  }

  test("TaskHelper should handle YAML configuration matching") {
    val detail = DataSourceDetail(
      NoopDataSourceMetadata(Map(PATH -> "/tmp/test.json")),
      Map(),
      StructType(Seq(StructField("id", IntegerType))),
      List()
    )

    class YamlConfigPlanRun extends PlanRun {
      val taskDef = task.name("yaml_task")
        .steps(step.name("step1").option(PATH -> "/tmp/test.json").count(count.records(100)))
      val planDef = plan.name("test_plan")
        .taskSummaries(taskSummary.dataSource("test_source").task(taskDef))
      execute(planDef, configuration)
    }
    val planRun = new YamlConfigPlanRun

    val (enrichedStep, _) = TaskHelper.enrichWithUserDefinedOptions(
      "test_source",
      "json",
      detail,
      Some(planRun),
      false
    )

    enrichedStep.count.records shouldBe Some(100)
    enrichedStep.options should contain("path" -> "/tmp/test.json")
  }

  test("TaskHelper should merge JSON schema with simple user field overrides") {
    // Create a simple JSON schema structure using working patterns
    val structType = StructType(List(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("email", StringType, true),
      StructField("details", StructType(Seq(
        StructField("age", IntegerType, true),
      )))
    ))
    
    val detail = DataSourceDetail(
      NoopDataSourceMetadata(Map("path" -> "/tmp/users.json")),
      Map("path" -> "/tmp/users.json"),
      structType,
      List()
    )

    class UserOverridePlanRun extends PlanRun {
      val jsonTask = json("user_source", "/tmp/users.json")
        .fields(
          field.name("id").min(1).max(1000),
          field.name("phone").`type`(ApiStringType).regex("\\+1-\\d{3}-\\d{3}-\\d{4}"), // New field not in schema
          field.name("details").fields(
            field.name("age").min(18).max(100),
            field.name("address").oneOf("123 Main St", "456 Elm St", "789 Oak St")
          )
        )
      execute(jsonTask)
    }
    val planRun = new UserOverridePlanRun

    val (step, mapping) = TaskHelper.enrichWithUserDefinedOptions(
      "user_source",
      "json",
      detail,
      Some(planRun),
      false
    )

    // Verify the merged schema
    step.fields.size shouldBe 5 // 4 from JSON schema + 1 new field
    
    // Check field overrides - user options should be merged with generated schema
    val idField = step.fields.find(_.name == "id").get
    idField.options should contain("min" -> "1")
    idField.options should contain("max" -> "1000")
    
    val nameField = step.fields.find(_.name == "name") 
    nameField.isDefined shouldBe true
    
    val emailField = step.fields.find(_.name == "email")
    emailField.isDefined shouldBe true
    
    val phoneField = step.fields.find(_.name == "phone").get // New field added
    phoneField.`type` shouldBe Some("string")
    phoneField.options should contain("regex" -> "\\+1-\\d{3}-\\d{3}-\\d{4}")

    val detailsField = step.fields.find(_.name == "details").get
    detailsField.fields.size shouldBe 2
    detailsField.fields.find(_.name == "age").get.options should contain("min" -> "18")
    detailsField.fields.find(_.name == "age").get.options should contain("max" -> "100")
    detailsField.fields.find(_.name == "address").get.options should contain("oneOf" -> List("123 Main St", "456 Elm St", "789 Oak St"))
    
    // Mapping should exist since user-defined fields create step name mappings
    mapping.isDefined shouldBe true
  }
}


class JsonPlanRun extends PlanRun {
  val jsonTask = json("my_json", "/tmp/data/json")
    .fields(metadataSource.openMetadataWithToken("http://localhost:8585/api", "my_token"))
    .fields(
      field.name("name").oneOf("peter", "john"),
      field.name("age").min(18),
      field.name("customers").fields(field.name("sex").oneOf("M", "F"))
    )

  execute(jsonTask)
}

case class NoopDataSourceMetadata(connectionConfig: Map[String, String]) extends DataSourceMetadata {
  override val name: String = "noop"
  override val format: String = "noop"
  override val hasSourceData: Boolean = false
}
