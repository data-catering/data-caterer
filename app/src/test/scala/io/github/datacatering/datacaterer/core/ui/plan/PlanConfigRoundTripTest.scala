package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.api.model.{Plan, Step, TaskSummary, YamlValidationConfiguration}
import io.github.datacatering.datacaterer.core.ui.model.{ConfigurationRequest, PlanRunRequest}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}
import scala.reflect.io.Directory
import java.io.File

/**
 * Tests to verify that plan configurations are correctly preserved during save/load round-trips.
 * 
 * These tests document bugs where certain plan configurations are lost when:
 * 1. A plan is saved via the UI
 * 2. The plan is loaded back for editing
 * 
 * The UI JavaScript code is responsible for restoring the UI state from the saved JSON,
 * but certain options are not being checked when loading plans back.
 * 
 * Run tests with:
 * ./gradlew :app:test --tests "io.github.datacatering.datacaterer.core.ui.plan.PlanConfigRoundTripTest" --info
 */
class PlanConfigRoundTripTest extends AnyFunSuite with Matchers {

  private val objectMapper = ObjectMapperUtil.jsonObjectMapper

  /**
   * Helper to save a plan to a temporary directory and read it back
   */
  private def roundTripPlan(planRunRequest: PlanRunRequest): PlanRunRequest = {
    val tempDir = Files.createTempDirectory("datacaterer-roundtrip-test")
    try {
      val planFile = tempDir.resolve(s"${planRunRequest.plan.name}.json")
      val fileContent = objectMapper.writeValueAsString(planRunRequest)
      Files.writeString(planFile, fileContent)
      
      val readContent = Files.readString(planFile)
      objectMapper.readValue(readContent, classOf[PlanRunRequest])
    } finally {
      new Directory(new File(tempDir.toString)).deleteRecursively()
    }
  }

  // ============================================================================
  // BUG: Auto Generation Flag Not Restored
  // ============================================================================
  
  /**
   * BUG: When a plan is saved with "Auto" generation enabled, the enableDataGeneration
   * option is saved in task.options, but when loading the plan back into the UI,
   * the createGenerationElements() function in helper-generation.js does NOT check
   * for this option to restore the "Auto" checkbox state.
   * 
   * The code has a TODO comment: "// TODO check if there is auto schema defined"
   * 
   * Location: app/src/main/resources/ui/helper-generation.js:53-72
   * 
   * Expected behavior: When loading a plan with enableDataGeneration="true" in task options,
   * the "Auto" generation checkbox should be checked.
   */
  test("BUG: enableDataGeneration option should be preserved in round-trip for UI restoration") {
    val planName = "auto-gen-test-plan"
    val taskName = "auto-gen-task"
    val dataSourceName = "test-datasource"
    
    // Create a plan with auto generation enabled (as the UI would create it)
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map(
        "enableDataGeneration" -> "true",  // This is set when "Auto" checkbox is checked
        "path" -> "/tmp/test"
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    // Round-trip the plan (save and load)
    val loadedRequest = roundTripPlan(originalRequest)
    
    // Verify the enableDataGeneration option is preserved
    loadedRequest.tasks should have size 1
    val loadedStep = loadedRequest.tasks.head
    
    // This assertion documents what SHOULD happen for the UI to work correctly
    loadedStep.options should contain key "enableDataGeneration"
    loadedStep.options("enableDataGeneration") shouldBe "true"
    
    // The bug is that helper-generation.js:createGenerationElements() does NOT check for this option
    // It only checks for:
    // 1. dataSource.options["metadataSourceName"] -> auto-from-metadata checkbox
    // 2. dataSource.fields && dataSource.fields.length > 0 -> manual checkbox
    // But NEVER checks for enableDataGeneration -> auto checkbox
  }

  /**
   * This test verifies the structure of a plan with auto generation from metadata source.
   * This configuration IS correctly restored because the code checks for metadataSourceName.
   */
  test("Auto generation from metadata source option is preserved (this works correctly)") {
    val planName = "auto-metadata-test-plan"
    val taskName = "auto-metadata-task"
    val dataSourceName = "test-datasource"
    
    val step = Step(
      name = taskName,
      `type` = "postgres",
      options = Map(
        "metadataSourceName" -> "my-metadata-source",  // This triggers auto-from-metadata checkbox
        "dbtable" -> "public.users"
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.tasks should have size 1
    val loadedStep = loadedRequest.tasks.head
    
    // This is correctly restored because createGenerationElements() checks for it
    loadedStep.options should contain key "metadataSourceName"
    loadedStep.options("metadataSourceName") shouldBe "my-metadata-source"
  }

  /**
   * This test verifies manual field definitions are preserved.
   * This configuration IS correctly restored because the code checks for fields.length > 0.
   */
  test("Manual generation fields are preserved (this works correctly)") {
    val planName = "manual-fields-test-plan"
    val taskName = "manual-fields-task"
    val dataSourceName = "test-datasource"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test"),
      fields = List(
        io.github.datacatering.datacaterer.api.model.Field(
          name = "id",
          `type` = Some("string")
        ),
        io.github.datacatering.datacaterer.api.model.Field(
          name = "name",
          `type` = Some("string")
        )
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.tasks should have size 1
    val loadedStep = loadedRequest.tasks.head
    
    // This is correctly restored because createGenerationElements() checks for fields.length > 0
    loadedStep.fields should have size 2
    loadedStep.fields.map(_.name) should contain allOf ("id", "name")
  }

  // ============================================================================
  // BUG: Auto Validation Flag Not Restored
  // ============================================================================
  
  /**
   * BUG: When a plan is saved with "Auto" validation enabled, the enableDataValidation
   * option is saved in validation.options, but when loading the plan back into the UI,
   * the createValidationFromPlan() function in helper-validation.js does NOT check
   * for this option to restore the "Auto" checkbox state.
   * 
   * Location: app/src/main/resources/ui/helper-validation.js:91-110
   * 
   * The code only checks for:
   * 1. dataSource.validations && dataSource.options["metadataSourceName"] -> auto-from-metadata
   * 2. dataSource.validations && dataSource.validations.length > 0 -> manual
   * But NEVER checks for enableDataValidation -> auto checkbox
   * 
   * Expected behavior: When loading a plan with enableDataValidation="true" in validation options,
   * the "Auto" validation checkbox should be checked.
   */
  test("BUG: enableDataValidation option should be preserved in round-trip for UI restoration") {
    val planName = "auto-validation-test-plan"
    val taskName = "auto-validation-task"
    val dataSourceName = "test-datasource"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName)),
      validations = List(taskName)  // Reference the task for validation
    )
    
    // Create validation config with auto validation enabled
    val validationConfig = YamlValidationConfiguration(
      name = taskName,
      dataSources = Map(
        dataSourceName -> List(
          io.github.datacatering.datacaterer.api.model.YamlDataSourceValidation(
            options = Map("enableDataValidation" -> "true"),  // This is set when "Auto" checkbox is checked
            validations = List()
          )
        )
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      validation = List(validationConfig)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    // Verify the validation config is preserved
    loadedRequest.validation should have size 1
    val loadedValidation = loadedRequest.validation.head
    loadedValidation.dataSources should contain key dataSourceName
    
    val loadedDataSourceValidation = loadedValidation.dataSources(dataSourceName).head
    
    // This assertion documents what SHOULD happen for the UI to work correctly
    loadedDataSourceValidation.options should contain key "enableDataValidation"
    loadedDataSourceValidation.options("enableDataValidation") shouldBe "true"
    
    // The bug is that helper-validation.js:createValidationFromPlan() does NOT check for this option
  }

  // ============================================================================
  // Configuration Round-Trip Tests
  // ============================================================================

  /**
   * Test that advanced configuration flags are preserved during round-trip.
   * This is important because enableGeneratePlanAndTasks is linked to the auto generation checkbox.
   */
  test("Configuration flags including enableGeneratePlanAndTasks should be preserved") {
    val planName = "config-flags-test-plan"
    val taskName = "config-task"
    val dataSourceName = "test-datasource"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map(
        "enableDataGeneration" -> "true",
        "path" -> "/tmp/test"
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    // Create configuration with enableGeneratePlanAndTasks flag
    // This flag is automatically set to true when "Auto" generation checkbox is checked
    val configRequest = ConfigurationRequest(
      flag = Map(
        "enableGeneratePlanAndTasks" -> "true",
        "enableGenerateData" -> "true"
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      configuration = Some(configRequest)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    // Verify configuration is preserved
    loadedRequest.configuration shouldBe defined
    val loadedConfig = loadedRequest.configuration.get
    
    loadedConfig.flag should contain key "enableGeneratePlanAndTasks"
    loadedConfig.flag("enableGeneratePlanAndTasks") shouldBe "true"
    loadedConfig.flag should contain key "enableGenerateData"
    loadedConfig.flag("enableGenerateData") shouldBe "true"
  }

  /**
   * Test that record count configuration is preserved during round-trip.
   */
  test("Record count configuration should be preserved") {
    val planName = "record-count-test-plan"
    val taskName = "record-count-task"
    val dataSourceName = "test-datasource"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test"),
      count = io.github.datacatering.datacaterer.api.model.Count(
        records = Some(1000),
        perField = None,
        options = Map()
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.tasks should have size 1
    val loadedStep = loadedRequest.tasks.head
    
    loadedStep.count.records shouldBe Some(1000)
  }

  /**
   * Test that foreign key configuration is preserved during round-trip.
   */
  test("Foreign key configuration should be preserved") {
    val planName = "fk-test-plan"
    val task1Name = "accounts-task"
    val task2Name = "transactions-task"
    val ds1Name = "accounts-ds"
    val ds2Name = "transactions-ds"
    
    val step1 = Step(
      name = task1Name,
      `type` = "csv",
      options = Map("path" -> "/tmp/accounts")
    )
    
    val step2 = Step(
      name = task2Name,
      `type` = "csv",
      options = Map("path" -> "/tmp/transactions")
    )
    
    // Create foreign key using the proper model structure
    val foreignKey = io.github.datacatering.datacaterer.api.model.ForeignKey(
      source = io.github.datacatering.datacaterer.api.model.ForeignKeyRelation(
        dataSource = ds1Name,
        step = task1Name,
        fields = List("account_id")
      ),
      generate = List(
        io.github.datacatering.datacaterer.api.model.ForeignKeyRelation(
          dataSource = ds2Name,
          step = task2Name,
          fields = List("account_id")
        )
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(
        TaskSummary(task1Name, ds1Name),
        TaskSummary(task2Name, ds2Name)
      ),
      sinkOptions = Some(io.github.datacatering.datacaterer.api.model.SinkOptions(
        foreignKeys = List(foreignKey)
      ))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step1, step2)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.plan.sinkOptions shouldBe defined
    val loadedSinkOptions = loadedRequest.plan.sinkOptions.get
    loadedSinkOptions.foreignKeys should have size 1
    
    val loadedFk = loadedSinkOptions.foreignKeys.head
    loadedFk.source.dataSource shouldBe ds1Name
    loadedFk.source.step shouldBe task1Name
    loadedFk.source.fields should contain ("account_id")
    loadedFk.generate should have size 1
    loadedFk.generate.head.dataSource shouldBe ds2Name
    loadedFk.generate.head.step shouldBe task2Name
  }

  // ============================================================================
  // Combined Configuration Tests
  // ============================================================================

  /**
   * Test a comprehensive plan with multiple configuration options to ensure
   * all settings are preserved during round-trip.
   */
  test("Comprehensive plan with all configuration types should be preserved") {
    val planName = "comprehensive-test-plan"
    val taskName = "comprehensive-task"
    val dataSourceName = "test-datasource"
    
    // Create a step with auto generation AND manual fields (both can be enabled)
    val step = Step(
      name = taskName,
      `type` = "postgres",
      options = Map(
        "enableDataGeneration" -> "true",  // Auto generation
        "metadataSourceName" -> "my-metadata",  // Auto from metadata
        "dbtable" -> "public.users"
      ),
      fields = List(  // Manual fields
        io.github.datacatering.datacaterer.api.model.Field(name = "id", `type` = Some("string")),
        io.github.datacatering.datacaterer.api.model.Field(name = "name", `type` = Some("string"))
      ),
      count = io.github.datacatering.datacaterer.api.model.Count(records = Some(500))
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName)),
      validations = List(taskName)
    )
    
    val validationConfig = YamlValidationConfiguration(
      name = taskName,
      dataSources = Map(
        dataSourceName -> List(
          io.github.datacatering.datacaterer.api.model.YamlDataSourceValidation(
            options = Map("enableDataValidation" -> "true"),
            validations = List()
          )
        )
      )
    )
    
    val configRequest = ConfigurationRequest(
      flag = Map(
        "enableGeneratePlanAndTasks" -> "true",
        "enableGenerateData" -> "true",
        "enableRecordTracking" -> "true"
      ),
      generation = Map(
        "numRecordsPerBatch" -> "1000"
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      validation = List(validationConfig),
      configuration = Some(configRequest)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    // Verify all components are preserved
    loadedRequest.plan.name shouldBe planName
    loadedRequest.tasks should have size 1
    loadedRequest.validation should have size 1
    loadedRequest.configuration shouldBe defined
    
    val loadedStep = loadedRequest.tasks.head
    
    // These are the options that need to be checked by the UI JavaScript
    // to properly restore the checkbox states
    loadedStep.options should contain key "enableDataGeneration"
    loadedStep.options should contain key "metadataSourceName"
    loadedStep.fields should have size 2
    loadedStep.count.records shouldBe Some(500)
    
    val loadedValidation = loadedRequest.validation.head.dataSources(dataSourceName).head
    loadedValidation.options should contain key "enableDataValidation"
    
    val loadedConfig = loadedRequest.configuration.get
    loadedConfig.flag should contain key "enableGeneratePlanAndTasks"
    loadedConfig.generation should contain key "numRecordsPerBatch"
  }

  // ============================================================================
  // JSON Structure Tests - Understanding UI vs Backend Format
  // ============================================================================

  /**
   * Test that the UI now sends count in the correct backend format.
   * 
   * After the fix, the UI sends:
   * {
   *   "options": {"min": 500, "max": 1500}  // for "Generated records between"
   * }
   * 
   * Instead of the old broken format:
   * {
   *   "recordsMin": 500,
   *   "recordsMax": 1500
   * }
   */
  test("FIXED: UI count format with min/max in options should work correctly") {
    // This is the NEW format the UI sends when user selects "Generated records between"
    val fixedFormatJson = """
      {
        "id": "test-id",
        "plan": {
          "name": "test-plan",
          "tasks": [{"name": "test-task", "dataSourceName": "test-ds"}]
        },
        "tasks": [{
          "name": "test-task",
          "type": "csv",
          "count": {
            "options": {"min": 500, "max": 1500}
          },
          "options": {"path": "/tmp/test"}
        }]
      }
    """
    
    val parsed = objectMapper.readValue(fixedFormatJson, classOf[PlanRunRequest])
    
    parsed.tasks should have size 1
    val step = parsed.tasks.head
    step.count.options should contain key "min"
    step.count.options should contain key "max"
    step.count.options("min") shouldBe 500
    step.count.options("max") shouldBe 1500
  }
  
  /**
   * Test that simple count format (without min/max) works correctly.
   * This is the format used when user selects "Records" (not "Generated records between").
   */
  test("Simple count format (records only) should work correctly") {
    val simpleFormatJson = """
      {
        "id": "test-id",
        "plan": {
          "name": "test-plan",
          "tasks": [{"name": "test-task", "dataSourceName": "test-ds"}]
        },
        "tasks": [{
          "name": "test-task",
          "type": "csv",
          "count": {
            "records": 1000
          },
          "options": {"path": "/tmp/test"}
        }]
      }
    """
    
    val parsed = objectMapper.readValue(simpleFormatJson, classOf[PlanRunRequest])
    
    parsed.tasks should have size 1
    val step = parsed.tasks.head
    step.count.records shouldBe Some(1000)
  }

  /**
   * Test that documents the correct backend format for count with min/max.
   * This is the format that SHOULD be sent by the UI for proper round-trip.
   */
  test("Backend count format with options should be correctly parsed") {
    val backendFormatJson = """
      {
        "id": "test-id",
        "plan": {
          "name": "test-plan",
          "tasks": [{"name": "test-task", "dataSourceName": "test-ds"}]
        },
        "tasks": [{
          "name": "test-task",
          "type": "csv",
          "count": {
            "records": 1000,
            "options": {"min": 500, "max": 1500}
          },
          "options": {"path": "/tmp/test"}
        }]
      }
    """
    
    val parsed = objectMapper.readValue(backendFormatJson, classOf[PlanRunRequest])
    
    parsed.tasks should have size 1
    val step = parsed.tasks.head
    
    step.count.records shouldBe Some(1000)
    step.count.options should contain key "min"
    step.count.options should contain key "max"
    step.count.options("min") shouldBe 500
    step.count.options("max") shouldBe 1500
  }

  /**
   * Test that documents the correct backend format for per-field count.
   */
  test("Backend per-field count format should be correctly parsed") {
    val backendFormatJson = """
      {
        "id": "test-id",
        "plan": {
          "name": "test-plan",
          "tasks": [{"name": "test-task", "dataSourceName": "test-ds"}]
        },
        "tasks": [{
          "name": "test-task",
          "type": "csv",
          "count": {
            "records": 1000,
            "perField": {
              "fieldNames": ["account_id"],
              "count": 5
            }
          },
          "options": {"path": "/tmp/test"}
        }]
      }
    """
    
    val parsed = objectMapper.readValue(backendFormatJson, classOf[PlanRunRequest])
    
    parsed.tasks should have size 1
    val step = parsed.tasks.head
    
    step.count.records shouldBe Some(1000)
    step.count.perField shouldBe defined
    step.count.perField.get.fieldNames should contain ("account_id")
    step.count.perField.get.count shouldBe Some(5)
  }

  /**
   * FIXED: Count with min/max in options is correctly round-tripped.
   * 
   * The UI now:
   * - Saves min/max in count.options (not as flat recordsMin/recordsMax)
   * - Loads min/max from count.options
   */
  test("FIXED: Count with min/max in options round-trips correctly") {
    val planName = "count-roundtrip-test"
    val taskName = "count-task"
    val dataSourceName = "test-ds"
    
    // Create a step with the backend's expected format
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test"),
      count = io.github.datacatering.datacaterer.api.model.Count(
        records = Some(1000),
        options = Map("min" -> 500, "max" -> 1500)
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    // Round-trip
    val loadedRequest = roundTripPlan(originalRequest)
    
    // Verify the backend format is preserved
    val loadedStep = loadedRequest.tasks.head
    loadedStep.count.records shouldBe Some(1000)
    loadedStep.count.options should contain key "min"
    loadedStep.count.options should contain key "max"
    
    // After the fix, the UI correctly reads from count.options["min"]/["max"]
  }

  /**
   * FIXED: Per-field count format is correctly round-tripped.
   * 
   * The UI now:
   * - Saves field names in perField.fieldNames (not as flat perFieldNames)
   * - Saves count in perField.count (not as flat perFieldRecords)
   * - Loads from the correct nested structure
   */
  test("FIXED: Per-field count format round-trips correctly") {
    val planName = "per-field-roundtrip-test"
    val taskName = "per-field-task"
    val dataSourceName = "test-ds"
    
    // Create a step with per-field count using backend format
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test"),
      count = io.github.datacatering.datacaterer.api.model.Count(
        records = Some(1000),
        perField = Some(io.github.datacatering.datacaterer.api.model.PerFieldCount(
          fieldNames = List("account_id", "user_id"),
          count = Some(5),
          options = Map()
        ))
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    // Round-trip
    val loadedRequest = roundTripPlan(originalRequest)
    
    // Verify the backend format is preserved
    val loadedStep = loadedRequest.tasks.head
    loadedStep.count.perField shouldBe defined
    loadedStep.count.perField.get.fieldNames should contain allOf ("account_id", "user_id")
    loadedStep.count.perField.get.count shouldBe Some(5)
    
    // After the fix, the UI correctly reads from:
    // - count.perField.fieldNames
    // - count.perField.count
  }
  
  /**
   * FIXED: Per-field count with min/max in options round-trips correctly.
   */
  test("FIXED: Per-field count with min/max in options round-trips correctly") {
    val planName = "per-field-minmax-test"
    val taskName = "per-field-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test"),
      count = io.github.datacatering.datacaterer.api.model.Count(
        records = Some(1000),
        perField = Some(io.github.datacatering.datacaterer.api.model.PerFieldCount(
          fieldNames = List("account_id"),
          count = None,
          options = Map("min" -> 2, "max" -> 10, "distribution" -> "exponential", "rateParam" -> 0.5)
        ))
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    val loadedStep = loadedRequest.tasks.head
    loadedStep.count.perField shouldBe defined
    loadedStep.count.perField.get.fieldNames should contain ("account_id")
    loadedStep.count.perField.get.options should contain key "min"
    loadedStep.count.perField.get.options should contain key "max"
    loadedStep.count.perField.get.options should contain key "distribution"
    loadedStep.count.perField.get.options("distribution") shouldBe "exponential"
  }

  // ============================================================================
  // Configuration Round-Trip Tests - All Configuration Categories
  // ============================================================================

  /**
   * Test all flag configurations are preserved during round-trip.
   * These flags control various features of the data generation/validation process.
   */
  test("All flag configurations should be preserved during round-trip") {
    val planName = "all-flags-test"
    val taskName = "flags-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    // All available flag configurations
    val configRequest = ConfigurationRequest(
      flag = Map(
        "enableCount" -> "false",
        "enableGenerateData" -> "true",
        "enableReferenceMode" -> "true",
        "enableFailOnError" -> "false",
        "enableUniqueCheck" -> "true",
        "enableSinkMetadata" -> "true",
        "enableSaveReports" -> "false",
        "enableValidation" -> "false",
        "enableAlerts" -> "false",
        "enableGenerateValidations" -> "true",
        "enableRecordTracking" -> "false",
        "enableDeleteGeneratedRecords" -> "true",
        "enableGeneratePlanAndTasks" -> "true",
        "enableFastGeneration" -> "true"
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      configuration = Some(configRequest)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.configuration shouldBe defined
    val loadedFlags = loadedRequest.configuration.get.flag
    
    loadedFlags should contain ("enableCount" -> "false")
    loadedFlags should contain ("enableGenerateData" -> "true")
    loadedFlags should contain ("enableReferenceMode" -> "true")
    loadedFlags should contain ("enableFailOnError" -> "false")
    loadedFlags should contain ("enableUniqueCheck" -> "true")
    loadedFlags should contain ("enableSinkMetadata" -> "true")
    loadedFlags should contain ("enableSaveReports" -> "false")
    loadedFlags should contain ("enableValidation" -> "false")
    loadedFlags should contain ("enableAlerts" -> "false")
    loadedFlags should contain ("enableGenerateValidations" -> "true")
    loadedFlags should contain ("enableRecordTracking" -> "false")
    loadedFlags should contain ("enableDeleteGeneratedRecords" -> "true")
    loadedFlags should contain ("enableGeneratePlanAndTasks" -> "true")
    loadedFlags should contain ("enableFastGeneration" -> "true")
  }

  /**
   * Test generation configurations are preserved during round-trip.
   */
  test("Generation configurations should be preserved during round-trip") {
    val planName = "generation-config-test"
    val taskName = "gen-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val configRequest = ConfigurationRequest(
      generation = Map(
        "numRecordsPerBatch" -> "50000",
        "numRecordsPerStep" -> "2000"
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      configuration = Some(configRequest)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.configuration shouldBe defined
    val loadedGeneration = loadedRequest.configuration.get.generation
    
    loadedGeneration should contain ("numRecordsPerBatch" -> "50000")
    loadedGeneration should contain ("numRecordsPerStep" -> "2000")
  }

  /**
   * Test validation configurations are preserved during round-trip.
   */
  test("Validation configurations should be preserved during round-trip") {
    val planName = "validation-config-test"
    val taskName = "val-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val configRequest = ConfigurationRequest(
      validation = Map(
        "numSampleErrorRecords" -> "15",
        "enableDeleteRecordTrackingFiles" -> "false"
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      configuration = Some(configRequest)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.configuration shouldBe defined
    val loadedValidation = loadedRequest.configuration.get.validation
    
    loadedValidation should contain ("numSampleErrorRecords" -> "15")
    loadedValidation should contain ("enableDeleteRecordTrackingFiles" -> "false")
  }

  /**
   * Test metadata configurations are preserved during round-trip.
   */
  test("Metadata configurations should be preserved during round-trip") {
    val planName = "metadata-config-test"
    val taskName = "meta-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val configRequest = ConfigurationRequest(
      metadata = Map(
        "numGeneratedSamples" -> "25",
        "numRecordsFromDataSource" -> "5000",
        "numRecordsForAnalysis" -> "5000",
        "oneOfDistinctCountVsCountThreshold" -> "0.3",
        "oneOfMinCount" -> "500"
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      configuration = Some(configRequest)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.configuration shouldBe defined
    val loadedMetadata = loadedRequest.configuration.get.metadata
    
    loadedMetadata should contain ("numGeneratedSamples" -> "25")
    loadedMetadata should contain ("numRecordsFromDataSource" -> "5000")
    loadedMetadata should contain ("numRecordsForAnalysis" -> "5000")
    loadedMetadata should contain ("oneOfDistinctCountVsCountThreshold" -> "0.3")
    loadedMetadata should contain ("oneOfMinCount" -> "500")
  }

  /**
   * Test alert configurations are preserved during round-trip.
   */
  test("Alert configurations should be preserved during round-trip") {
    val planName = "alert-config-test"
    val taskName = "alert-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val configRequest = ConfigurationRequest(
      alert = Map(
        "triggerOn" -> "failure",
        "slackToken" -> "xoxb-test-token-12345",
        "slackChannels" -> "#alerts,#monitoring"
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      configuration = Some(configRequest)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.configuration shouldBe defined
    val loadedAlert = loadedRequest.configuration.get.alert
    
    loadedAlert should contain ("triggerOn" -> "failure")
    loadedAlert should contain ("slackToken" -> "xoxb-test-token-12345")
    loadedAlert should contain ("slackChannels" -> "#alerts,#monitoring")
  }

  /**
   * Test folder configurations are preserved during round-trip.
   */
  test("Folder configurations should be preserved during round-trip") {
    val planName = "folder-config-test"
    val taskName = "folder-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val configRequest = ConfigurationRequest(
      folder = Map(
        "generatedReportsFolderPath" -> "/custom/reports",
        "validationFolderPath" -> "/custom/validation",
        "planFilePath" -> "/custom/plan/my-plan.yaml",
        "taskFolderPath" -> "/custom/tasks",
        "generatedPlanAndTaskFolderPath" -> "/custom/generated",
        "recordTrackingFolderPath" -> "/custom/tracking",
        "recordTrackingForValidationFolderPath" -> "/custom/tracking-validation"
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      configuration = Some(configRequest)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.configuration shouldBe defined
    val loadedFolder = loadedRequest.configuration.get.folder
    
    loadedFolder should contain ("generatedReportsFolderPath" -> "/custom/reports")
    loadedFolder should contain ("validationFolderPath" -> "/custom/validation")
    loadedFolder should contain ("planFilePath" -> "/custom/plan/my-plan.yaml")
    loadedFolder should contain ("taskFolderPath" -> "/custom/tasks")
    loadedFolder should contain ("generatedPlanAndTaskFolderPath" -> "/custom/generated")
    loadedFolder should contain ("recordTrackingFolderPath" -> "/custom/tracking")
    loadedFolder should contain ("recordTrackingForValidationFolderPath" -> "/custom/tracking-validation")
  }

  // ============================================================================
  // Validation Configuration Round-Trip Tests
  // ============================================================================

  /**
   * Test validation with field validations is preserved during round-trip.
   * Uses FieldValidations which is the correct Validation type for field-level validations.
   */
  test("Field validations should be preserved during round-trip") {
    val planName = "field-validation-test"
    val taskName = "field-val-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName)),
      validations = List(taskName)
    )
    
    // Create validation with field validations using the correct model types
    val validationConfig = YamlValidationConfiguration(
      name = taskName,
      dataSources = Map(
        dataSourceName -> List(
          io.github.datacatering.datacaterer.api.model.YamlDataSourceValidation(
            options = Map(),
            validations = List(
              io.github.datacatering.datacaterer.api.model.FieldValidations(
                field = "account_id",
                validation = List(
                  io.github.datacatering.datacaterer.api.model.NullFieldValidation(negate = true)
                )
              ),
              io.github.datacatering.datacaterer.api.model.FieldValidations(
                field = "balance",
                validation = List(
                  io.github.datacatering.datacaterer.api.model.GreaterThanFieldValidation(value = 0)
                )
              )
            )
          )
        )
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      validation = List(validationConfig)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.validation should have size 1
    val loadedValidation = loadedRequest.validation.head
    loadedValidation.dataSources should contain key dataSourceName
    
    val loadedDataSourceValidation = loadedValidation.dataSources(dataSourceName).head
    loadedDataSourceValidation.validations should have size 2
    
    // Check first validation (account_id not null)
    val firstValidation = loadedDataSourceValidation.validations.head.asInstanceOf[io.github.datacatering.datacaterer.api.model.FieldValidations]
    firstValidation.field shouldBe "account_id"
    firstValidation.validation should have size 1
    
    // Check second validation (balance > 0)
    val secondValidation = loadedDataSourceValidation.validations(1).asInstanceOf[io.github.datacatering.datacaterer.api.model.FieldValidations]
    secondValidation.field shouldBe "balance"
    secondValidation.validation should have size 1
  }

  /**
   * Test validation from metadata source is preserved during round-trip.
   */
  test("Validation from metadata source should be preserved during round-trip") {
    val planName = "metadata-validation-test"
    val taskName = "metadata-val-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "postgres",
      options = Map("url" -> "jdbc:postgresql://localhost:5432/test")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName)),
      validations = List(taskName)
    )
    
    // Create validation with metadata source
    val validationConfig = YamlValidationConfiguration(
      name = taskName,
      dataSources = Map(
        dataSourceName -> List(
          io.github.datacatering.datacaterer.api.model.YamlDataSourceValidation(
            options = Map(
              "metadataSourceName" -> "my-openmetadata",
              "tableFQN" -> "database.schema.users"
            ),
            validations = List()
          )
        )
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      validation = List(validationConfig)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.validation should have size 1
    val loadedValidation = loadedRequest.validation.head
    loadedValidation.dataSources should contain key dataSourceName
    
    val loadedDataSourceValidation = loadedValidation.dataSources(dataSourceName).head
    loadedDataSourceValidation.options should contain ("metadataSourceName" -> "my-openmetadata")
    loadedDataSourceValidation.options should contain ("tableFQN" -> "database.schema.users")
  }

  /**
   * Test group by validation is preserved during round-trip.
   */
  test("Group by validation should be preserved during round-trip") {
    val planName = "groupby-validation-test"
    val taskName = "groupby-val-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName)),
      validations = List(taskName)
    )
    
    // Create validation with group by using the correct model type
    val validationConfig = YamlValidationConfiguration(
      name = taskName,
      dataSources = Map(
        dataSourceName -> List(
          io.github.datacatering.datacaterer.api.model.YamlDataSourceValidation(
            options = Map(),
            validations = List(
              io.github.datacatering.datacaterer.api.model.GroupByValidation(
                groupByFields = Seq("account_id", "region"),
                aggType = "count",
                aggField = "transaction_id",
                validation = List(
                  io.github.datacatering.datacaterer.api.model.GreaterThanFieldValidation(value = 0)
                )
              )
            )
          )
        )
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step),
      validation = List(validationConfig)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.validation should have size 1
    val loadedValidation = loadedRequest.validation.head
    val loadedDataSourceValidation = loadedValidation.dataSources(dataSourceName).head
    loadedDataSourceValidation.validations should have size 1
    
    val groupByValidation = loadedDataSourceValidation.validations.head.asInstanceOf[io.github.datacatering.datacaterer.api.model.GroupByValidation]
    groupByValidation.groupByFields should contain allOf ("account_id", "region")
    groupByValidation.aggType shouldBe "count"
    groupByValidation.aggField shouldBe "transaction_id"
  }

  /**
   * Test upstream validation is preserved during round-trip.
   */
  test("Upstream validation should be preserved during round-trip") {
    val planName = "upstream-validation-test"
    val task1Name = "accounts-task"
    val task2Name = "transactions-task"
    val ds1Name = "accounts-ds"
    val ds2Name = "transactions-ds"
    
    val step1 = Step(
      name = task1Name,
      `type` = "csv",
      options = Map("path" -> "/tmp/accounts")
    )
    
    val step2 = Step(
      name = task2Name,
      `type` = "csv",
      options = Map("path" -> "/tmp/transactions")
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(
        TaskSummary(task1Name, ds1Name),
        TaskSummary(task2Name, ds2Name)
      ),
      validations = List(task2Name)
    )
    
    // Create upstream validation using the correct model type
    val validationConfig = YamlValidationConfiguration(
      name = task2Name,
      dataSources = Map(
        ds2Name -> List(
          io.github.datacatering.datacaterer.api.model.YamlDataSourceValidation(
            options = Map(),
            validations = List(
              io.github.datacatering.datacaterer.api.model.YamlUpstreamDataSourceValidation(
                upstreamTaskName = task1Name,
                joinFields = List("account_id"),
                joinType = "inner",
                validation = List(
                  io.github.datacatering.datacaterer.api.model.ExpressionValidation(expr = "count > 0")
                )
              )
            )
          )
        )
      )
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step1, step2),
      validation = List(validationConfig)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    loadedRequest.validation should have size 1
    val loadedValidation = loadedRequest.validation.head
    val loadedDataSourceValidation = loadedValidation.dataSources(ds2Name).head
    loadedDataSourceValidation.validations should have size 1
    
    val upstreamValidation = loadedDataSourceValidation.validations.head.asInstanceOf[io.github.datacatering.datacaterer.api.model.YamlUpstreamDataSourceValidation]
    upstreamValidation.upstreamTaskName shouldBe task1Name
    upstreamValidation.joinFields should contain ("account_id")
    upstreamValidation.joinType shouldBe "inner"
  }

  // ============================================================================
  // Data Source Options Round-Trip Tests
  // ============================================================================

  /**
   * Test that data source override options (schema, table, partitions, etc.) are preserved.
   */
  test("Data source override options should be preserved during round-trip") {
    val planName = "datasource-options-test"
    val taskName = "ds-options-task"
    val dataSourceName = "postgres-ds"
    
    val step = Step(
      name = taskName,
      `type` = "postgres",
      options = Map(
        "url" -> "jdbc:postgresql://localhost:5432/mydb",
        "user" -> "testuser",
        "password" -> "testpass",
        "schema" -> "public",
        "table" -> "users",
        "partitions" -> "4",
        "partitionBy" -> "created_date"
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    val loadedStep = loadedRequest.tasks.head
    loadedStep.options should contain ("url" -> "jdbc:postgresql://localhost:5432/mydb")
    loadedStep.options should contain ("user" -> "testuser")
    loadedStep.options should contain ("password" -> "testpass")
    loadedStep.options should contain ("schema" -> "public")
    loadedStep.options should contain ("table" -> "users")
    loadedStep.options should contain ("partitions" -> "4")
    loadedStep.options should contain ("partitionBy" -> "created_date")
  }

  /**
   * Test that Kafka-specific options are preserved during round-trip.
   */
  test("Kafka data source options should be preserved during round-trip") {
    val planName = "kafka-options-test"
    val taskName = "kafka-task"
    val dataSourceName = "kafka-ds"
    
    val step = Step(
      name = taskName,
      `type` = "kafka",
      options = Map(
        "url" -> "localhost:9092",
        "topic" -> "my-events"
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    val loadedStep = loadedRequest.tasks.head
    loadedStep.options should contain ("url" -> "localhost:9092")
    loadedStep.options should contain ("topic" -> "my-events")
  }

  /**
   * Test that HTTP data source options are preserved during round-trip.
   */
  test("HTTP data source options should be preserved during round-trip") {
    val planName = "http-options-test"
    val taskName = "http-task"
    val dataSourceName = "http-ds"
    
    val step = Step(
      name = taskName,
      `type` = "http",
      options = Map(
        "user" -> "apiuser",
        "password" -> "apipass",
        "method" -> "POST",
        "endpoint" -> "/api/v1/data"
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    val loadedStep = loadedRequest.tasks.head
    loadedStep.options should contain ("user" -> "apiuser")
    loadedStep.options should contain ("password" -> "apipass")
    loadedStep.options should contain ("method" -> "POST")
    loadedStep.options should contain ("endpoint" -> "/api/v1/data")
  }

  /**
   * Test that Iceberg data source options are preserved during round-trip.
   */
  test("Iceberg data source options should be preserved during round-trip") {
    val planName = "iceberg-options-test"
    val taskName = "iceberg-task"
    val dataSourceName = "iceberg-ds"
    
    val step = Step(
      name = taskName,
      `type` = "iceberg",
      options = Map(
        "catalogType" -> "hadoop",
        "path" -> "/data/warehouse",
        "catalogUri" -> "thrift://localhost:9083",
        "table" -> "mydb.users",
        "partitions" -> "2",
        "partitionBy" -> "date"
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    val loadedStep = loadedRequest.tasks.head
    loadedStep.options should contain ("catalogType" -> "hadoop")
    loadedStep.options should contain ("path" -> "/data/warehouse")
    loadedStep.options should contain ("catalogUri" -> "thrift://localhost:9083")
    loadedStep.options should contain ("table" -> "mydb.users")
    loadedStep.options should contain ("partitions" -> "2")
    loadedStep.options should contain ("partitionBy" -> "date")
  }

  // ============================================================================
  // Field Configuration Round-Trip Tests
  // ============================================================================

  /**
   * Test that field options (generators, constraints) are preserved during round-trip.
   */
  test("Field options should be preserved during round-trip") {
    val planName = "field-options-test"
    val taskName = "field-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "csv",
      options = Map("path" -> "/tmp/test"),
      fields = List(
        io.github.datacatering.datacaterer.api.model.Field(
          name = "id",
          `type` = Some("string"),
          options = Map(
            "isUnique" -> "true",
            "regex" -> "[A-Z]{3}[0-9]{6}"
          )
        ),
        io.github.datacatering.datacaterer.api.model.Field(
          name = "email",
          `type` = Some("string"),
          options = Map(
            "expression" -> "#{Internet.emailAddress}"
          )
        ),
        io.github.datacatering.datacaterer.api.model.Field(
          name = "balance",
          `type` = Some("double"),
          options = Map(
            "min" -> "0.0",
            "max" -> "10000.0",
            "round" -> "2"
          )
        ),
        io.github.datacatering.datacaterer.api.model.Field(
          name = "status",
          `type` = Some("string"),
          options = Map(
            "oneOf" -> "active,inactive,suspended"
          )
        ),
        io.github.datacatering.datacaterer.api.model.Field(
          name = "created_at",
          `type` = Some("timestamp"),
          options = Map(
            "min" -> "2024-01-01 00:00:00",
            "max" -> "2024-12-31 23:59:59"
          )
        )
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    val loadedStep = loadedRequest.tasks.head
    loadedStep.fields should have size 5
    
    // Check id field
    val idField = loadedStep.fields.find(_.name == "id").get
    idField.`type` shouldBe Some("string")
    idField.options should contain ("isUnique" -> "true")
    idField.options should contain ("regex" -> "[A-Z]{3}[0-9]{6}")
    
    // Check email field
    val emailField = loadedStep.fields.find(_.name == "email").get
    emailField.options should contain ("expression" -> "#{Internet.emailAddress}")
    
    // Check balance field
    val balanceField = loadedStep.fields.find(_.name == "balance").get
    balanceField.`type` shouldBe Some("double")
    balanceField.options should contain ("min" -> "0.0")
    balanceField.options should contain ("max" -> "10000.0")
    balanceField.options should contain ("round" -> "2")
    
    // Check status field
    val statusField = loadedStep.fields.find(_.name == "status").get
    statusField.options should contain ("oneOf" -> "active,inactive,suspended")
    
    // Check created_at field
    val createdAtField = loadedStep.fields.find(_.name == "created_at").get
    createdAtField.`type` shouldBe Some("timestamp")
    createdAtField.options should contain ("min" -> "2024-01-01 00:00:00")
    createdAtField.options should contain ("max" -> "2024-12-31 23:59:59")
  }

  /**
   * Test that nested struct fields are preserved during round-trip.
   */
  test("Nested struct fields should be preserved during round-trip") {
    val planName = "nested-fields-test"
    val taskName = "nested-task"
    val dataSourceName = "test-ds"
    
    val step = Step(
      name = taskName,
      `type` = "json",
      options = Map("path" -> "/tmp/test"),
      fields = List(
        io.github.datacatering.datacaterer.api.model.Field(
          name = "user",
          `type` = Some("struct"),
          fields = List(
            io.github.datacatering.datacaterer.api.model.Field(
              name = "name",
              `type` = Some("string")
            ),
            io.github.datacatering.datacaterer.api.model.Field(
              name = "address",
              `type` = Some("struct"),
              fields = List(
                io.github.datacatering.datacaterer.api.model.Field(
                  name = "street",
                  `type` = Some("string")
                ),
                io.github.datacatering.datacaterer.api.model.Field(
                  name = "city",
                  `type` = Some("string")
                )
              )
            )
          )
        )
      )
    )
    
    val plan = Plan(
      name = planName,
      tasks = List(TaskSummary(taskName, dataSourceName))
    )
    
    val originalRequest = PlanRunRequest(
      id = "test-run-id",
      plan = plan,
      tasks = List(step)
    )
    
    val loadedRequest = roundTripPlan(originalRequest)
    
    val loadedStep = loadedRequest.tasks.head
    loadedStep.fields should have size 1
    
    val userField = loadedStep.fields.head
    userField.name shouldBe "user"
    userField.`type` shouldBe Some("struct")
    userField.fields should have size 2
    
    // Check nested address struct
    val addressField = userField.fields.find(_.name == "address").get
    addressField.`type` shouldBe Some("struct")
    addressField.fields should have size 2
    addressField.fields.map(_.name) should contain allOf ("street", "city")
  }
}

