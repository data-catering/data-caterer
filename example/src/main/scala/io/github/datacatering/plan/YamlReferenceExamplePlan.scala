package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DateType, TimestampType}

/**
 * Example demonstrating how to reference existing YAML plan and task files
 * in the Scala/Java API while being able to override specific configurations.
 * 
 * This provides a bridge between existing YAML configurations and programmatic
 * control, giving users the best of both worlds.
 */
class YamlReferenceExamplePlan extends PlanRun {

  /**
   * Example 1: Load from YAML task file and override specific configurations
   */
  def yamlTaskWithOverrides(): Unit = {
    // Reference existing YAML task file and override specific field
    val jsonTask = yaml
      .stepByFileAndName("app/src/test/resources/sample/task/file/simple-json-task.yaml", "simple_json", "file_account")

    // Create JSON file builder referencing the YAML task but with custom path
    val jsonWithYamlSchema = json("custom_json", "/tmp/custom-output")
      .fields(metadataSource.yamlTask(
        "app/src/test/resources/sample/task/file/simple-json-task.yaml",
        "simple_json",
        "file_account"
      ))
      // Override specific field configurations
      .fields(
        field.name("account_id").regex("YAML-[0-9]{10}"),  // Custom pattern
        field.name("amount").min(1000.0).max(5000.0)       // Custom range
      )

    execute(jsonWithYamlSchema)
  }

  /**
   * Example 2: Reference YAML plan and extend with additional tasks
   */
  def yamlPlanWithExtensions(): Unit = {
    // Load base plan from YAML
    val basePlan = yaml.plan("app/src/test/resources/sample/plan/simple-json-plan.yaml")
      .description("Extended plan with additional custom tasks")

    // Add custom tasks alongside YAML-defined ones
    val customCsvTask = csv("additional_csv", "/tmp/extra-data")
      .fields(
        field.name("id").regex("CUSTOM-[0-9]{5}"),
        field.name("timestamp").`type`(TimestampType),
        field.name("value").regex("[A-Z]{3}-[0-9]{2}")
      )

    execute(basePlan, configuration, List(), customCsvTask)
  }

  /**
   * Example 3: Use YAML task schema for multiple different outputs
   */
  def yamlSchemaMultipleOutputs(): Unit = {
    // Reference the same YAML task schema for different file formats
    val yamlTaskSchema = metadataSource.yamlTask(
      "app/src/test/resources/sample/task/file/simple-json-task.yaml",
      "simple_json",
      "file_account" 
    )

    // Apply same schema to JSON output
    val jsonOutput = json("json_from_yaml", "/tmp/yaml-json")
      .fields(yamlTaskSchema)
      .count(count.records(1000))

    // Apply same schema to CSV output with additional field
    val csvOutput = csv("csv_from_yaml", "/tmp/yaml-csv") 
      .fields(yamlTaskSchema)
      .fields(field.name("source").sql("'yaml-generated'"))  // Additional field
      .count(count.records(500))

    // Apply same schema to Parquet output with different record count
    val parquetOutput = parquet("parquet_from_yaml", "/tmp/yaml-parquet")
      .fields(yamlTaskSchema)
      .count(count.records(2000))

    execute(jsonOutput, csvOutput, parquetOutput)
  }

  /**
   * Example 4: Mix YAML and programmatic foreign key relationships
   */
  def yamlWithForeignKeys(): Unit = {
    // Load account schema from YAML
    val accountTask = json("accounts", "/tmp/accounts")
      .fields(metadataSource.yamlTask(
        "app/src/test/resources/sample/task/file/simple-json-task.yaml",
        "simple_json",
        "file_account"
      ))

    // Create related transaction task programmatically
    val transactionTask = json("transactions", "/tmp/transactions")
      .fields(
        field.name("transaction_id").regex("TXN-[0-9]{8}"),
        field.name("account_id"),  // Will be linked to accounts
        field.name("amount").min(10.0).max(1000.0),
        field.name("transaction_date").`type`(DateType)
      )

    // Set up foreign key relationship
    val planWithRelations = plan
      .name("yaml-with-relationships")
      .addForeignKeyRelationship(transactionTask, "account_id", List((accountTask, "account_id")))

    execute(planWithRelations, configuration, List(), accountTask, transactionTask)
  }

  /**
   * Example 5: Use YAML task with validations
   */
  def yamlWithValidations(): Unit = {
    val jsonTask = json("validated_json", "/tmp/validated-output")
      .fields(metadataSource.yamlTask(
        "app/src/test/resources/sample/task/file/simple-json-task.yaml",
        "simple_json",
        "file_account"
      ))
      // Add custom validations
      .validations(
        validation.field("account_id").matches("[A-Z0-9-]+"),
        validation.field("amount").greaterThan(0),
        validation.field("year").between(2020, 2025)
      )

    val config = configuration
      .enableValidation(true)
      .enableGenerateData(true)

    execute(plan, config, List(), jsonTask)
  }

  /**
   * Example 6: Environment-specific YAML file loading
   */
  def environmentSpecificYaml(): Unit = {
    // Load YAML file based on environment
    val env = sys.env.getOrElse("ENV", "local")
    val yamlFile = s"app/src/test/resources/sample/task/file/${env}-json-task.yaml"
    
    val envSpecificTask = json(s"${env}_data", s"/tmp/${env}-output")
      .fields(metadataSource.yamlTask(yamlFile, "simple_json"))
      // Override count based on environment
      .count(if (env == "prod") count.records(10000) else count.records(100))

    execute(envSpecificTask)
  }
}
