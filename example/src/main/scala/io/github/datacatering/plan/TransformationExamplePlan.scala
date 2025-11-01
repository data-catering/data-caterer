package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DecimalType, DateType, TimestampType}

/**
 * Example demonstrating how to use custom transformations to modify generated data.
 * 
 * Transformations allow you to apply custom logic to the generated output files,
 * enabling you to:
 * - Convert to custom file formats
 * - Apply business-specific transformations
 * - Restructure or reformat data
 * - Add custom headers, footers, or metadata
 * 
 * There are two types of transformations:
 * 1. Per-record: Transform each line/record independently
 * 2. Whole-file: Transform the entire file as a unit
 */
class TransformationExamplePlan extends PlanRun {
  
  /**
   * Example 1: Per-record transformation
   * Converts each CSV record to uppercase using a custom transformer
   */
  val perRecordTask = csv("uppercase_accounts", "/tmp/transformation/uppercase", Map("partitions" -> "1", "saveMode" -> "overwrite"))
    .fields(
      field.name("account_id").regex("ACC[0-9]{8}"),
      field.name("name").expression("#{Name.name}"),
      field.name("status").oneOf("active", "inactive", "pending")
    )
    .count(count.records(10))
    .transformationPerRecord(
      "io.github.datacatering.transformer.UpperCasePerRecordTransformer",
      "transform"
    )
  
  /**
   * Example 2: Per-record transformation with options
   * Adds a prefix and suffix to each record
   */
  val perRecordWithOptionsTask = csv("prefixed_accounts", "/tmp/transformation/prefixed")
    .fields(
      field.name("account_id").regex("ACC[0-9]{8}"),
      field.name("balance").`type`(new DecimalType(10,2)).min(0).max(10000)
    )
    .count(count.records(50))
    .transformationPerRecord(
      "io.github.datacatering.transformer.UpperCasePerRecordTransformer",
      "transformWithOptions"
    )
    .transformationOptions(Map(
      "prefix" -> "BANK_",
      "suffix" -> "_VERIFIED"
    ))
  
  /**
   * Example 3: Whole-file transformation
   * Converts JSON lines format to a proper JSON array
   */
  val wholeFileTask = json("json_array_accounts", "/tmp/transformation/json_array")
    .fields(
      field.name("id").regex("[0-9]{10}"),
      field.name("email").expression("#{Internet.emailAddress}"),
      field.name("created_date").`type`(DateType).min(java.sql.Date.valueOf("2023-01-01"))
    )
    .count(count.records(200))
    .transformationWholeFile(
      "io.github.datacatering.transformer.JsonArrayWrapperTransformer",
      "transformFile"
    )
  
  /**
   * Example 4: Whole-file transformation with options
   * Creates a minified JSON array (no whitespace)
   */
  val wholeFileMinifiedTask = json("minified_transactions", "/tmp/transformation/minified")
    .fields(
      field.name("transaction_id").regex("TXN[0-9]{12}"),
      field.name("amount").`type`(new DecimalType(10,2)).min(1).max(1000),
      field.name("timestamp").`type`(TimestampType)
    )
    .count(count.records(500))
    .transformationWholeFile(
      "io.github.datacatering.transformer.JsonArrayWrapperTransformer",
      "transformFileWithOptions"
    )
    .transformationOptions(Map(
      "minify" -> "true"
    ))
  
  /**
   * Example 5: Task-level transformation
   * Apply the same transformation to all steps in a task
   */
  val taskLevelTransformation = json("task_level_json", "/tmp/transformation/task_level")
    .fields(
      field.name("id"),
      field.name("data")
    )
    .count(count.records(100))
  
  // Apply transformation at task level
  val taskWithTransformation = taskLevelTransformation
    .transformation(
      "io.github.datacatering.transformer.JsonArrayWrapperTransformer"
    )
  
  /**
   * Example 6: Custom output path for transformed file
   * Specify where the transformed file should be saved
   */
  val customOutputTask = csv("custom_output_accounts", "/tmp/transformation/original")
    .fields(
      field.name("account_id"),
      field.name("name")
    )
    .count(count.records(75))
    .transformationPerRecord(
      "io.github.datacatering.transformer.UpperCasePerRecordTransformer",
      "transform"
    )
    .transformationOutput(
      "/tmp/transformation/custom/transformed_accounts.csv",
      true  // Delete the original file after transformation
    )
  
  /**
   * Example 7: Conditional transformation
   * Enable or disable transformation based on configuration
   */
  val conditionalTransformationEnabled = sys.env.getOrElse("ENABLE_TRANSFORMATION", "false").toBoolean
  
  val conditionalTask = json("conditional_accounts", "/tmp/transformation/conditional")
    .fields(
      field.name("id"),
      field.name("name")
    )
    .count(count.records(100))
    .transformationWholeFile(
      "io.github.datacatering.transformer.JsonArrayWrapperTransformer",
      "transformFile"
    )
    .enableTransformation(conditionalTransformationEnabled)
  
  // Configuration
  val config = configuration
    .generatedReportsFolderPath("/tmp/transformation/report")
  
  // Execute examples - uncomment the ones you want to run
  execute(
    config,
    perRecordTask,
    // perRecordWithOptionsTask,
    // wholeFileTask,
    // wholeFileMinifiedTask,
    // taskWithTransformation,
    // customOutputTask,
    // conditionalTask
  )
}

