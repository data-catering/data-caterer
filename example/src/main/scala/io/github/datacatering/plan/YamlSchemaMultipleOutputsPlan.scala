package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

/**
 * Example 3: Use YAML task schema for multiple different outputs
 */
class YamlSchemaMultipleOutputsPlan extends PlanRun {

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