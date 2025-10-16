package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

/**
 * Example 1: Load from YAML task file and override specific configurations
 */
class YamlTaskWithOverridesPlan extends PlanRun {

  val yamlTaskSchema = metadataSource.yamlTask(
    "/opt/app/custom/task/file/json/json-account-task.yaml",
    "json_account_file",
    "account"
  )

  // Create JSON file builder referencing the YAML task but with custom path
  val jsonWithYamlSchema = json("custom_json", "/opt/app/data/yaml-custom-output", Map("saveMode" -> "overwrite"))
    .fields(yamlTaskSchema)
    // Override specific field configurations
//    .fields(
//      field.name("account_id").regex("YAML-[0-9]{10}"),  // Custom pattern
//      field.name("amount").min(1000.0).max(5000.0)       // Custom range
//    )

  val config = configuration.enableGeneratePlanAndTasks(true)

  execute(config, jsonWithYamlSchema)
}