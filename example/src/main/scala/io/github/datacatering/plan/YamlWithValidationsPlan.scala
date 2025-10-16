package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

/**
 * Example 5: Use YAML task with validations
 */
class YamlWithValidationsPlan extends PlanRun {

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