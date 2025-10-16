package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.TimestampType

/**
 * Example 2: Reference YAML plan and extend with additional tasks
 */
class YamlPlanWithExtensionsPlan extends PlanRun {

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