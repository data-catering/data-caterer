package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

/**
 * Example 6: Environment-specific YAML file loading
 */
class YamlEnvironmentSpecificPlan extends PlanRun {

  // Load YAML file based on environment
  val env = sys.env.getOrElse("ENV", "local")
  val yamlFile = s"app/src/test/resources/sample/task/file/simple-json-task.yaml"
  
  val envSpecificTask = json(s"${env}_data", s"/tmp/${env}-output")
    .fields(metadataSource.yamlTask(yamlFile, "simple_json"))
    // Override count based on environment
    .count(if (env == "prod") count.records(10000) else count.records(100))

  execute(envSpecificTask)
}