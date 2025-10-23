package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

/**
 * Alternative example showing reuse with step-level customization
 */
class YamlMetadataReuseWithStepCustomization extends PlanRun {

  // Load the YAML schema
  val yamlSchema = metadataSource.yamlTask(
    "app/src/test/resources/sample/task/file/simple-json-task.yaml",
    "simple_json",
    "file_account"
  )

  // Region-specific outputs with different characteristics
  val regions = List(
    ("us_east", "US-EAST-", 25000),
    ("us_west", "US-WEST-", 30000),
    ("eu_central", "EU-CENT-", 20000),
    ("asia_pacific", "APAC-", 15000),
    ("south_america", "LATAM-", 10000)
  )

  // Generate a task for each region with customizations
  val regionTasks = regions.map { case (regionName, idPrefix, recordCount) =>
    json(s"${regionName}_accounts", s"/tmp/yaml-reuse/regions/$regionName/accounts.json")
      .fields(yamlSchema)
      .fields(
        field.name("account_id").regex(s"$idPrefix[0-9]{8}"),
        field.name("region").sql(s"'$regionName'")  // Add region identifier
      )
      .count(count.records(recordCount))
  }

  val config = configuration
    .enableGeneratePlanAndTasks(true)
    .enableGenerateData(true)

  // Execute all regional tasks
  execute(config, regionTasks)
}
