package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.{OPEN_METADATA_AUTH_TYPE_OPEN_METADATA, OPEN_METADATA_JWT_TOKEN, OPEN_METADATA_TABLE_FQN}

class OpenMetadataSourcePlanRun extends PlanRun {

  val openMetadataSource = metadataSource.openMetadata(
    "http://host.docker.internal:8585/api",
    OPEN_METADATA_AUTH_TYPE_OPEN_METADATA,
    Map(
      OPEN_METADATA_JWT_TOKEN -> "abc123", //find under settings/bots/ingestion-bot/token
      OPEN_METADATA_TABLE_FQN -> "sample_data.ecommerce_db.shopify.raw_customer"
    )
  )

  val jsonTask = json("my_json", "/opt/app/data/json", Map("saveMode" -> "overwrite"))
    .fields(openMetadataSource)
    .fields(
      field.name("platform").oneOf("website", "mobile"),
      field.name("customer").fields(field.name("sex").oneOf("M", "F", "O"))
    )
    .count(count.records(10))

  val csvTask = csv("my_csv", "/opt/app/data/csv")
    .fields(openMetadataSource)

  val conf = configuration.enableGeneratePlanAndTasks(true)
    .enableGenerateValidations(true)
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(conf, jsonTask)
}
