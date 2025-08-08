package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.{ROWS_PER_SECOND, VALIDATION_IDENTIFIER}

class HttpOpenAPIPlanRun extends PlanRun {

  val httpTask = http("my_http", options = Map(ROWS_PER_SECOND -> "1"))
    .fields(metadataSource.openApi("/opt/app/mount/http/petstore.json"))
    .fields(field.httpBody(field.name("id").regex("ID[0-9]{8}")))
    .count(count.records(2))

  val httpGetTask = http("my_http", options = Map(VALIDATION_IDENTIFIER -> "GET/pets/{id}"))
    .validations(
      validation.field("request.method").isEqual("GET"),
      validation.field("request.method").isEqualField("response.statusText"),
      validation.field("response.statusCode").isEqual(200),
      validation.field("response.timeTakenMs").lessThan(10),
      validation.field("response.headers.Content-Length").greaterThan(0),
      validation.field("response.headers.Content-Type").isEqual("application/json"),
    )

  val myPlan = plan.addForeignKeyRelationship(
    foreignField("my_http", "POST/pets", "body.id"),
    foreignField("my_http", "GET/pets/{id}", "pathParamid"),
    foreignField("my_http", "DELETE/pets/{id}", "pathParamid")
  )

  val conf = configuration.enableGeneratePlanAndTasks(true)
    .generatedReportsFolderPath("/opt/app/data/report")
    .recordTrackingFolderPath("/opt/app/data/record-tracking")
    .recordTrackingForValidationFolderPath("/opt/app/data/record-tracking-valid")

  execute(myPlan, conf, httpTask, httpGetTask)
}
