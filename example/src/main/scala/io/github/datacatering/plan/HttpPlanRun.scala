package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.model.Constants.VALIDATION_IDENTIFIER
import io.github.datacatering.datacaterer.api.model.IntegerType
import io.github.datacatering.datacaterer.api.{HttpMethodEnum, PlanRun}

class HttpPlanRun extends PlanRun {

  val httpPostTask = http("post_http", options = Map(VALIDATION_IDENTIFIER -> "POST/pets"))
    .fields(field.httpHeader("Content-Type").static("application/json"))
    .fields(field.httpUrl(
      "http://host.docker.internal:80/anything/pets", //url
      HttpMethodEnum.POST //method
    ))
    .fields(
      field.httpBody(
        field.name("id").regex("[0-9]{8}"),
        field.name("name").expression("#{Name.name}"),
      )
    )
    .count(count.records(10))
    .validations(
      validation.field("request.method").isEqual("POST"),
      validation.field("request.method").isEqualField("response.statusText"),
      validation.field("response.statusCode").isEqual(200),
      validation.field("response.timeTaken").lessThan(100),
      validation.field("response.headers.Content-Length").greaterThan(0),
      validation.field("response.headers.Content-Type").isEqual("application/json"),
    )

  val httpGetTask = http("get_http", options = Map(VALIDATION_IDENTIFIER -> "GET/pets/{id}"))
    .fields(
      field.httpHeader("Content-Type").static("application/json"),
      field.httpHeader("Content-Length"),
      field.httpHeader("X-Account-Id").sql("body.account_id"),
    )
    .fields(
      field.httpUrl(
        "http://host.docker.internal:80/anything/pets/{id}", //url
        HttpMethodEnum.GET, //method
        List(
          field.name("id") //path parameters
        ),
        List(
          field.name("limit").`type`(IntegerType).min(1).max(10) //query parameters
        )
      )
    )
    .validations(
      validation.field("request.method").isEqual("GET"),
      validation.field("request.method").isEqualField("response.statusText"),
      validation.field("response.statusCode").isEqual(200),
      validation.field("response.headers.Content-Length").greaterThan(0),
      validation.field("response.headers.Content-Type").isEqual("application/json"),
    )

  val myPlan = plan.addForeignKeyRelationship(
    foreignField(httpPostTask, "body.id"),
    foreignField(httpGetTask, "id")
  )

  val conf = configuration.enableGeneratePlanAndTasks(true)
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(myPlan, conf, httpPostTask, httpGetTask)
}
