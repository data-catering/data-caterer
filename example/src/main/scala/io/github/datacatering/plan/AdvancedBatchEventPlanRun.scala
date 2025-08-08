package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

class AdvancedBatchEventPlanRun extends PlanRun {

  val kafkaTask = new KafkaPlanRun().kafkaTask

  val csvTask = csv("my_csv", "/opt/app/data/csv/account")
    .fields(
      field.name("account_number"),
      field.name("year"),
      field.name("name"),
      field.name("payload")
    )

  val myPlan = plan.addForeignKeyRelationship(
    kafkaTask, List("key", "tmp_year", "tmp_name", "value"),
    List(csvTask -> List("account_number", "year", "name", "payload"))
  )

  val conf = configuration.generatedReportsFolderPath("/opt/app/data/report")

  execute(myPlan, conf, kafkaTask, csvTask)
}
