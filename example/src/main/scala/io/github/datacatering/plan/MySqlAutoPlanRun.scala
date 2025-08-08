package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

class MySqlAutoPlanRun extends PlanRun {

  val accountTask = mysql("customer_mysql", "jdbc:mysql://host.docker.internal:3306/customer")
    .fields(field.name("account_number").regex("[0-9]{10}"))
    .count(count.records(100))

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableGeneratePlanAndTasks(true)
    .enableRecordTracking(true)
    .enableDeleteGeneratedRecords(true)
    .enableGenerateData(false)

  execute(config, accountTask)
}
