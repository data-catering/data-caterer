package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

class AdvancedCassandraPlanRun extends PlanRun {

  val accountTask = cassandra("customer_cassandra", "host.docker.internal:9042")
    .schema(field.name("account_id").regex("ACC[0-9]{8}"))

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableGeneratePlanAndTasks(true)
    .enableRecordTracking(true)
    .enableDeleteGeneratedRecords(true)
    .enableGenerateData(false)

  execute(config, accountTask)
}
