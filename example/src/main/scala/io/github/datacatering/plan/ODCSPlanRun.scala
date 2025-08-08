package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

class ODCSPlanRun extends PlanRun {

  val accountTask = csv("customer_accounts", "/opt/app/data/customer/account-odcs", Map("header" -> "true"))
    .fields(metadataSource.openDataContractStandard("/opt/app/mount/odcs/full-example.odcs.yaml"))
    .count(count.records(100))

  val conf = configuration.enableGeneratePlanAndTasks(true)
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(conf, accountTask)
}
