package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

class DataContractCliPlanRun extends PlanRun {

  val accountTask = csv("customer_accounts", "/opt/app/data/customer/account-datacontract-cli", Map("header" -> "true"))
    .fields(metadataSource.dataContractCli("/opt/app/mount/datacontract-cli/datacontract.yaml"))
    .fields(
      field.name("latitude").min(-90).max(90),
      field.name("longitude").min(-180).max(180),
      field.name("country_region").expression("#{Address.state}")
    )
    .count(count.records(100))

  val conf = configuration.enableGeneratePlanAndTasks(true)
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(conf, accountTask)
}
