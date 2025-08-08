package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.DoubleType

class ODCSV3PlanRun extends PlanRun {

  val accountTask = csv("customer_accounts", "/opt/app/data/customer/account-odcs-v3", Map("header" -> "true", "saveMode" -> "overwrite"))
    .fields(metadataSource.openDataContractStandard("/opt/app/mount/odcs/full-example-v3.odcs.yaml"))
    .fields(
      field.name("rcvr_cntry_code").expression("#{Country.countryCode3}"),
      field.name("id").regex("ACC[0-9]{8}"),
      field.name("balance").`type`(DoubleType).round(2),
      field.name("status").oneOf("open", "closed"),
    )
    .count(count.records(100))

  val conf = configuration.enableGeneratePlanAndTasks(true)
    .enableRecordTracking(true)
    .enableGenerateData(true)
    .enableDeleteGeneratedRecords(false)
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(conf, accountTask)
}
