package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

class CsvGenerateIcebergValidatePlan extends PlanRun {

  val accountTask = csv("csv_accounts", "/opt/app/data/csv/customer/account", Map("header" -> "true"))
    .fields(metadataSource.openDataContractStandard("/opt/app/mount/odcs/full-example.odcs.yaml"))

  val validationTask = iceberg("iceberg_accounts", "dev.transactions", "/opt/app/data/iceberg/customer/transaction")
    .validations(
      validation.unique("account_id"),
      validation.groupBy("account_id").sum("balance").greaterThan(0),
      validation.field("open_time").isIncreasing(),
      validation.count().isEqual(1000),
      validation.preFilter(validation.field("status").isEqual("closed")).field("balance").isEqual(0),
      validation.upstreamData(accountTask)
        .joinFields("account_id")
        .validations(
          validation.field("open_time").isEqualField("csv_accounts.open_time"),
          validation.groupBy("account_id", "csv_accounts_balance").sum("amount").isEqualField("csv_accounts_balance")
        )
    )
    .validationWait(waitCondition.file("/opt/app/data/iceberg/customer/transaction"))

  val config = configuration.generatedReportsFolderPath("/opt/app/data/report")

  execute(config, validationTask)
}
