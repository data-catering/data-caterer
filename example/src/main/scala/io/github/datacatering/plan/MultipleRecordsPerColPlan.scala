package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DateType, DoubleType, TimestampType}

class MultipleRecordsPerColPlan extends PlanRun {

  val transactionTask = csv("customer_transactions", "/opt/app/data/customer/transaction", Map("header" -> "true"))
    .fields(
      field.name("account_id").regex("ACC[0-9]{8}"),
      field.name("full_name").expression("#{Name.name}"),
      field.name("amount").`type`(DoubleType.instance).min(1).max(100),
      field.name("time").`type`(TimestampType.instance).min(java.sql.Date.valueOf("2022-01-01")),
      field.name("date").`type`(DateType.instance).sql("DATE(time)")
    )
    .count(count.recordsPerFieldGenerator(generator.min(0).max(5), "account_id", "full_name"))

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(config, transactionTask)
}
