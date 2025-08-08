package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DateType, DecimalType, TimestampType}

class DeltaLakePlan extends PlanRun {

  val accountTask = delta("customer_accounts", "/opt/app/data/customer/delta")
    .fields(
      field.name("account_id").regex("ACC[0-9]{8}").unique(true),
      field.name("balance").`type`(new DecimalType(5, 2)).min(1).max(1000),
      field.name("created_by").sql("CASE WHEN status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
      field.name("name").expression("#{Name.name}"),
      field.name("open_time").`type`(TimestampType).min(java.sql.Date.valueOf("2022-01-01")),
      field.name("status").oneOf("open", "closed", "suspended", "pending")
    )
    .count(count.records(100))

  val transactionTask = delta("customer_transactions", "/opt/app/data/customer/delta")
    .fields(
      field.name("account_id"),
      field.name("full_name"),
      field.name("amount").`type`(new DecimalType(4, 2)).min(1).max(100),
      field.name("time").`type`(TimestampType).min(java.sql.Date.valueOf("2022-01-01")),
      field.name("date").`type`(DateType).sql("DATE(time)")
    )
    .count(count.recordsPerFieldGenerator(generator.min(1).max(5), "account_id", "full_name"))

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableUniqueCheck(true)

  val myPlan = plan.addForeignKeyRelationship(
    accountTask, List("account_id", "name"),
    List(transactionTask -> List("account_id", "full_name"))
  )

  execute(myPlan, config, accountTask, transactionTask)
}
