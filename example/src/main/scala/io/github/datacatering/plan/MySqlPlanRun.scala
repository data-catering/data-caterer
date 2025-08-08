package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DoubleType, IntegerType, TimestampType}

import java.sql.Date

class MySqlPlanRun extends PlanRun {

  val accountTask = mysql("customer_mysql", "jdbc:mysql://host.docker.internal:3306/customer")
    .table("customer", "accounts")
    .fields(
      field.name("id").`type`(IntegerType).sql("ROW_NUMBER() OVER (ORDER BY account_number)"),
      field.name("account_number").regex("[0-9]{10}").unique(true),
      field.name("customer_id_int").`type`(IntegerType).min(1).max(1000),
      field.name("created_by").expression("#{Name.name}"),
      field.name("created_by_fixed_length").sql("CASE WHEN account_status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
      field.name("open_timestamp").`type`(TimestampType).min(Date.valueOf("2022-01-01")),
      field.name("account_status").oneOf("open", "closed", "suspended", "pending")
    )
    .count(count.records(10))

  val balancesTask = mysql(accountTask)
    .table("customer", "balances")
    .fields(
      field.name("id"),
      field.name("create_time").`type`(TimestampType),
      field.name("balance").`type`(DoubleType)
    )

  val transactionsTask = mysql(accountTask)
    .table("customer", "transactions")
    .fields(
      field.name("id"),
      field.name("create_time").`type`(TimestampType),
      field.name("transaction_id"),
      field.name("amount").`type`(DoubleType)
    )

  val myPlan = plan.addForeignKeyRelationship(
    accountTask, "id",
    List(transactionsTask -> "id", balancesTask -> "id")
  )

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableUniqueCheck(true)

  execute(myPlan, config, accountTask, balancesTask, transactionsTask)
}
