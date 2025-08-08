package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DoubleType, IntegerType, TimestampType}

import java.sql.Date
import java.time.LocalDate

class PostgresPlanRun extends PlanRun {

  val accountTask = postgres("customer_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")
    .table("account", "accounts")
    .fields(
      field.name("account_number").regex("[0-9]{10}").unique(true),
      field.name("customer_id_int").`type`(IntegerType).min(1).max(1000),
      field.name("created_by").expression("#{Name.name}"),
      field.name("created_by_fixed_length").sql("CASE WHEN account_status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
      field.name("open_timestamp").`type`(TimestampType).min(Date.valueOf(LocalDate.now())),
      field.name("account_status").oneOf("open", "closed", "suspended", "pending")
    )
    .count(count.records(100))

  val balancesTask = postgres(accountTask)
    .table("account", "balances")
    .fields(
      field.name("account_number").regex("[0-9]{10}").unique(true),
      field.name("create_time").`type`(TimestampType),
      field.name("account_status").oneOf("open", "closed", "suspended", "pending"),
      field.name("balance").`type`(DoubleType)
    )

  val transactionsTask = postgres(accountTask)
    .table("account", "transactions")
    .fields(
      field.name("account_number"),
      field.name("create_time").`type`(TimestampType),
      field.name("transaction_id"),
      field.name("amount").`type`(DoubleType)
    )

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableUniqueCheck(true)

  val myPlan = plan.addForeignKeyRelationship(
    accountTask, "account_number",
    List(transactionsTask -> "account_number", balancesTask -> "account_number")
  )

  execute(myPlan, config, accountTask, balancesTask, transactionsTask)
}
