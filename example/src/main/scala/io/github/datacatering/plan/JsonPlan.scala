package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DateType, DecimalType, DoubleType, IntegerType, TimestampType}

class JsonPlan extends PlanRun {

  val accountTask = json("customer_accounts", "/opt/app/data/customer/account_json")
    .fields(
      field.name("account_id").regex("ACC[0-9]{8}").unique(true),
      field.name("balance").`type`(new DecimalType(5, 2)).min(1).max(1000),
      field.name("created_by").sql("CASE WHEN status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
      field.name("open_time").`type`(TimestampType).min(java.sql.Date.valueOf("2022-01-01")),
      field.name("status").oneOf("open", "closed", "suspended", "pending"),
      field.name("customer_details")
        .fields(
          field.name("name").sql("tmp_name"),
          field.name("age").`type`(IntegerType).min(18).max(90),
          field.name("city").expression("#{Address.city}")
        ),
      field.name("tmp_name").expression("#{Name.name}").omit(true)
    )

  val transactionTask = json("customer_transactions", "/opt/app/data/customer/transaction_json")
    .fields(
      field.name("account_id"),
      field.name("full_name"),
      field.name("amount").`type`(DoubleType).min(1).max(100),
      field.name("time").`type`(TimestampType).min(java.sql.Date.valueOf("2022-01-01")),
      field.name("date").`type`(DateType).sql("DATE(time)")
    )
    .count(count.recordsPerFieldGenerator(generator.min(0).max(5), "account_id", "full_name"))

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableUniqueCheck(true)

  val myPlan = plan.addForeignKeyRelationship(
    accountTask, List("account_id", "tmp_name"), //the task and columns we want linked
    List(transactionTask -> List("account_id", "full_name")) //list of other tasks and their respective column names we want matched
  )

  execute(myPlan, config, accountTask, transactionTask)
}
