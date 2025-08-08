package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{ArrayType, DateType, DoubleType, IntegerType}

import java.sql.Date

class MultipleDataSourcePlanRun extends PlanRun {

  val startDate = Date.valueOf("2022-01-01")
  val accountIdField = field.name("account_id").regex("ACC[0-9]{8}")
  val nameField = field.name("name").expression("#{Name.name}")

  val postgresTransactionTask = postgres("customer_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")
    .table("account.transaction")
    .fields(
      accountIdField,
      field.name("txn_id").regex("txn_[0-9]{5}"),
      field.name("year").`type`(IntegerType).sql("YEAR(date)"),
      nameField,
      field.name("date").`type`(DateType).min(startDate),
      field.name("amount").`type`(DoubleType).max(10000),
      field.name("credit_debit").sql("CASE WHEN amount < 0 THEN 'C' ELSE 'D' END"),
    )

  val postgresAccountTask = postgres(postgresTransactionTask)
    .table("account.account")
    .fields(
      accountIdField,
      nameField,
      field.name("open_date").`type`(DateType).min(startDate),
      field.name("status").oneOf("open", "closed", "pending")
    )

  val jsonTask = json("account_json", "src/main/resources/sample/json")
    .fields(
      accountIdField,
      nameField,
      field.name("txn_list")
        .`type`(ArrayType)
        .fields(
          field.name("id"),
          field.name("date").`type`(DateType).min(startDate),
          field.name("amount").`type`(DoubleType),
        )
    )

  execute(postgresTransactionTask, postgresAccountTask, jsonTask)
}
