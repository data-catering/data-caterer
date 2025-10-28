package io.github.datacatering.plan.benchmark

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{ArrayType, DateType, DoubleType, IntegerType, TimestampType}

import java.sql.{Date, Timestamp}

class BenchmarkJsonFastPlanRun extends PlanRun {

  val recordCount = System.getenv("RECORD_COUNT").toInt

  val baseFolder = "/opt/app/data"
  val accountStatus = List("open", "closed", "pending", "suspended")
  val jsonTask = json("account_info", s"$baseFolder/json", Map("saveMode" -> "overwrite"))
    .fields(
      field.name("account_id").regex("ACC[0-9]{8}"),
      field.name("year").`type`(IntegerType).sql("YEAR(date)"),
      field.name("balance").`type`(DoubleType).min(10).max(1000),
      field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
      field.name("status").sql("element_at(sort_array(update_history, false), 1).status"),
      field.name("update_history")
        .`type`(ArrayType)
        .arrayMinLength(1)
        .fields(
          field.name("updated_time").`type`(TimestampType).min(Timestamp.valueOf("2022-01-01 00:00:00")),
          field.name("status").oneOf(accountStatus),
        ),
      field.name("customer_details")
        .fields(
          field.name("name").sql("_join_txn_name"),
          field.name("age").`type`(IntegerType).min(18).max(90),
          field.name("city").expression("#{Address.city}")
        ),
      field.name("_join_txn_name").expression("#{Name.name}").omit(true)
    )
    .count(count.records(recordCount))

  val conf = configuration
    .enableSaveReports(false)
    .enableCount(false)
    .enableFastGeneration(true)

  val basePlan = plan.seed(1)

  execute(basePlan, conf, jsonTask)
}
