package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DateType, DoubleType, TimestampType}

import java.sql.Date

class CassandraPlanRun extends PlanRun {

  val accountTask = cassandra("customer_cassandra", "host.docker.internal:9042")
    .table("account", "accounts")
    .fields(
      field.name("account_id").regex("ACC[0-9]{8}").unique(true),
      field.name("amount").`type`(DoubleType).min(1).max(1000),
      field.name("created_by").sql("CASE WHEN status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
      field.name("name").expression("#{Name.name}"),
      field.name("open_time").`type`(TimestampType).min(Date.valueOf("2022-01-01")),
      field.name("status").oneOf("open", "closed", "suspended", "pending")
    )
    .count(count.records(100))

  val accountHistoryTask = cassandra(accountTask)
    .table("account", "account_status_history")
    .fields(
      field.name("account_id"),
      field.name("eod_date").`type`(DateType),
      field.name("status"),
      field.name("updated_by"),
      field.name("updated_time").`type`(TimestampType)
    )
    .count(count.recordsPerFieldGenerator(generator.min(0).max(5), "account_id"))

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableUniqueCheck(true)

  val myPlan = plan
    .addForeignKeyRelationship(accountTask, "account_id", List(accountHistoryTask -> "account_id"))

  execute(myPlan, config, accountTask, accountHistoryTask)
}
