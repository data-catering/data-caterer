package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DoubleType, IntegerType, TimestampType}

import java.sql.Date
import java.time.LocalDate

class BigQueryPlanRun extends PlanRun {

  val accountTask = bigquery("customer_bigquery", "gs://<my-bucket-name>/temp-data-gen")
    .table("<project>.data_caterer_test.accounts")
    .fields(
      field.name("account_number").regex("[0-9]{10}").unique(true),
      field.name("customer_id_int").`type`(IntegerType).min(1).max(1000),
      field.name("created_by").expression("#{Name.name}"),
      field.name("created_by_fixed_length").sql("CASE WHEN account_status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
      field.name("open_timestamp").`type`(TimestampType).min(Date.valueOf(LocalDate.now())),
      field.name("account_status").oneOf("open", "closed", "suspended", "pending")
    )
    .count(count.records(10))

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableUniqueCheck(true)

  execute(config, accountTask)
}
