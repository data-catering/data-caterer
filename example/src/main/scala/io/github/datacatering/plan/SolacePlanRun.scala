package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{ArrayType, DateType, DoubleType, IntegerType, TimestampType}

import java.sql.Date

class SolacePlanRun extends PlanRun {

  val solaceTask = solace("my_solace", "smf://host.docker.internal:55554")
    .destination("/JNDI/T/rest_test_topic")
    .fields(
      //field.name("partition").`type`(IntegerType), //can define message JMS priority here
      field.messageHeaders(
        field.messageHeader("account-id", "body.account_id"),
        field.messageHeader("updated", "body.details.updated_by.time"),
      ),
    )
    .fields(
      field.messageBody(
        field.name("account_id").regex("ACC[0-9]{8}"),
        field.name("year").`type`(IntegerType).min(2021).max(2023),
        field.name("account_status").oneOf("open", "closed", "suspended", "pending"),
        field.name("amount").`type`(DoubleType),
        field.name("details")
          .fields(
            field.name("name").expression("#{Name.name}"),
            field.name("first_txn_date").`type`(DateType).sql("ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"),
            field.name("updated_by")
              .fields(
                field.name("user"),
                field.name("time").`type`(TimestampType),
              ),
          ),
        field.name("transactions").`type`(ArrayType)
          .fields(
            field.name("txn_date").`type`(DateType).min(Date.valueOf("2021-01-01")).max("2021-12-31"),
            field.name("amount").`type`(DoubleType),
          )
      )
    ).count(count.records(10))

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(config, solaceTask)
}
