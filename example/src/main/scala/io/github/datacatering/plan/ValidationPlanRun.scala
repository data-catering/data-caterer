package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{ArrayType, DateType, DoubleType, IntegerType, TimestampType}

import java.sql.{Date, Timestamp}

class ValidationPlanRun extends PlanRun {

  val baseSchema = List(
    field.name("account_id").regex("ACC[0-9]{8}"),
    field.name("year").`type`(IntegerType).sql("YEAR(date)"),
    field.name("balance").`type`(DoubleType).min(10).max(1000),
    field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
    field.name("status").oneOf("open", "closed"),
    field.name("update_history")
      .`type`(ArrayType)
      .fields(
        field.name("updated_time").`type`(TimestampType).min(Timestamp.valueOf("2022-01-01 00:00:00")),
        field.name("prev_status").oneOf("open", "closed"),
        field.name("new_status").oneOf("open", "closed")
      ),
    field.name("customer_details")
      .fields(
        field.name("name").expression("#{Name.name}"),
        field.name("age").`type`(IntegerType).min(18).max(90),
        field.name("city").expression("#{Address.city}")
      )
  )

  val firstJsonTask = json("my_first_json", "/opt/app/data/first_json", Map("saveMode" -> "overwrite"))
    .fields(baseSchema)
    .count(count.records(10))

  val thirdJsonTask = json("my_third_json", "/opt/app/data/third_json", Map("saveMode" -> "overwrite"))
    .fields(
      field.name("account_id"),
      field.name("amount").`type`(IntegerType).min(1).max(100),
      field.name("name").expression("#{Name.name}"),
    )
    .count(count.records(10).recordsPerField(3, "account_id"))

  val jsonValidationTask = json("my_json", "/opt/app/data/json", Map("saveMode" -> "overwrite"))
    .fields(baseSchema)
    .count(count.records(10))
    .validations(
      validation.fieldNames.countEqual(10),
      validation.fieldNames.countBetween(1, 3),
      validation.fieldNames.matchOrder("account_id", "year", "date"),
      validation.fieldNames.matchSet("account_id", "status"),
      validation.field("customer_details.name").matches("[A-Z][a-z]+ [A-Z][a-z]+").errorThreshold(0.1).description("Names generally follow the same pattern"),
      validation.field("date").isNull(true).errorThreshold(10),
      validation.field("balance").greaterThan(500),
      validation.expr("YEAR(date) == year"),
      validation.field("status").in("open", "closed", "pending").errorThreshold(0.2).description("Could be new status introduced"),
      validation.field("customer_details.age").greaterThan(18),
      validation.expr("FORALL(update_history, x -> x.updated_time > TIMESTAMP('2022-01-01 00:00:00'))"),
      validation.field("update_history").greaterThanSize(2),
      validation.unique("account_id"),
      validation.count().isEqual(100),
      validation.groupBy("account_id").max("balance").lessThan(900),
      validation.upstreamData(firstJsonTask)
        .joinFields("account_id")
        .validations(
          validation.field("my_first_json_customer_details.name")
            .isEqualField("customer_details.name")
        ),
      validation.upstreamData(thirdJsonTask)
        .joinExpr("account_id == my_third_json_account_id")
        .validations(
          validation.groupBy("account_id", "balance")
            .sum("my_third_json_amount")
            .betweenFields("balance * 0.8", "balance * 1.2")
        ),
      validation.upstreamData(firstJsonTask)
        .joinFields("account_id")
        .joinType("anti")
        .validations(validation.count().isEqual(0)),
      validation.upstreamData(firstJsonTask)
        .joinFields("account_id")
        .validations(validation.count().isEqual(10)),
      validation.upstreamData(firstJsonTask)
        .joinFields("account_id")
        .validations(
          validation.upstreamData(thirdJsonTask)
            .joinFields("account_id")
            .validations(validation.count().isEqual(30))
        )
    )

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableValidation(true)

  val foreignPlan = plan
    .addForeignKeyRelationship(firstJsonTask, "account_id", List(jsonValidationTask -> "account_id", thirdJsonTask -> "account_id"))

  execute(foreignPlan, config, firstJsonTask, jsonValidationTask, thirdJsonTask)
}
