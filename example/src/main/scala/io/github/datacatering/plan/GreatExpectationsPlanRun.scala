package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DoubleType, IntegerType, TimestampType}

class GreatExpectationsPlanRun extends PlanRun {

  val greatExpectationsSource = metadataSource.greatExpectations("/opt/app/mount/ge/taxi-expectations.json")

  val jsonTask = json("my_json", "/opt/app/data/taxi_json", Map("saveMode" -> "overwrite"))
    .fields(
      field.name("vendor_id"),
      field.name("pickup_datetime").`type`(TimestampType),
      field.name("dropoff_datetime").`type`(TimestampType),
      field.name("passenger_count").`type`(IntegerType),
      field.name("trip_distance").`type`(DoubleType),
      field.name("rate_code_id"),
      field.name("store_and_fwd_flag"),
      field.name("pickup_location_id"),
      field.name("dropoff_location_id"),
      field.name("payment_type"),
      field.name("fare_amount").`type`(DoubleType),
      field.name("extra"),
      field.name("mta_tax").`type`(DoubleType),
      field.name("tip_amount").`type`(DoubleType),
      field.name("tolls_amount").`type`(DoubleType),
      field.name("improvement_surcharge").`type`(DoubleType),
      field.name("total_amount").`type`(DoubleType),
      field.name("congestion_surcharge").`type`(DoubleType),
    )
    .validations(greatExpectationsSource)
    .validations(validation.field("trip_distance").lessThan(500))

  val conf = configuration
    .enableGenerateValidations(true)
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(conf, jsonTask)
}
