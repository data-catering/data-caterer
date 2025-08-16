package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.{HttpMethodEnum, HttpQueryParameterStyleEnum, PlanRun}
import io.github.datacatering.datacaterer.api.model.{ArrayType, DoubleType, IntegerType}

class FastGenerationAndReferencePlanRun extends PlanRun {

  private val base = "/opt/app/data"

  // Step demonstrating reference mode (no generation; use existing data)
  private val referenceJson = json("reference_json", s"$base/reference/json")
    .enableReferenceMode(true)

  // Step demonstrating unwrap top-level array
  private val jsonWithArray = json("json_array", s"$base/json-array")
    .fields(
      field.name("items").`type`(ArrayType).unwrapTopLevelArray(true)
        .fields(
          field.name("id"),
          field.name("score").`type`(DoubleType).min(0).max(100)
        )
    )
    .count(count.records(5))

  // HTTP with query parameter style example
  private val httpStyled = http("my_http")
    .fields(
      field.httpUrl(
        "http://host.docker.internal:80/anything/pets",
        HttpMethodEnum.GET,
        List(),
        List(
          field.httpQueryParam("tags", ArrayType, HttpQueryParameterStyleEnum.FORM, explode = false),
          field.httpQueryParam("limit", IntegerType)
        )
      ): _*
    )
    .rowsPerSecond(1)

  // CSV showing partitioning and per-step controls
  private val csvStep = csv("csv_accounts", s"$base/csv/accounts", Map("header" -> "true"))
    .partitionBy("year", "account_id")
    .numPartitions(4)
    .fields(
      field.name("account_id"),
      field.name("year").`type`(IntegerType).min(2020).max(2025)
    )
    .count(count.records(20))

  // Cassandra with key positions
  private val cassandraStep = cassandra("customer_cassandra", "host.docker.internal:9042")
    .table("account", "accounts")
    .fields(
      field.name("account_id").primaryKey(true).primaryKeyPosition(1),
      field.name("open_time").clusteringPosition(1)
    )
    .count(count.records(10))

  // Enable fast generation globally
  private val conf = configuration
    .enableFastGeneration(true)

  execute(conf, referenceJson, jsonWithArray, httpStyled, csvStep, cassandraStep)
}


