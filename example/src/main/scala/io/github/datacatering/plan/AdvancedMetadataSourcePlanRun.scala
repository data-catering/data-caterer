package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

/**
 * Generate data for all tables in Postgres
 */
class AdvancedMetadataSourcePlanRun extends PlanRun {

  val csvTask = csv("my_csv", "/opt/app/data/csv", Map("saveMode" -> "overwrite", "header" -> "true"))
    .fields(metadataSource.marquez("http://host.docker.internal:5001", "food_delivery", "public.delivery_7_days"))
    .count(count.records(10))

  val postgresTask = postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/food_delivery", "postgres", "password")
    .fields(metadataSource.marquez("http://host.docker.internal:5001", "food_delivery"))
    .count(count.records(10))

  val foreignCols = List("order_id", "order_placed_on", "order_dispatched_on", "order_delivered_on", "customer_email",
    "customer_address", "menu_id", "restaurant_id", "restaurant_address", "menu_item_id", "category_id", "discount_id",
    "city_id", "driver_id")

  val myPlan = plan.addForeignKeyRelationships(
    csvTask, foreignCols,
    List(foreignField(postgresTask, "food_delivery_public.delivery_7_days", foreignCols))
  )

  val conf = configuration.enableGeneratePlanAndTasks(true)
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(myPlan, conf, csvTask, postgresTask)
}
