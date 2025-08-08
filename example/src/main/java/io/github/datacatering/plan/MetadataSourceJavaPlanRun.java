package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.List;
import java.util.Map;

public class MetadataSourceJavaPlanRun extends PlanRun {
    {
        var csvTask = csv("my_csv", "/opt/app/data/csv", Map.of("saveMode", "overwrite", "header", "true"))
                .fields(metadataSource().marquez("http://host.docker.internal:5001", "food_delivery", "public.delivery_7_days"))
                .count(count().records(10));

        var postgresTask = postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/food_delivery", "postgres", "password", Map.of())
                .fields(metadataSource().marquez("http://host.docker.internal:5001", "food_delivery"))
                .count(count().records(10));

        var foreignCols = List.of("order_id", "order_placed_on", "order_dispatched_on", "order_delivered_on", "customer_email",
                "customer_address", "menu_id", "restaurant_id", "restaurant_address", "menu_item_id", "category_id", "discount_id",
                "city_id", "driver_id");

        var myPlan = plan().addForeignKeyRelationships(
                csvTask, foreignCols,
                List.of(foreignField(postgresTask, "public.delivery_7_days", foreignCols))
        );

        var conf = configuration().enableGeneratePlanAndTasks(true)
                .generatedReportsFolderPath("/opt/app/data/report");

        execute(myPlan, conf, csvTask, postgresTask);
    }
}
