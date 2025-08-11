package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.HttpMethodEnum;
import io.github.datacatering.datacaterer.api.HttpQueryParameterStyleEnum;
import io.github.datacatering.datacaterer.api.model.ArrayType;
import io.github.datacatering.datacaterer.api.model.DoubleType;
import io.github.datacatering.datacaterer.api.model.IntegerType;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.List;
import java.util.Map;

public class FastGenerationAndReferenceJavaPlanRun extends PlanRun {
    {
        String base = "/opt/app/data";

        // Step demonstrating reference mode (no generation; use existing data)
        var referenceJson = json("reference_json", base + "/reference/json")
                .enableReferenceMode(true);

        // Step demonstrating unwrap top-level array
        var jsonWithArray = json("json_array", base + "/json-array")
                .fields(
                        field().name("items").type(ArrayType.instance()).unwrapTopLevelArray(true)
                                .fields(
                                        field().name("id"),
                                        field().name("score").type(DoubleType.instance()).min(0).max(100)
                                )
                )
                .count(count().records(5));

        // HTTP with query parameter style example
        var httpStyled = http("my_http")
                .fields(
                        field().httpUrl(
                                "http://host.docker.internal:80/anything/pets",
                                HttpMethodEnum.GET(),
                                List.of(),
                                List.of(
                                        field().httpQueryParam("tags", ArrayType.instance(), HttpQueryParameterStyleEnum.FORM(), false),
                                        field().httpQueryParam("limit", IntegerType.instance())
                                )
                        )
                )
                .rowsPerSecond(1);

        // CSV showing partitioning and per-step controls
        var csvStep = csv("csv_accounts", base + "/csv/accounts", Map.of("header", "true"))
                .partitionBy("year", "account_id")
                .numPartitions(4)
                .fields(
                        field().name("account_id"),
                        field().name("year").type(IntegerType.instance()).min(2020).max(2025)
                )
                .count(count().records(20));

        // Cassandra with key positions
        var cassandraStep = cassandra("customer_cassandra", "host.docker.internal:9042")
                .table("account", "accounts")
                .fields(
                        field().name("account_id").primaryKey(true).primaryKeyPosition(1),
                        field().name("open_time").clusteringPosition(1)
                )
                .count(count().records(10));

        // Enable fast generation globally
        var conf = configuration()
                .enableFastGeneration(true);

        execute(conf, referenceJson, jsonWithArray, httpStyled, csvStep, cassandraStep);
    }
}