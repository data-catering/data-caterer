package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.model.DoubleType;
import io.github.datacatering.datacaterer.api.model.IntegerType;
import io.github.datacatering.datacaterer.api.model.TimestampType;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.Map;

public class GreatExpectationsJavaPlanRun extends PlanRun {
    {
        var greatExpectations = metadataSource().greatExpectations("/opt/app/mount/ge/taxi-expectations.json");

        var jsonTask = json("my_json", "/opt/app/data/json", Map.of("saveMode", "overwrite"))
                .fields(
                        field().name("vendor_id"),
                        field().name("pickup_datetime").type(TimestampType.instance()),
                        field().name("dropoff_datetime").type(TimestampType.instance()),
                        field().name("passenger_count").type(IntegerType.instance()),
                        field().name("trip_distance").type(DoubleType.instance()),
                        field().name("rate_code_id"),
                        field().name("store_and_fwd_flag"),
                        field().name("pickup_location_id"),
                        field().name("dropoff_location_id"),
                        field().name("payment_type"),
                        field().name("fare_amount").type(DoubleType.instance()),
                        field().name("extra"),
                        field().name("mta_tax").type(DoubleType.instance()),
                        field().name("tip_amount").type(DoubleType.instance()),
                        field().name("tolls_amount").type(DoubleType.instance()),
                        field().name("improvement_surcharge").type(DoubleType.instance()),
                        field().name("total_amount").type(DoubleType.instance()),
                        field().name("congestion_surcharge").type(DoubleType.instance())
                )
                .validations(greatExpectations)
                .validations(validation().field("trip_distance").lessThan(500));

        var conf = configuration().enableGeneratePlanAndTasks(true)
                .enableGenerateValidations(true)
                .generatedReportsFolderPath("/opt/app/data/report");

        execute(conf, jsonTask);
    }
}
