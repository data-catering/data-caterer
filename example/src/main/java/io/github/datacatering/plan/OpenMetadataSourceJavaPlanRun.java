package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.model.Constants;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.Map;

public class OpenMetadataSourceJavaPlanRun extends PlanRun {
    {
        var jsonTask = json("my_json", "/opt/app/data/json", Map.of("saveMode", "overwrite"))
                .fields(metadataSource().openMetadataJava(
                        "http://host.docker.internal:5001",
                        Constants.OPEN_METADATA_AUTH_TYPE_OPEN_METADATA(),
                        Map.of(
                                Constants.OPEN_METADATA_JWT_TOKEN(), "abc123",
                                Constants.OPEN_METADATA_TABLE_FQN(), "sample_data.ecommerce_db.shopify.raw_customer"
                        )
                ))
                .fields(
                        field().name("platform").oneOf("website", "mobile"),
                        field().name("customer").fields(field().name("sex").oneOf("M", "F", "O"))
                )
                .count(count().records(10));

        var conf = configuration().enableGeneratePlanAndTasks(true)
                .enableGenerateValidations(true)
                .generatedReportsFolderPath("/opt/app/data/report");

        execute(conf, jsonTask);
    }
}
