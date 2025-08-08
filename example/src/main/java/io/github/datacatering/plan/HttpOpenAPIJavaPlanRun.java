package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.model.Constants;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.Map;

public class HttpOpenAPIJavaPlanRun extends PlanRun {
    {
        var httpTask = http("my_http", Map.of(Constants.ROWS_PER_SECOND(), "1"))
                .fields(metadataSource().openApi("/opt/app/mount/http/petstore.json"))
                .fields(field().httpBody(field().name("id").regex("ID[0-9]{8}")))
                .count(count().records(2));

        var myPlan = plan().addForeignKeyRelationship(
                foreignField("my_http", "POST/pets", "body.id"),
                foreignField("my_http", "GET/pets/{id}", "pathParamid"),
                foreignField("my_http", "DELETE/pets/{id}", "pathParamid")
        );

        var conf = configuration().enableGeneratePlanAndTasks(true)
                .generatedReportsFolderPath("/opt/app/data/report");

        execute(myPlan, conf, httpTask);
    }
}
