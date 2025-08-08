package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.List;
import java.util.Map;

public class AdvancedBatchEventJavaPlanRun extends PlanRun {
    {
        var kafkaTask = new KafkaJavaPlanRun().getKafkaTask();

        var csvTask = csv("my_csv", "/opt/app/data/csv/account")
                .fields(
                        field().name("account_number"),
                        field().name("year"),
                        field().name("name"),
                        field().name("payload")
                );

        var myPlan = plan().addForeignKeyRelationship(
                kafkaTask, List.of("key", "tmp_year", "tmp_name", "value"),
                List.of(Map.entry(csvTask, List.of("account_number", "year", "name", "payload")))
        );

        var conf = configuration()
                .generatedReportsFolderPath("/opt/app/data/report");

        execute(myPlan, conf, kafkaTask, csvTask);
    }
}
