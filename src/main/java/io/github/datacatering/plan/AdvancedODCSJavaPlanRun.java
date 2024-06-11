package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.Map;

public class AdvancedODCSJavaPlanRun extends PlanRun {
    {
        var accountTask = csv("my_csv", "/opt/app/data/account-odcs", Map.of("saveMode", "overwrite", "header", "true"))
                .schema(metadataSource().openDataContractStandard("/opt/app/mount/odcs/full-example.yaml"))
                .count(count().records(100));

        var conf = configuration().enableGeneratePlanAndTasks(true)
                .enableGenerateValidations(true)
                .generatedReportsFolderPath("/opt/app/data/report");

        execute(conf, accountTask);
    }
}
