package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

public class AutomatedJavaPlanRun extends PlanRun {
    {
        var autoRun = configuration()
                .postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")
                .enableGeneratePlanAndTasks(true)
                .generatedPlanAndTaskFolderPath("/opt/app/data/generated")
                .enableUniqueCheck(true)
                .generatedReportsFolderPath("/opt/app/data/report");

        execute(autoRun);
    }
}
