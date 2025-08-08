package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

public class DeleteJavaPlanRun extends PlanRun {
    {
        var autoRun = configuration()
                .postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")
                .enableGeneratePlanAndTasks(true)
                .enableRecordTracking(true)
                .enableDeleteGeneratedRecords(false)
                .enableUniqueCheck(true)
                .generatedPlanAndTaskFolderPath("/opt/app/data/generated")
                .recordTrackingFolderPath("/opt/app/data/recordTracking")
                .generatedReportsFolderPath("/opt/app/data/report");

        execute(autoRun);
    }
}
