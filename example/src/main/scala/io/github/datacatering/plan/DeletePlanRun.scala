package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

/**
 * Generate data for all tables in Postgres
 */
class DeletePlanRun extends PlanRun {

  val autoRun = configuration
    .postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")
    .enableGeneratePlanAndTasks(true)
    .enableRecordTracking(true)
    .enableDeleteGeneratedRecords(false)
    .enableUniqueCheck(true)
    .generatedPlanAndTaskFolderPath("/opt/app/data/generated")
    .recordTrackingFolderPath("/opt/app/data/recordTracking")
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableGenerateData(true)

  execute(configuration = autoRun)
}
