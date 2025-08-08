package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

/**
 * Generate data for all tables in Postgres with foreign key relationships maintained
 */
class AdvancedAutomatedPlanRun extends PlanRun {

  val autoRun = configuration
    .postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")
    .enableUniqueCheck(true)
    .enableGeneratePlanAndTasks(true)
    .enableGenerateValidations(true)
    .generatedPlanAndTaskFolderPath("/opt/app/data/generated")
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(configuration = autoRun)
}

/**
 * Generate data in Postgres only for schema 'account' and table 'accounts'
 */
class AdvancedAutomatedWithFilterPlanRun extends PlanRun {

  val autoRun = configuration
    .postgres(
      "my_postgres",
      "jdbc:postgresql://host.docker.internal:5432/customer",
      options = Map("filterOutTable" -> "balances, transactions")
    )
    .enableGeneratePlanAndTasks(true)
    .generatedPlanAndTaskFolderPath("/opt/app/data/generated")
    .enableUniqueCheck(true)

  execute(configuration = autoRun)
}
