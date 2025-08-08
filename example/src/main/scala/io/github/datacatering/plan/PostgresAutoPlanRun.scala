package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun

class PostgresAutoPlanRun extends PlanRun {

  val postgresTask = postgres("customer_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")
  val transactionTask = postgres(postgresTask).table("account.transactions")
    .count(count.recordsPerFieldGenerator(generator.min(1).max(10), "account_number"))
  val config = configuration.enableGeneratePlanAndTasks(true)

  execute(config, postgresTask, transactionTask)
}
