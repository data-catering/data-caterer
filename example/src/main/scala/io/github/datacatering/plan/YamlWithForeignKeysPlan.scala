package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.DateType

/**
 * Example 4: Mix YAML and programmatic foreign key relationships
 */
class YamlWithForeignKeysPlan extends PlanRun {

  // Load account schema from YAML
  val accountTask = json("accounts", "/tmp/accounts")
    .fields(metadataSource.yamlTask(
      "app/src/test/resources/sample/task/file/simple-json-task.yaml",
      "simple_json",
      "file_account"
    ))

  // Create related transaction task programmatically
  val transactionTask = json("transactions", "/tmp/transactions")
    .fields(
      field.name("transaction_id").regex("TXN-[0-9]{8}"),
      field.name("account_id"),  // Will be linked to accounts
      field.name("amount").min(10.0).max(1000.0),
      field.name("transaction_date").`type`(DateType)
    )

  // Set up foreign key relationship
  val planWithRelations = plan
    .name("yaml-with-relationships")
    .addForeignKeyRelationship(transactionTask, "account_id", List((accountTask, "account_id")))

  execute(planWithRelations, configuration, List(), accountTask, transactionTask)
}