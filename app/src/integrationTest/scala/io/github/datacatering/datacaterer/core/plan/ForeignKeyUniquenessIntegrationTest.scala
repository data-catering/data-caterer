package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.Constants.IS_UNIQUE
import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.util.SparkSuite

/**
 * Integration test demonstrating the real-world scenario where foreign key uniqueness is critical.
 *
 * Scenario:
 * - Source table (accounts) has a limited value space for FK field
 * - Without unique=true, duplicate FK values are generated
 * - With cardinality defined (e.g., 1:2 ratio), duplicate FKs cause unexpected record counts
 *
 * This test validates that ForeignKeyUniquenessProcessor correctly enforces uniqueness.
 */
class ForeignKeyUniquenessIntegrationTest extends SparkSuite {

  test("Limited value space with foreign key - uniqueness enforced") {
    // Setup: accounts with limited value space (only 10 possible account_ids)
    // Request 50 accounts, FK to transactions with 1:3 ratio
    // Expected: 50 unique accounts (through unique enforcement) -> 150 transactions

    val accountsStep = Step(
      name = "accounts",
      `type` = "csv",
      count = Count(Some(50)),
      options = Map("path" -> "/tmp/test_accounts"),
      fields = List(
        // Limited value space: only 10 possibilities (0-9)
        // Without unique=true, this would generate duplicates
        Field(name = "account_id", `type` = Some("string"), options = Map("regex" -> "[0-9]")),
        Field(name = "name", `type` = Some("string"), options = Map("expression" -> "#{Name.name}"))
      )
    )

    val transactionsStep = Step(
      name = "transactions",
      `type` = "csv",
      count = Count(Some(100)), // Initial count (will be adjusted to 150 by cardinality processor)
      options = Map("path" -> "/tmp/test_transactions"),
      fields = List(
        Field(name = "txn_id", `type` = Some("string"), options = Map("expression" -> "#{IdNumber.valid}")),
        Field(name = "account_id", `type` = Some("string")),
        Field(name = "amount", `type` = Some("double"), options = Map("min" -> "10.0", "max" -> "1000.0"))
      )
    )

    val accountsTask = Task(
      name = "accounts_task",
      steps = List(accountsStep)
    )

    val transactionsTask = Task(
      name = "transactions_task",
      steps = List(transactionsStep)
    )

    // Define FK with cardinality (1:3 ratio) at target level
    val foreignKey = ForeignKey(
      source = ForeignKeyRelation("my_accounts", "accounts", List("account_id")),
      generate = List(ForeignKeyRelation("my_transactions", "transactions", List("account_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(3.0), distribution = "uniform"))))
    )

    val plan = Plan(
      name = "test_plan",
      tasks = List(
        TaskSummary(name = "accounts_task", dataSourceName = "my_accounts", steps = Some(List(accountsStep))),
        TaskSummary(name = "transactions_task", dataSourceName = "my_transactions", steps = Some(List(transactionsStep)))
      ),
      sinkOptions = Some(SinkOptions(
        foreignKeys = List(foreignKey)
      ))
    )

    val dataCatererConfig = DataCatererConfiguration()

    // Apply processors in order
    val uniquenessProcessor = new ForeignKeyUniquenessProcessor(dataCatererConfig)
    val (planAfterUniqueness, tasksAfterUniqueness, validationsAfterUniqueness) =
      uniquenessProcessor.apply(plan, List(accountsTask, transactionsTask), List())

    val cardinalityProcessor = new CardinalityCountAdjustmentProcessor(dataCatererConfig)
    val (finalPlan, finalTasks, finalValidations) =
      cardinalityProcessor.apply(planAfterUniqueness, tasksAfterUniqueness, validationsAfterUniqueness)

    // Verify uniqueness was enforced on accounts
    val accountsTaskAfter = finalTasks.find(_.name == "accounts_task").get
    val accountIdField = accountsTaskAfter.steps.head.fields.find(_.name == "account_id").get

    assert(accountIdField.options.contains(IS_UNIQUE),
      "account_id should be marked as unique")
    assert(accountIdField.options(IS_UNIQUE).toString == "true",
      s"account_id should have unique=true, got ${accountIdField.options(IS_UNIQUE)}")

    // Verify cardinality adjustment happened correctly
    // With perField, records is set to SOURCE count (50), and perField count is set to ratio (3)
    // This generates 50 * 3 = 150 total records
    val transactionsTaskAfter = finalTasks.find(_.name == "transactions_task").get
    val transactionsCount = transactionsTaskAfter.steps.head.count.records.get

    assert(transactionsCount == 50,
      s"Transactions count should be 50 (source count, perField will multiply by 3), got $transactionsCount")

    // Verify perField configuration for FK grouping
    val perField = transactionsTaskAfter.steps.head.count.perField
    assert(perField.isDefined, "PerField should be configured for FK grouping")
    assert(perField.get.fieldNames.contains("account_id"),
      "PerField should include account_id")
    assert(perField.get.count.contains(3L),
      s"PerField count should be 3 (ratio), got ${perField.get.count}")
  }

  test("Multiple FK fields should all be marked unique") {
    // Scenario: composite FK with multiple fields
    val accountsStep = Step(
      name = "accounts",
      `type` = "csv",
      count = Count(Some(30)),
      fields = List(
        Field(name = "account_id", `type` = Some("string"), options = Map("regex" -> "[A-Z]{2}")),
        Field(name = "branch_id", `type` = Some("string"), options = Map("regex" -> "[0-9]{2}")),
        Field(name = "name", `type` = Some("string"))
      )
    )

    val transactionsStep = Step(
      name = "transactions",
      `type` = "csv",
      count = Count(Some(60)),
      fields = List(
        Field(name = "txn_id", `type` = Some("string")),
        Field(name = "account_id", `type` = Some("string")),
        Field(name = "branch_id", `type` = Some("string")),
        Field(name = "amount", `type` = Some("double"))
      )
    )

    val accountsTask = Task(name = "accounts_task", steps = List(accountsStep))
    val transactionsTask = Task(name = "transactions_task", steps = List(transactionsStep))

    // Composite FK (both account_id and branch_id) with target-level cardinality
    val foreignKey = ForeignKey(
      source = ForeignKeyRelation("my_accounts", "accounts", List("account_id", "branch_id")),
      generate = List(ForeignKeyRelation("my_transactions", "transactions", List("account_id", "branch_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(2.0), distribution = "uniform"))))
    )

    val plan = Plan(
      name = "test_plan",
      tasks = List(
        TaskSummary(name = "accounts_task", dataSourceName = "my_accounts", steps = Some(List(accountsStep))),
        TaskSummary(name = "transactions_task", dataSourceName = "my_transactions", steps = Some(List(transactionsStep)))
      ),
      sinkOptions = Some(SinkOptions(foreignKeys = List(foreignKey)))
    )

    val processor = new ForeignKeyUniquenessProcessor(DataCatererConfiguration())
    val (_, updatedTasks, _) = processor.apply(plan, List(accountsTask, transactionsTask), List())

    val accountsTaskAfter = updatedTasks.find(_.name == "accounts_task").get
    val accountIdField = accountsTaskAfter.steps.head.fields.find(_.name == "account_id").get
    val branchIdField = accountsTaskAfter.steps.head.fields.find(_.name == "branch_id").get

    // Both composite FK fields should be marked unique
    assert(accountIdField.options.get(IS_UNIQUE).exists(_.toString == "true"),
      "account_id should be marked unique")
    assert(branchIdField.options.get(IS_UNIQUE).exists(_.toString == "true"),
      "branch_id should be marked unique")

    // Non-FK field should NOT be marked unique
    val nameField = accountsTaskAfter.steps.head.fields.find(_.name == "name").get
    assert(!nameField.options.contains(IS_UNIQUE) || nameField.options(IS_UNIQUE).toString == "false",
      "name field should not be marked unique")
  }

  test("Processor preserves existing configuration while adding uniqueness") {
    // Verify that other field options are preserved
    val accountsStep = Step(
      name = "accounts",
      `type` = "csv",
      count = Count(Some(20)),
      fields = List(
        Field(
          name = "account_id",
          `type` = Some("string"),
          options = Map(
            "regex" -> "[A-Z][0-9]{3}",
            "enableFastRegex" -> "true",
            "customOption" -> "custom_value"
          )
        ),
        Field(name = "balance", `type` = Some("double"), options = Map("min" -> "100.0", "max" -> "10000.0"))
      )
    )

    val transactionsStep = Step(
      name = "transactions",
      `type` = "csv",
      count = Count(Some(40)),
      fields = List(
        Field(name = "txn_id", `type` = Some("string")),
        Field(name = "account_id", `type` = Some("string"))
      )
    )

    val accountsTask = Task(name = "accounts_task", steps = List(accountsStep))
    val transactionsTask = Task(name = "transactions_task", steps = List(transactionsStep))

    val foreignKey = ForeignKey(
      source = ForeignKeyRelation("my_accounts", "accounts", List("account_id")),
      generate = List(ForeignKeyRelation("my_transactions", "transactions", List("account_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(2.0), distribution = "uniform"))))
    )

    val plan = Plan(
      name = "test_plan",
      tasks = List(
        TaskSummary(name = "accounts_task", dataSourceName = "my_accounts", steps = Some(List(accountsStep))),
        TaskSummary(name = "transactions_task", dataSourceName = "my_transactions", steps = Some(List(transactionsStep)))
      ),
      sinkOptions = Some(SinkOptions(foreignKeys = List(foreignKey)))
    )

    val processor = new ForeignKeyUniquenessProcessor(DataCatererConfiguration())
    val (_, updatedTasks, _) = processor.apply(plan, List(accountsTask, transactionsTask), List())

    val accountsTaskAfter = updatedTasks.find(_.name == "accounts_task").get
    val accountIdField = accountsTaskAfter.steps.head.fields.find(_.name == "account_id").get

    // Verify uniqueness was added
    assert(accountIdField.options.get(IS_UNIQUE).exists(_.toString == "true"),
      "account_id should be marked unique")

    // Verify existing options were preserved
    assert(accountIdField.options.get("regex").contains("[A-Z][0-9]{3}"),
      "regex option should be preserved")
    assert(accountIdField.options.get("enableFastRegex").contains("true"),
      "enableFastRegex option should be preserved")
    assert(accountIdField.options.get("customOption").contains("custom_value"),
      "customOption should be preserved")
  }
}
