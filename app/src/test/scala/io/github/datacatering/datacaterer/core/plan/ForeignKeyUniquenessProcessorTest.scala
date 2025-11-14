package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.Constants.IS_UNIQUE
import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.util.SparkSuite

class ForeignKeyUniquenessProcessorTest extends SparkSuite {

  val dataCatererConfig: DataCatererConfiguration = DataCatererConfiguration()

  test("Should mark source foreign key fields as unique when they are not already unique") {
    // Create a simple scenario: accounts -> transactions FK
    val accountsStep = Step(
      name = "accounts",
      `type` = "csv",
      count = Count(Some(30)),
      fields = List(
        Field(name = "account_id", `type` = Some("string")),
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
          Field(name = "amount", `type` = Some("double"))
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

    // Define FK with cardinality
    val foreignKey = ForeignKey(
      source = ForeignKeyRelation("my_accounts", "accounts", List("account_id")),
      generate = List(ForeignKeyRelation("my_transactions", "transactions", List("account_id"))),
      cardinality = Some(CardinalityConfig(ratio = Some(2.0), distribution = "uniform"))
    )

    val plan = Plan(
      name = "test_plan",
      sinkOptions = Some(SinkOptions(
        foreignKeys = List(foreignKey)
      )),
      tasks = List(
        TaskSummary(name = "accounts_task", dataSourceName = "my_accounts", steps = Some(List(accountsStep))),
        TaskSummary(name = "transactions_task", dataSourceName = "my_transactions", steps = Some(List(transactionsStep)))
      )
    )

    val processor = new ForeignKeyUniquenessProcessor(dataCatererConfig)
    val (_, updatedTasks, _) = processor.apply(plan, List(accountsTask, transactionsTask), List())

    // Verify that account_id in accounts is now marked as unique
    val updatedAccountsTask = updatedTasks.find(_.name == "accounts_task").get
    val accountIdField = updatedAccountsTask.steps.head.fields.find(_.name == "account_id").get

    assert(accountIdField.options.contains(IS_UNIQUE))
    assert(accountIdField.options(IS_UNIQUE).toString == "true")

    // Verify that transactions task is NOT modified (it's the target, not source)
    val updatedTransactionsTask = updatedTasks.find(_.name == "transactions_task").get
    val txnAccountIdField = updatedTransactionsTask.steps.head.fields.find(_.name == "account_id").get
    assert(!txnAccountIdField.options.contains(IS_UNIQUE) || txnAccountIdField.options(IS_UNIQUE).toString == "false")
  }

  test("Should not modify fields that are already marked as unique") {
    val accountsStep = Step(
      name = "accounts",
      `type` = "csv",
      count = Count(Some(30)),
      fields = List(
          Field(name = "account_id", `type` = Some("string"), options = Map(IS_UNIQUE -> "true")),
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
          Field(name = "amount", `type` = Some("double"))
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

    val foreignKey = ForeignKey(
      source = ForeignKeyRelation("my_accounts", "accounts", List("account_id")),
      generate = List(ForeignKeyRelation("my_transactions", "transactions", List("account_id"))),
      cardinality = Some(CardinalityConfig(ratio = Some(2.0), distribution = "uniform"))
    )

    val plan = Plan(
      name = "test_plan",
      sinkOptions = Some(SinkOptions(
        foreignKeys = List(foreignKey)
      )),
      tasks = List(
        TaskSummary(name = "accounts_task", dataSourceName = "my_accounts", steps = Some(List(accountsStep))),
        TaskSummary(name = "transactions_task", dataSourceName = "my_transactions", steps = Some(List(transactionsStep)))
      )
    )

    val processor = new ForeignKeyUniquenessProcessor(dataCatererConfig)
    val (_, updatedTasks, _) = processor.apply(plan, List(accountsTask, transactionsTask), List())

    // Verify that account_id is still marked as unique (not overwritten)
    val updatedAccountsTask = updatedTasks.find(_.name == "accounts_task").get
    val accountIdField = updatedAccountsTask.steps.head.fields.find(_.name == "account_id").get

    assert(accountIdField.options.contains(IS_UNIQUE))
    assert(accountIdField.options(IS_UNIQUE).toString == "true")
  }

  test("Should handle multiple foreign key fields in the same source") {
    val accountsStep = Step(
      name = "accounts",
      `type` = "csv",
      count = Count(Some(30)),
      fields = List(
          Field(name = "account_id", `type` = Some("string")),
          Field(name = "branch_id", `type` = Some("string")),
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

    val accountsTask = Task(
      name = "accounts_task",
      steps = List(accountsStep)
    )

    val transactionsTask = Task(
      name = "transactions_task",
      steps = List(transactionsStep)
    )

    val foreignKey = ForeignKey(
      source = ForeignKeyRelation("my_accounts", "accounts", List("account_id", "branch_id")),
      generate = List(ForeignKeyRelation("my_transactions", "transactions", List("account_id", "branch_id"))),
      cardinality = Some(CardinalityConfig(ratio = Some(2.0), distribution = "uniform"))
    )

    val plan = Plan(
      name = "test_plan",
      sinkOptions = Some(SinkOptions(
        foreignKeys = List(foreignKey)
      )),
      tasks = List(
        TaskSummary(name = "accounts_task", dataSourceName = "my_accounts", steps = Some(List(accountsStep))),
        TaskSummary(name = "transactions_task", dataSourceName = "my_transactions", steps = Some(List(transactionsStep)))
      )
    )

    val processor = new ForeignKeyUniquenessProcessor(dataCatererConfig)
    val (_, updatedTasks, _) = processor.apply(plan, List(accountsTask, transactionsTask), List())

    // Verify that both FK fields in accounts are marked as unique
    val updatedAccountsTask = updatedTasks.find(_.name == "accounts_task").get
    val accountIdField = updatedAccountsTask.steps.head.fields.find(_.name == "account_id").get
    val branchIdField = updatedAccountsTask.steps.head.fields.find(_.name == "branch_id").get

    assert(accountIdField.options.contains(IS_UNIQUE))
    assert(accountIdField.options(IS_UNIQUE).toString == "true")
    assert(branchIdField.options.contains(IS_UNIQUE))
    assert(branchIdField.options(IS_UNIQUE).toString == "true")
  }

  test("Should handle foreign keys with no cardinality defined (no-op)") {
    val accountsStep = Step(
      name = "accounts",
      `type` = "csv",
      count = Count(Some(30)),
      fields = List(
          Field(name = "account_id", `type` = Some("string")),
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
          Field(name = "amount", `type` = Some("double"))
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

    // FK without cardinality
    val foreignKey = ForeignKey(
      source = ForeignKeyRelation("my_accounts", "accounts", List("account_id")),
      generate = List(ForeignKeyRelation("my_transactions", "transactions", List("account_id"))),
      cardinality = None
    )

    val plan = Plan(
      name = "test_plan",
      sinkOptions = Some(SinkOptions(
        foreignKeys = List(foreignKey)
      )),
      tasks = List(
        TaskSummary(name = "accounts_task", dataSourceName = "my_accounts", steps = Some(List(accountsStep))),
        TaskSummary(name = "transactions_task", dataSourceName = "my_transactions", steps = Some(List(transactionsStep)))
      )
    )

    val processor = new ForeignKeyUniquenessProcessor(dataCatererConfig)
    val (_, updatedTasks, _) = processor.apply(plan, List(accountsTask, transactionsTask), List())

    // Should still mark as unique even without cardinality (1:1 relationship assumed)
    val updatedAccountsTask = updatedTasks.find(_.name == "accounts_task").get
    val accountIdField = updatedAccountsTask.steps.head.fields.find(_.name == "account_id").get

    assert(accountIdField.options.contains(IS_UNIQUE))
    assert(accountIdField.options(IS_UNIQUE).toString == "true")
  }

  test("Should handle limited value space scenario") {
    // This is the critical test case: when the field has limited possible values
    // (e.g., regex that only generates 10 possibilities), but we need more records
    val accountsStep = Step(
      name = "accounts",
      `type` = "csv",
      count = Count(Some(50)), // Requesting 50 records
      fields = List(
          // This regex only generates values like: A, B, C, D, E (5 possibilities)
          // Without unique=true, we might get duplicates, causing FK issues
          Field(name = "account_id", `type` = Some("string"), options = Map("regex" -> "[A-E]")),
          Field(name = "name", `type` = Some("string"))
        )
    )

    val transactionsStep = Step(
      name = "transactions",
      `type` = "csv",
      count = Count(Some(100)),
      fields = List(
          Field(name = "txn_id", `type` = Some("string")),
          Field(name = "account_id", `type` = Some("string")),
          Field(name = "amount", `type` = Some("double"))
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

    val foreignKey = ForeignKey(
      source = ForeignKeyRelation("my_accounts", "accounts", List("account_id")),
      generate = List(ForeignKeyRelation("my_transactions", "transactions", List("account_id"))),
      cardinality = Some(CardinalityConfig(ratio = Some(2.0), distribution = "uniform"))
    )

    val plan = Plan(
      name = "test_plan",
      sinkOptions = Some(SinkOptions(
        foreignKeys = List(foreignKey)
      )),
      tasks = List(
        TaskSummary(name = "accounts_task", dataSourceName = "my_accounts", steps = Some(List(accountsStep))),
        TaskSummary(name = "transactions_task", dataSourceName = "my_transactions", steps = Some(List(transactionsStep)))
      )
    )

    val processor = new ForeignKeyUniquenessProcessor(dataCatererConfig)
    val (_, updatedTasks, _) = processor.apply(plan, List(accountsTask, transactionsTask), List())

    // Verify that account_id is marked as unique to ensure proper FK behavior
    val updatedAccountsTask = updatedTasks.find(_.name == "accounts_task").get
    val accountIdField = updatedAccountsTask.steps.head.fields.find(_.name == "account_id").get

    assert(accountIdField.options.contains(IS_UNIQUE))
    assert(accountIdField.options(IS_UNIQUE).toString == "true")

    // The regex option should still be present
    assert(accountIdField.options.contains("regex"))
    assert(accountIdField.options("regex") == "[A-E]")
  }

  test("Should do nothing when no foreign keys are defined") {
    val accountsStep = Step(
      name = "accounts",
      `type` = "csv",
      count = Count(Some(30)),
      fields = List(
          Field(name = "account_id", `type` = Some("string")),
          Field(name = "name", `type` = Some("string"))
        )
    )

    val accountsTask = Task(
      name = "accounts_task",
      steps = List(accountsStep)
    )

    val plan = Plan(
      name = "test_plan",
      sinkOptions = None,
      tasks = List(
        TaskSummary(name = "accounts_task", dataSourceName = "my_accounts", steps = Some(List(accountsStep)))
      )
    )

    val processor = new ForeignKeyUniquenessProcessor(dataCatererConfig)
    val (updatedPlan, updatedTasks, _) = processor.apply(plan, List(accountsTask), List())

    // Verify nothing changed
    assert(updatedTasks == List(accountsTask))
    assert(updatedPlan == plan)
  }
}
