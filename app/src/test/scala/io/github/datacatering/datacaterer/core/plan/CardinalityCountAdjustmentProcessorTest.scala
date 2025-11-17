package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.util.SparkSuite

/**
 * Unit tests for CardinalityCountAdjustmentProcessor.
 *
 * This processor is responsible for:
 * 1. Calculating required child record counts based on cardinality config
 * 2. Adjusting task record counts BEFORE data generation
 * 3. Setting up perField configuration to create grouped data
 *
 * Key Rules:
 * - ALWAYS adjust to required count (even if original > required)
 * - For ratio cardinality: set records = source × ratio, perField count = ratio
 * - For bounded cardinality: set records = source count, perField with min/max options
 * - perField should be set on FK fields to create grouping during generation
 */
class CardinalityCountAdjustmentProcessorTest extends SparkSuite {

  val dataCatererConfiguration = DataCatererConfiguration()

  // ========================================================================================
  // RATIO-BASED CARDINALITY TESTS
  // ========================================================================================

  test("Ratio cardinality: Adjusts count DOWN when original > required") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("accounts", "accounts_table", List("account_id")),
      List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"),
          cardinality = Some(CardinalityConfig(ratio = Some(5.0), distribution = "uniform"))))
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("account_task", "accounts", enabled = true, steps = Some(List(
        Step(name = "accounts_table", count = Count(records = Some(3)))
      ))),
      TaskSummary("transaction_task", "transactions", enabled = true, steps = Some(List(
        Step(name = "transactions_table", count = Count(records = Some(30))) // Original too high
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("account_task", List(Step(name = "accounts_table", count = Count(records = Some(3))))),
      Task("transaction_task", List(Step(name = "transactions_table", count = Count(records = Some(30)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
    val (_, adjustedTasks, _) = processor.apply(plan, tasks, List())

    // Verify count was adjusted DOWN from 30 to 3
    val transactionStep = adjustedTasks
      .find(_.name == "transaction_task")
      .flatMap(_.steps.headOption)
      .get

    assert(transactionStep.count.records.contains(3),
      s"Transaction count should be adjusted to 3 (3 accounts x 5 ratio), got ${transactionStep.count.records}")

    // Verify perField was set
    assert(transactionStep.count.perField.isDefined,
      "PerField should be configured")
    assert(transactionStep.count.perField.get.fieldNames.contains("account_id"),
      "PerField should include account_id")
    assert(transactionStep.count.perField.get.count.contains(5L),
      s"PerField count should be 5 (ratio), got ${transactionStep.count.perField.get.count}")
  }

  test("Ratio cardinality: Adjusts count UP when original < required") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("accounts", "accounts_table", List("account_id")),
      List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(5.0), distribution = "uniform"))))
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("account_task", "accounts", enabled = true, steps = Some(List(
        Step(name = "accounts_table", count = Count(records = Some(3)))
      ))),
      TaskSummary("transaction_task", "transactions", enabled = true, steps = Some(List(
        Step(name = "transactions_table", count = Count(records = Some(2))) // Original too low
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("account_task", List(Step(name = "accounts_table", count = Count(records = Some(3))))),
      Task("transaction_task", List(Step(name = "transactions_table", count = Count(records = Some(2)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
    val (_, adjustedTasks, _) = processor.apply(plan, tasks, List())

    val transactionStep = adjustedTasks
      .find(_.name == "transaction_task")
      .flatMap(_.steps.headOption)
      .get

    assert(transactionStep.count.records.contains(3),
      s"Transaction count should be adjusted UP to 3, got ${transactionStep.count.records}")
    assert(transactionStep.count.perField.isDefined, "PerField should be configured")
    assert(transactionStep.count.perField.get.count.contains(5L),
      s"PerField count should be 5, got ${transactionStep.count.perField.get.count}")
  }

  test("Ratio cardinality: Sets perField count equal to ratio") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("customers", "customers_table", List("customer_id")),
      List(ForeignKeyRelation("orders", "orders_table", List("customer_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(3.0), distribution = "uniform")))),
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("customer_task", "customers", enabled = true, steps = Some(List(
        Step(name = "customers_table", count = Count(records = Some(10)))
      ))),
      TaskSummary("order_task", "orders", enabled = true, steps = Some(List(
        Step(name = "orders_table", count = Count(records = Some(100)))
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("customer_task", List(Step(name = "customers_table", count = Count(records = Some(10))))),
      Task("order_task", List(Step(name = "orders_table", count = Count(records = Some(100)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
    val (_, adjustedTasks, _) = processor.apply(plan, tasks, List())

    val orderStep = adjustedTasks.find(_.name == "order_task").flatMap(_.steps.headOption).get

    // 10 customers × 3 ratio = 30 orders
    assert(orderStep.count.records.contains(10), s"Expected 10 records, got ${orderStep.count.records}")
    assert(orderStep.count.perField.isDefined, "PerField should be set")
    assert(orderStep.count.perField.get.fieldNames.contains("customer_id"), "PerField should include customer_id")
    assert(orderStep.count.perField.get.count.contains(3L), s"PerField count should be 3, got ${orderStep.count.perField.get.count}")
  }

  test("Ratio cardinality: Non-uniform distribution uses options instead of count") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("products", "products_table", List("product_id")),
      List(ForeignKeyRelation(
        "reviews", "reviews_table", List("product_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(5.0), distribution = "normal"))
      ))
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("product_task", "products", enabled = true, steps = Some(List(
        Step(name = "products_table", count = Count(records = Some(4)))
      ))),
      TaskSummary("review_task", "reviews", enabled = true, steps = Some(List(
        Step(name = "reviews_table", count = Count(records = Some(50)))
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("product_task", List(Step(name = "products_table", count = Count(records = Some(4))))),
      Task("review_task", List(Step(name = "reviews_table", count = Count(records = Some(50)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
    val (_, adjustedTasks, _) = processor.apply(plan, tasks, List())

    val reviewStep = adjustedTasks.find(_.name == "review_task").flatMap(_.steps.headOption).get

    // 4 products × 5 ratio = 20 reviews
    assert(reviewStep.count.records.contains(4), s"Expected 4 records, got ${reviewStep.count.records}")
    assert(reviewStep.count.perField.isDefined, "PerField should be set")

    val perField = reviewStep.count.perField.get
    assert(perField.count.isEmpty, "PerField count should be empty for non-uniform distribution")
    assert(perField.options.nonEmpty, "PerField options should be set")
    assert(perField.options("min") == 5, "PerField min should be 5")
    assert(perField.options("max") == 5, "PerField max should be 5")
    assert(perField.options("distribution") == "normal", "Distribution should be normal")
  }

  // ========================================================================================
  // BOUNDED CARDINALITY TESTS
  // ========================================================================================

  test("Bounded cardinality: Sets records to SOURCE count, not target count") {
    // This is a key insight: For bounded with perField, we set records = source count
    // because perField will multiply by min/max range during generation

    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("authors", "authors_table", List("author_id")),
      List(ForeignKeyRelation(
        "articles", "articles_table", List("author_id"),
        cardinality = Some(CardinalityConfig(min = Some(2), max = Some(4), distribution = "uniform"))
      ))
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("author_task", "authors", enabled = true, steps = Some(List(
        Step(name = "authors_table", count = Count(records = Some(5)))
      ))),
      TaskSummary("article_task", "articles", enabled = true, steps = Some(List(
        Step(name = "articles_table", count = Count(records = Some(100)))
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("author_task", List(Step(name = "authors_table", count = Count(records = Some(5))))),
      Task("article_task", List(Step(name = "articles_table", count = Count(records = Some(100)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
    val (_, adjustedTasks, _) = processor.apply(plan, tasks, List())

    val articleStep = adjustedTasks.find(_.name == "article_task").flatMap(_.steps.headOption).get

    // For bounded, records should be SOURCE count (5), not max * source (20)
    assert(articleStep.count.records.contains(5),
      s"Records should be set to SOURCE count (5), got ${articleStep.count.records}")

    assert(articleStep.count.perField.isDefined, "PerField should be set")
    val perField = articleStep.count.perField.get
    assert(perField.count.isEmpty, "PerField count should be empty for bounded")
    assert(perField.options("min") == 2, "Min should be 2")
    assert(perField.options("max") == 4, "Max should be 4")
  }

  // ========================================================================================
  // EXISTING PERFIELD TESTS
  // ========================================================================================

  test("Existing perField: Rejects when both cardinality and perField on FK fields") {
    // When target has BOTH cardinality AND perField on FK fields, validation should reject it
    // to prevent conflicting configuration

    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("accounts", "accounts_table", List("account_id")),
      List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(5.0), distribution = "uniform"))))
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val existingPerField = PerFieldCount(
      fieldNames = List("account_id"),
      count = Some(5L)
    )

    val taskSummaries = List(
      TaskSummary("account_task", "accounts", enabled = true, steps = Some(List(
        Step(name = "accounts_table", count = Count(records = Some(10)))
      ))),
      TaskSummary("transaction_task", "transactions", enabled = true, steps = Some(List(
        Step(name = "transactions_table", count = Count(records = Some(100), perField = Some(existingPerField)))
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("account_task", List(Step(name = "accounts_table", count = Count(records = Some(10))))),
      Task("transaction_task", List(Step(name = "transactions_table", count = Count(records = Some(100), perField = Some(existingPerField)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)

    // This should throw because target has BOTH cardinality AND perField on FK fields
    val exception = intercept[IllegalArgumentException] {
      processor.apply(plan, tasks, List())
    }

    assert(exception.getMessage.contains("BOTH cardinality config AND step perField on FK fields"))
  }

  // ========================================================================================
  // NO CARDINALITY TESTS
  // ========================================================================================

  test("No cardinality config: Does not adjust counts") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("users", "users_table", List("user_id")),
      List(ForeignKeyRelation("sessions", "sessions_table", List("user_id"))),
      List()
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("user_task", "users", enabled = true, steps = Some(List(
        Step(name = "users_table", count = Count(records = Some(10)))
      ))),
      TaskSummary("session_task", "sessions", enabled = true, steps = Some(List(
        Step(name = "sessions_table", count = Count(records = Some(50)))
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("user_task", List(Step(name = "users_table", count = Count(records = Some(10))))),
      Task("session_task", List(Step(name = "sessions_table", count = Count(records = Some(50)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
    val (_, adjustedTasks, _) = processor.apply(plan, tasks, List())

    val sessionStep = adjustedTasks.find(_.name == "session_task").flatMap(_.steps.headOption).get

    // Should NOT adjust count when no cardinality config
    assert(sessionStep.count.records.contains(50),
      s"Count should remain 50, got ${sessionStep.count.records}")
    assert(sessionStep.count.perField.isEmpty, "PerField should NOT be set")
  }

  // ========================================================================================
  // COMPOSITE KEY TESTS
  // ========================================================================================

  test("Composite key cardinality: Sets perField with all FK fields") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("locations", "locations_table", List("country", "state")),
      List(ForeignKeyRelation(
        "stores", "stores_table", List("country", "state"),
        cardinality = Some(CardinalityConfig(ratio = Some(3.0), distribution = "uniform"))
      ))
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("location_task", "locations", enabled = true, steps = Some(List(
        Step(name = "locations_table", count = Count(records = Some(5)))
      ))),
      TaskSummary("store_task", "stores", enabled = true, steps = Some(List(
        Step(name = "stores_table", count = Count(records = Some(100)))
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("location_task", List(Step(name = "locations_table", count = Count(records = Some(5))))),
      Task("store_task", List(Step(name = "stores_table", count = Count(records = Some(100)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
    val (_, adjustedTasks, _) = processor.apply(plan, tasks, List())

    val storeStep = adjustedTasks.find(_.name == "store_task").flatMap(_.steps.headOption).get

    // 5 locations × 3 ratio = 15 stores
    assert(storeStep.count.records.contains(5), s"Expected 5 records, got ${storeStep.count.records}")
    assert(storeStep.count.perField.isDefined, "PerField should be set")

    val perField = storeStep.count.perField.get
    assert(perField.fieldNames.contains("country"), "PerField should include country")
    assert(perField.fieldNames.contains("state"), "PerField should include state")
    assert(perField.count.contains(3L), s"PerField count should be 3, got ${perField.count}")
  }

  // ========================================================================================
  // MULTIPLE FK RELATIONSHIPS TESTS
  // ========================================================================================

  test("Multiple FK relationships: Uses source required count") {
    val foreignKeys = List(
      ForeignKey(
        ForeignKeyRelation("accounts", "accounts_table", List("account_id")),
        List(ForeignKeyRelation(
          "transactions", "transactions_table", List("account_id"),
          cardinality = Some(CardinalityConfig(ratio = Some(5.0), distribution = "uniform"))
        ))
      ),
      ForeignKey(
        ForeignKeyRelation("merchants", "merchants_table", List("merchant_id")),
        List(ForeignKeyRelation(
          "transactions", "transactions_table", List("merchant_id"),
          cardinality = Some(CardinalityConfig(ratio = Some(10.0), distribution = "uniform"))
        ))
      )
    )

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("account_task", "accounts", enabled = true, steps = Some(List(
        Step(name = "accounts_table", count = Count(records = Some(3)))
      ))),
      TaskSummary("merchant_task", "merchants", enabled = true, steps = Some(List(
        Step(name = "merchants_table", count = Count(records = Some(2)))
      ))),
      TaskSummary("transaction_task", "transactions", enabled = true, steps = Some(List(
        Step(name = "transactions_table", count = Count(records = Some(100)))
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("account_task", List(Step(name = "accounts_table", count = Count(records = Some(3))))),
      Task("merchant_task", List(Step(name = "merchants_table", count = Count(records = Some(2))))),
      Task("transaction_task", List(Step(name = "transactions_table", count = Count(records = Some(100)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
    val (_, adjustedTasks, _) = processor.apply(plan, tasks, List())

    val transactionStep = adjustedTasks.find(_.name == "transaction_task").flatMap(_.steps.headOption).get

    // Should use source count
    assert(transactionStep.count.records.contains(3),
      s"Should use source count (3), got ${transactionStep.count.records}")
  }

  // ========================================================================================
  // VALIDATION TESTS FOR CONFLICTING CONFIGURATIONS
  // ========================================================================================

  test("Validation: Rejects target with both cardinality AND step perField on FK fields") {
    // Define a target step with perField on the FK field
    val transactionStep = Step(
      name = "transactions_table",
      count = Count(
        records = Some(3),
        perField = Some(PerFieldCount(
          fieldNames = List("account_id"),
          count = Some(5)
        ))
      )
    )

    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("accounts", "accounts_table", List("account_id")),
      List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(10.0)))  // Conflict!
      ))
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("account_task", "accounts", enabled = true, steps = Some(List(
        Step(name = "accounts_table", count = Count(records = Some(3)))
      ))),
      TaskSummary("transaction_task", "transactions", enabled = true, steps = Some(List(
        transactionStep
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("account_task", List(Step(name = "accounts_table", count = Count(records = Some(3))))),
      Task("transaction_task", List(transactionStep))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)

    // Should throw IllegalArgumentException with descriptive message
    val exception = intercept[IllegalArgumentException] {
      processor.apply(plan, tasks, List())
    }

    assert(exception.getMessage.contains("BOTH cardinality config AND step perField on FK fields"),
      s"Error message should mention conflict, got: ${exception.getMessage}")
    assert(exception.getMessage.contains("transactions"),
      s"Error message should mention target data source, got: ${exception.getMessage}")
  }

  test("Validation: Rejects cardinality with both ratio AND min/max") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("accounts", "accounts_table", List("account_id")),
      List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"),
        cardinality = Some(CardinalityConfig(
          ratio = Some(5.0),    // Conflict!
          min = Some(2),        // Conflict!
          max = Some(10)        // Conflict!
        ))
      ))
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("account_task", "accounts", enabled = true, steps = Some(List(
        Step(name = "accounts_table", count = Count(records = Some(3)))
      ))),
      TaskSummary("transaction_task", "transactions", enabled = true, steps = Some(List(
        Step(name = "transactions_table", count = Count(records = Some(30)))
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("account_task", List(Step(name = "accounts_table", count = Count(records = Some(3))))),
      Task("transaction_task", List(Step(name = "transactions_table", count = Count(records = Some(30)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)

    // Should throw IllegalArgumentException
    val exception = intercept[IllegalArgumentException] {
      processor.apply(plan, tasks, List())
    }

    assert(exception.getMessage.contains("BOTH ratio AND min/max"),
      s"Error message should mention conflicting config, got: ${exception.getMessage}")
  }

  test("Validation: Accepts target cardinality without step perField (no conflict)") {
    // This should NOT throw - target has cardinality but step has NO perField on FK fields
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("accounts", "accounts_table", List("account_id")),
      List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(5.0)))
      ))
    ))

    val sinkOptions = SinkOptions(Some("seed"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("account_task", "accounts", enabled = true, steps = Some(List(
        Step(name = "accounts_table", count = Count(records = Some(3)))
      ))),
      TaskSummary("transaction_task", "transactions", enabled = true, steps = Some(List(
        Step(name = "transactions_table", count = Count(records = Some(30)))
      )))
    )

    val plan = Plan("test", "test", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("account_task", List(Step(name = "accounts_table", count = Count(records = Some(3))))),
      Task("transaction_task", List(Step(name = "transactions_table", count = Count(records = Some(30)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)

    // Should NOT throw
    val (_, adjustedTasks, _) = processor.apply(plan, tasks, List())

    // Verify it actually processed correctly
    val transactionStep = adjustedTasks.find(_.name == "transaction_task").flatMap(_.steps.headOption).get
    assert(transactionStep.count.records.contains(3), s"Should be 3 (3 × 5), got ${transactionStep.count.records}")
  }
}
