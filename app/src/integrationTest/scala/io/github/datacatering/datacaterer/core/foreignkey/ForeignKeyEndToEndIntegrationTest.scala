package io.github.datacatering.datacaterer.core.foreignkey

import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.plan.CardinalityCountAdjustmentProcessor
import io.github.datacatering.datacaterer.core.util.{ForeignKeyUtil, SparkSuite}

/**
 * End-to-end integration tests for the complete foreign key workflow:
 * 1. CardinalityCountAdjustmentProcessor adjusts task counts and sets perField
 * 2. Data generation creates records (simulated here)
 * 3. ForeignKeyUtil applies foreign key values using the new architecture
 *
 * These tests verify the full pipeline works correctly and that cardinality
 * is properly handled throughout the entire flow.
 */
class ForeignKeyEndToEndIntegrationTest extends SparkSuite {

  // ========================================================================================
  // RATIO-BASED CARDINALITY END-TO-END TESTS
  // ========================================================================================

  test("E2E: Ratio-based cardinality with uniform distribution - full flow") {
    // Step 1: Define plan with cardinality at target level
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("accounts", "accounts_table", List("account_id")),
      List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(5.0))))),
      List()
    ))

    val sinkOptions = SinkOptions(Some("12345"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("account_task", "accounts", enabled = true),
      TaskSummary("transaction_task", "transactions", enabled = true)
    )

    val plan = Plan("cardinality e2e test", "test plan", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("account_task", List(
        Step(name = "accounts_table", count = Count(records = Some(3)))
      )),
      Task("transaction_task", List(
        Step(name = "transactions_table", count = Count(records = Some(30)))
      ))
    )

    val validations = List[ValidationConfiguration]()
    val dataCatererConfiguration = DataCatererConfiguration()

    // Step 2: Run CardinalityCountAdjustmentProcessor
    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
    val (adjustedPlan, adjustedTasks, _) = processor.apply(plan, tasks, validations)

    // Verify count was adjusted: 3 accounts * 5 ratio = 15 transactions
    // With perField, records is set to SOURCE count (3), and perField count is set to ratio (5)
    // This generates 3 * 5 = 15 total records
    val adjustedTransactionStep = adjustedTasks
      .find(_.name == "transaction_task")
      .flatMap(_.steps.headOption)
      .get

    assert(adjustedTransactionStep.count.records.contains(3),
      s"Transaction count should be set to source count (3), got ${adjustedTransactionStep.count.records}")

    // Verify perField was set
    assert(adjustedTransactionStep.count.perField.isDefined,
      "PerField configuration should be set")
    assert(adjustedTransactionStep.count.perField.get.fieldNames.contains("account_id"),
      "PerField should be configured for account_id")
    assert(adjustedTransactionStep.count.perField.get.count.contains(5L),
      "PerField count should be 5 (ratio)")

    // Step 3: Simulate data generation with perField grouping
    val accountsDf = sparkSession.createDataFrame(Seq(
      ("ACC001", "Alice"),
      ("ACC002", "Bob"),
      ("ACC003", "Charlie")
    )).toDF("account_id", "account_name")

    // Simulate what data generation would create: 15 transactions with grouping
    // 5 transactions per account_id group
    val transactionsDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "GROUP1", 100.0),
      ("TXN002", "GROUP1", 200.0),
      ("TXN003", "GROUP1", 150.0),
      ("TXN004", "GROUP1", 300.0),
      ("TXN005", "GROUP1", 250.0),
      ("TXN006", "GROUP2", 175.0),
      ("TXN007", "GROUP2", 225.0),
      ("TXN008", "GROUP2", 125.0),
      ("TXN009", "GROUP2", 275.0),
      ("TXN010", "GROUP2", 325.0),
      ("TXN011", "GROUP3", 135.0),
      ("TXN012", "GROUP3", 185.0),
      ("TXN013", "GROUP3", 235.0),
      ("TXN014", "GROUP3", 285.0),
      ("TXN015", "GROUP3", 335.0)
    )).toDF("txn_id", "account_id", "amount")

    val dfMap = List(
      "accounts.accounts_table" -> accountsDf,
      "transactions.transactions_table" -> transactionsDf
    )

    // Step 4: Apply foreign keys using ForeignKeyUtil
    val executableTasks = adjustedTasks.map { task =>
      val taskSummary = adjustedPlan.tasks.find(_.name == task.name).get
      (taskSummary, task)
    }

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(
      adjustedPlan,
      dfMap,
      executableTasks = Some(executableTasks)
    )

    val updatedTransactionsDf = result.find(_._1.equalsIgnoreCase("transactions.transactions_table")).get._2

    // Verify results
    assert(updatedTransactionsDf.count() == 15, "Should have 15 transactions")

    // Verify each account has exactly 5 transactions (uniform distribution)
    val accountCounts = updatedTransactionsDf.groupBy("account_id").count().collect()
    assert(accountCounts.length == 3, "Should have 3 accounts")
    accountCounts.foreach { row =>
      val count = row.getLong(1)
      assert(count == 5, s"Account ${row.getString(0)} should have exactly 5 transactions, got $count")
    }

    // Verify all txn_ids are unique (no duplication)
    val txnIds = updatedTransactionsDf.select("txn_id").collect().map(_.getString(0))
    assert(txnIds.length == txnIds.distinct.length, "All transaction IDs should be unique")

    // Verify all amounts are preserved (no duplication)
    val originalAmounts = transactionsDf.select("amount").collect().map(_.getDouble(0)).toSet
    val resultAmounts = updatedTransactionsDf.select("amount").collect().map(_.getDouble(0)).toSet
    assert(resultAmounts == originalAmounts, "All amounts should be preserved")
  }

  test("E2E: Bounded cardinality (min/max) - full flow") {
    // Step 1: Define plan with bounded cardinality at target level
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("authors", "authors_table", List("author_id")),
      List(ForeignKeyRelation("articles", "articles_table", List("author_id"),
        cardinality = Some(CardinalityConfig(min = Some(2), max = Some(4))))),
      List()
    ))

    val sinkOptions = SinkOptions(Some("12346"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("author_task", "authors", enabled = true),
      TaskSummary("article_task", "articles", enabled = true)
    )

    val plan = Plan("bounded cardinality e2e test", "test plan", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("author_task", List(Step(name = "authors_table", count = Count(records = Some(3))))),
      Task("article_task", List(Step(name = "articles_table", count = Count(records = Some(10)))))
    )

    val validations = List[ValidationConfiguration]()
    val dataCatererConfiguration = DataCatererConfiguration()

    // Step 2: Run CardinalityCountAdjustmentProcessor
    val processor = new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
    val (adjustedPlan, adjustedTasks, _) = processor.apply(plan, tasks, validations)

    // Verify count was adjusted: 3 authors * 4 max = 12 articles
    val adjustedArticleStep = adjustedTasks
      .find(_.name == "article_task")
      .flatMap(_.steps.headOption)
      .get

    // For bounded with perField, records should be set to source count (3), not max * source (12)
    assert(adjustedArticleStep.count.records.contains(3),
      s"Article count should be set to source count (3), got ${adjustedArticleStep.count.records}")

    // Verify perField was set with min/max options
    assert(adjustedArticleStep.count.perField.isDefined, "PerField should be set")
    val perField = adjustedArticleStep.count.perField.get
    assert(perField.fieldNames.contains("author_id"), "PerField should include author_id")
    assert(perField.options.get("min").contains(2), "PerField min should be 2")
    assert(perField.options.get("max").contains(4), "PerField max should be 4")

    // Step 3: Simulate data generation with perField grouping (varying counts 2-4)
    val authorsDf = sparkSession.createDataFrame(Seq(
      ("AUTH001", "John Doe"),
      ("AUTH002", "Jane Smith"),
      ("AUTH003", "Bob Wilson")
    )).toDF("author_id", "author_name")

    // Simulate varying counts per author: 3, 2, 4 articles respectively
    val articlesDf = sparkSession.createDataFrame(Seq(
      ("ART001", "GROUP1", "Title 1"),
      ("ART002", "GROUP1", "Title 2"),
      ("ART003", "GROUP1", "Title 3"),
      ("ART004", "GROUP2", "Title 4"),
      ("ART005", "GROUP2", "Title 5"),
      ("ART006", "GROUP3", "Title 6"),
      ("ART007", "GROUP3", "Title 7"),
      ("ART008", "GROUP3", "Title 8"),
      ("ART009", "GROUP3", "Title 9")
    )).toDF("article_id", "author_id", "title")

    val dfMap = List(
      "authors.authors_table" -> authorsDf,
      "articles.articles_table" -> articlesDf
    )

    // Step 4: Apply foreign keys
    val executableTasks = adjustedTasks.map { task =>
      val taskSummary = adjustedPlan.tasks.find(_.name == task.name).get
      (taskSummary, task)
    }

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(
      adjustedPlan,
      dfMap,
      executableTasks = Some(executableTasks)
    )

    val updatedArticlesDf = result.find(_._1.equalsIgnoreCase("articles.articles_table")).get._2

    // Verify results
    assert(updatedArticlesDf.count() == 9, "Should have 9 articles")

    // Verify each author has 2-4 articles
    val authorCounts = updatedArticlesDf.groupBy("author_id").count().collect()
    assert(authorCounts.length == 3, "Should have 3 authors")
    authorCounts.foreach { row =>
      val count = row.getLong(1)
      assert(count >= 2 && count <= 4,
        s"Author ${row.getString(0)} should have 2-4 articles, got $count")
    }

    // Verify no duplication
    val articleIds = updatedArticlesDf.select("article_id").collect().map(_.getString(0))
    assert(articleIds.length == articleIds.distinct.length, "All article IDs should be unique")
  }

  // ========================================================================================
  // CARDINALITY WITH GENERATION MODES END-TO-END TESTS
  // ========================================================================================

  test("E2E: Cardinality with all-exist mode - all FKs valid, cardinality preserved") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("customers", "customers_table", List("customer_id")),
      List(ForeignKeyRelation("orders", "orders_table", List("customer_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(2.0))),
        generationMode = Some("all-exist"))),
      List()
    ))

    val sinkOptions = SinkOptions(Some("12347"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("customer_task", "customers", enabled = true),
      TaskSummary("order_task", "orders", enabled = true)
    )

    val plan = Plan("cardinality all-exist e2e test", "test plan", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("customer_task", List(Step(name = "customers_table", count = Count(records = Some(3))))),
      Task("order_task", List(Step(name = "orders_table", count = Count(records = Some(10)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(DataCatererConfiguration())
    val (adjustedPlan, adjustedTasks, _) = processor.apply(plan, tasks, List())

    // Simulate data generation
    val customersDf = sparkSession.createDataFrame(Seq(
      ("CUST001", "Alice"),
      ("CUST002", "Bob"),
      ("CUST003", "Carol")
    )).toDF("customer_id", "customer_name")

    val ordersDf = sparkSession.createDataFrame(Seq(
      ("ORD001", "GROUP1", 100.0),
      ("ORD002", "GROUP1", 150.0),
      ("ORD003", "GROUP2", 200.0),
      ("ORD004", "GROUP2", 250.0),
      ("ORD005", "GROUP3", 300.0),
      ("ORD006", "GROUP3", 350.0)
    )).toDF("order_id", "customer_id", "amount")

    val dfMap = List(
      "customers.customers_table" -> customersDf,
      "orders.orders_table" -> ordersDf
    )

    val executableTasks = adjustedTasks.map { task =>
      val taskSummary = adjustedPlan.tasks.find(_.name == task.name).get
      (taskSummary, task)
    }

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(
      adjustedPlan,
      dfMap,
      executableTasks = Some(executableTasks)
    )

    val updatedOrdersDf = result.find(_._1.equalsIgnoreCase("orders.orders_table")).get._2

    // Verify all orders have valid customer_ids
    val validCustomerIds = customersDf.select("customer_id").collect().map(_.getString(0)).toSet
    val resultRows = updatedOrdersDf.collect()

    resultRows.foreach { row =>
      val customerId = row.getAs[String]("customer_id")
      assert(validCustomerIds.contains(customerId), s"Customer ID $customerId should be valid")
    }

    // Verify cardinality: each customer should have exactly 2 orders (uniform)
    val ordersByCustomer = updatedOrdersDf.groupBy("customer_id").count().collect()
    ordersByCustomer.foreach { row =>
      val count = row.getLong(1)
      assert(count == 2, s"Customer ${row.getString(0)} should have exactly 2 orders, got $count")
    }
  }

  test("E2E: Cardinality with partial mode - introduces violations while preserving cardinality") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("products", "products_table", List("product_id")),
      List(ForeignKeyRelation("reviews", "reviews_table", List("product_id"),
        cardinality = Some(CardinalityConfig(ratio = Some(3.0))),
        nullability = Some(NullabilityConfig(0.25)),
        generationMode = Some("partial"))),
      List()
    ))

    val sinkOptions = SinkOptions(Some("1"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("product_task", "products", enabled = true),
      TaskSummary("review_task", "reviews", enabled = true)
    )

    val plan = Plan("cardinality partial e2e test", "test plan", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("product_task", List(Step(name = "products_table", count = Count(records = Some(4))))),
      Task("review_task", List(Step(name = "reviews_table", count = Count(records = Some(20)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(DataCatererConfiguration())
    val (adjustedPlan, adjustedTasks, _) = processor.apply(plan, tasks, List())

    // Simulate data generation: 4 products * 3 reviews = 12 reviews
    val productsDf = sparkSession.createDataFrame(Seq(
      ("PROD001", "Product A"),
      ("PROD002", "Product B"),
      ("PROD003", "Product C"),
      ("PROD004", "Product D")
    )).toDF("product_id", "product_name")

    val reviewsDf = sparkSession.createDataFrame(Seq(
      ("REV001", "GROUP1", 5),
      ("REV002", "GROUP1", 4),
      ("REV003", "GROUP1", 5),
      ("REV004", "GROUP2", 3),
      ("REV005", "GROUP2", 4),
      ("REV006", "GROUP2", 5),
      ("REV007", "GROUP3", 2),
      ("REV008", "GROUP3", 4),
      ("REV009", "GROUP3", 5),
      ("REV010", "GROUP4", 5),
      ("REV011", "GROUP4", 4),
      ("REV012", "GROUP4", 3)
    )).toDF("review_id", "product_id", "rating")

    val dfMap = List(
      "products.products_table" -> productsDf,
      "reviews.reviews_table" -> reviewsDf
    )

    val executableTasks = adjustedTasks.map { task =>
      val taskSummary = adjustedPlan.tasks.find(_.name == task.name).get
      (taskSummary, task)
    }

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(
      adjustedPlan,
      dfMap,
      executableTasks = Some(executableTasks)
    )

    val updatedReviewsDf = result.find(_._1.equalsIgnoreCase("reviews.reviews_table")).get._2

    // Count null FKs (violations)
    val nullCount = updatedReviewsDf.filter(updatedReviewsDf("product_id").isNull).count()

    // With seed=1 and 25% ratio, expect exactly 4 nulls out of 12 records
    assert(nullCount == 4, s"Expected exactly 4 nulls with seed=1, got $nullCount")

    // Verify non-null FKs are valid
    val validProductIds = productsDf.select("product_id").collect().map(_.getString(0)).toSet
    val nonNullReviews = updatedReviewsDf.filter(updatedReviewsDf("product_id").isNotNull)

    nonNullReviews.collect().foreach { row =>
      val productId = row.getAs[String]("product_id")
      assert(validProductIds.contains(productId), s"Product ID $productId should be valid")
    }

    // Verify cardinality structure is preserved for non-null values
    val reviewsByProduct = nonNullReviews.groupBy("product_id").count().collect()
    reviewsByProduct.foreach { row =>
      val count = row.getLong(1)
      // With 25% nulls, expect 2-3 reviews per product on average
      assert(count >= 1 && count <= 3,
        s"Product ${row.getString(0)} should have 1-3 reviews (accounting for nulls), got $count")
    }
  }

  // ========================================================================================
  // COMPOSITE KEY CARDINALITY END-TO-END TESTS
  // ========================================================================================

  test("E2E: Composite key cardinality - full flow") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("locations", "locations_table", List("country", "state")),
      List(ForeignKeyRelation("stores", "stores_table", List("country", "state"),
        cardinality = Some(CardinalityConfig(ratio = Some(3.0))))),
      List()
    ))

    val sinkOptions = SinkOptions(Some("12348"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("location_task", "locations", enabled = true),
      TaskSummary("store_task", "stores", enabled = true)
    )

    val plan = Plan("composite key cardinality e2e test", "test plan", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("location_task", List(Step(name = "locations_table", count = Count(records = Some(2))))),
      Task("store_task", List(Step(name = "stores_table", count = Count(records = Some(10)))))
    )

    val processor = new CardinalityCountAdjustmentProcessor(DataCatererConfiguration())
    val (adjustedPlan, adjustedTasks, _) = processor.apply(plan, tasks, List())

    // Simulate data generation
    val locationsDf = sparkSession.createDataFrame(Seq(
      ("USA", "NY", "New York"),
      ("USA", "CA", "California")
    )).toDF("country", "state", "city")

    // 2 locations * 3 stores = 6 stores with composite key grouping
    val storesDf = sparkSession.createDataFrame(Seq(
      ("STORE001", "GROUP1A", "GROUP1B"),
      ("STORE002", "GROUP1A", "GROUP1B"),
      ("STORE003", "GROUP1A", "GROUP1B"),
      ("STORE004", "GROUP2A", "GROUP2B"),
      ("STORE005", "GROUP2A", "GROUP2B"),
      ("STORE006", "GROUP2A", "GROUP2B")
    )).toDF("store_id", "country", "state")

    val dfMap = List(
      "locations.locations_table" -> locationsDf,
      "stores.stores_table" -> storesDf
    )

    val executableTasks = adjustedTasks.map { task =>
      val taskSummary = adjustedPlan.tasks.find(_.name == task.name).get
      (taskSummary, task)
    }

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(
      adjustedPlan,
      dfMap,
      executableTasks = Some(executableTasks)
    )

    val updatedStoresDf = result.find(_._1.equalsIgnoreCase("stores.stores_table")).get._2

    // Verify count
    assert(updatedStoresDf.count() == 6, "Should have 6 stores")

    // Verify each location has exactly 3 stores
    val storeCounts = updatedStoresDf.groupBy("country", "state").count().collect()
    assert(storeCounts.length == 2, "Should have 2 locations")
    storeCounts.foreach { row =>
      val count = row.getLong(2)
      assert(count == 3, s"Location (${row.getString(0)}, ${row.getString(1)}) should have 3 stores, got $count")
    }

    // Verify no duplication
    val storeIds = updatedStoresDf.select("store_id").collect().map(_.getString(0))
    assert(storeIds.length == storeIds.distinct.length, "All store IDs should be unique")
  }

  // ========================================================================================
  // NULLABILITY END-TO-END TESTS
  // ========================================================================================

  test("E2E: FK with nullability (no cardinality) - standard processing") {
    val foreignKeys = List(ForeignKey(
      ForeignKeyRelation("stores", "stores_table", List("store_id")),
      List(ForeignKeyRelation("sales", "sales_table", List("store_id"),
        nullability = Some(NullabilityConfig(0.2)))),
      List()
    ))

    val sinkOptions = SinkOptions(Some("12349"), None, foreignKeys)

    val taskSummaries = List(
      TaskSummary("store_task", "stores", enabled = true),
      TaskSummary("sale_task", "sales", enabled = true)
    )

    val plan = Plan("nullability e2e test", "test plan", taskSummaries, Some(sinkOptions))

    val tasks = List(
      Task("store_task", List(Step(name = "stores_table", count = Count(records = Some(3))))),
      Task("sale_task", List(Step(name = "sales_table", count = Count(records = Some(10)))))
    )

    // No cardinality, so CardinalityCountAdjustmentProcessor shouldn't change anything
    val processor = new CardinalityCountAdjustmentProcessor(DataCatererConfiguration())
    val (adjustedPlan, adjustedTasks, _) = processor.apply(plan, tasks, List())

    // Verify no adjustment happened
    val saleStep = adjustedTasks.find(_.name == "sale_task").flatMap(_.steps.headOption).get
    assert(saleStep.count.records.contains(10), "Sale count should remain 10")
    assert(saleStep.count.perField.isEmpty, "PerField should not be set")

    // Simulate data generation
    val storesDf = sparkSession.createDataFrame(Seq(
      ("STORE001", "Downtown"),
      ("STORE002", "Uptown"),
      ("STORE003", "Suburbs")
    )).toDF("store_id", "location")

    val salesDf = sparkSession.createDataFrame(Seq(
      ("SALE001", "INVALID1", 100.0),
      ("SALE002", "INVALID2", 200.0),
      ("SALE003", "INVALID3", 300.0),
      ("SALE004", "INVALID4", 400.0),
      ("SALE005", "INVALID5", 500.0),
      ("SALE006", "INVALID6", 600.0),
      ("SALE007", "INVALID7", 700.0),
      ("SALE008", "INVALID8", 800.0),
      ("SALE009", "INVALID9", 900.0),
      ("SALE010", "INVALID10", 1000.0)
    )).toDF("sale_id", "store_id", "amount")

    val dfMap = List(
      "stores.stores_table" -> storesDf,
      "sales.sales_table" -> salesDf
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(
      adjustedPlan,
      dfMap,
      executableTasks = None // No perField
    )

    val updatedSalesDf = result.find(_._1.equalsIgnoreCase("sales.sales_table")).get._2

    // Count nulls
    val nullCount = updatedSalesDf.filter(updatedSalesDf("store_id").isNull).count()

    // With seed=12349 and 20% ratio, we expect around 2 nulls out of 10 records
    // Exact count depends on seed randomness
    assert(nullCount >= 0 && nullCount <= 4,
      s"Expected 0-4 nulls with 20% ratio (seed variance), got $nullCount")

    // Non-null values should be valid store IDs
    val validStoreIds = storesDf.select("store_id").collect().map(_.getString(0)).toSet
    val nonNullSales = updatedSalesDf.filter(updatedSalesDf("store_id").isNotNull)

    nonNullSales.collect().foreach { row =>
      val storeId = row.getAs[String]("store_id")
      assert(validStoreIds.contains(storeId), s"Store ID $storeId should be valid")
    }
  }
}
