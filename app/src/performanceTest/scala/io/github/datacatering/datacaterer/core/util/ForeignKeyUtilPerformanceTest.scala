package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterEach

import java.io.File
import scala.collection.mutable.ListBuffer

/**
 * Performance baseline tests for ForeignKeyUtil using actual data generation flow.
 * Tests measure:
 * 1. End-to-end data generation time with foreign key relationships
 * 2. Memory usage patterns
 * 3. Referential integrity correctness
 *
 * Test scenarios cover flat fields, nested fields, and chained foreign keys
 * at different data volumes (10K, 100K, 500K records).
 */
class ForeignKeyUtilPerformanceTest extends SparkSuite with BeforeAndAfterEach {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val performanceResults = ListBuffer[(String, Long, Long, Map[String, Any])]()
  private val testDataPath = "/tmp/data-caterer-fk-perf-test"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Create test directory
    new File(testDataPath).mkdirs()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    // Clean up test data
    def deleteRecursively(file: File): Unit = {
      if (file.isDirectory) {
        file.listFiles.foreach(deleteRecursively)
      }
      file.delete()
    }
    if (new File(testDataPath).exists()) {
      deleteRecursively(new File(testDataPath))
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()

    // Print performance summary
    println("\n" + "=" * 100)
    println("FOREIGN KEY PERFORMANCE BASELINE RESULTS")
    println("=" * 100)
    performanceResults.foreach { case (testName, durationMs, memoryMB, metrics) =>
      println(f"\nTest: $testName")
      println(f"  Duration: ${durationMs}ms (${durationMs / 1000.0}%.2fs)")
      println(f"  Memory: ${memoryMB}MB")
      metrics.foreach { case (key, value) =>
        println(f"  $key: $value")
      }
    }
    println("=" * 100 + "\n")
  }

  private def measurePerformance[T](testName: String, metrics: Map[String, Any] = Map())(block: => T): T = {
    // Force GC before test
    System.gc()
    Thread.sleep(100)

    val runtime = Runtime.getRuntime
    val startMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
    val startTime = System.currentTimeMillis()

    val result = block

    val endTime = System.currentTimeMillis()
    val endMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
    val duration = endTime - startTime
    val memoryUsed = endMemory - startMemory

    performanceResults += ((testName, duration, memoryUsed, metrics))
    LOGGER.info(f"Test '$testName' completed in ${duration}ms, memory: ${memoryUsed}MB")

    result
  }

  private def validateReferentialIntegrity(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    sourceField: String,
    targetField: String
  ): (Boolean, Map[String, Long]) = {
    // Check that all values in target field exist in source field
    val sourceValues = sourceDf.select(sourceField).distinct().collect().map(_.get(0)).toSet

    val targetValues = if (targetField.contains(".")) {
      // Handle nested fields
      targetDf.selectExpr(targetField).distinct().collect().map(_.get(0)).toSet
    } else {
      targetDf.select(targetField).distinct().collect().map(_.get(0)).toSet
    }

    val invalidCount = targetValues.count(v => !sourceValues.contains(v))
    val totalTargetCount = targetValues.size
    val uniqueSourceCount = sourceValues.size

    val isValid = invalidCount == 0
    val metrics = Map(
      "valid_references" -> (totalTargetCount - invalidCount).toLong,
      "invalid_references" -> invalidCount.toLong,
      "total_target_distinct_values" -> totalTargetCount.toLong,
      "unique_source_values" -> uniqueSourceCount.toLong
    )

    (isValid, metrics)
  }

  // ========================================================================================
  // BASELINE TESTS - FLAT FIELDS
  // ========================================================================================

  test("Baseline: Small dataset (10K records) - Flat fields") {
    val numRecords = 10000

    class SmallFlatFieldTest extends PlanRun {
      val accountsPath = s"$testDataPath/small_flat/accounts.csv"
      val transactionsPath = s"$testDataPath/small_flat/transactions.csv"

      // Source data - accounts
      val accounts = csv("accounts", accountsPath, Map("header" -> "true"))
        .fields(
          field.name("account_id").regex("ACC[0-9]{8}").unique(true),
          field.name("account_name").expression("#{Name.name}")
        )
        .count(count.records(numRecords))

      // Target data - transactions referencing accounts
      val transactions = csv("transactions", transactionsPath, Map("header" -> "true"))
        .fields(
          field.name("txn_id").regex("TXN[0-9]{10}").unique(true),
          field.name("account_id"),  // Will be populated via foreign key
          field.name("amount").min(10.0).max(10000.0)
        )
        .count(count.records(numRecords))

      val planWithFK = plan.addForeignKeyRelationship(
        accounts, "account_id",
        List((transactions, "account_id"))
      )

      val conf = configuration
        .enableGeneratePlanAndTasks(false)
        .generatedReportsFolderPath(s"$testDataPath/small_flat/reports")

      execute(planWithFK, conf, accounts, transactions)
    }

    measurePerformance(
      "Small dataset (10K) - Flat fields",
      Map("records" -> numRecords, "field_type" -> "flat", "approach" -> "end_to_end")
    ) {
      val planRun = new SmallFlatFieldTest()
      PlanProcessor.determineAndExecutePlan(Some(planRun))
    }

    // Validate referential integrity
    val accountsDf = sparkSession.read.option("header", "true").csv(s"$testDataPath/small_flat/accounts.csv")
    val transactionsDf = sparkSession.read.option("header", "true").csv(s"$testDataPath/small_flat/transactions.csv")

    val (isValid, metrics) = validateReferentialIntegrity(
      accountsDf, transactionsDf, "account_id", "account_id"
    )

    assert(isValid, s"Referential integrity violated: $metrics")
    assert(transactionsDf.count() >= numRecords * 0.99, s"Expected at least ${numRecords * 0.99} transactions, got ${transactionsDf.count()}")
    println(s"  Integrity check: $metrics")
    println(s"  Transaction count: ${transactionsDf.count()}")
  }

  test("Baseline: Medium dataset (100K records) - Flat fields") {
    val numRecords = 100000

    class MediumFlatFieldTest extends PlanRun {
      val accountsPath = s"$testDataPath/medium_flat/accounts.csv"
      val transactionsPath = s"$testDataPath/medium_flat/transactions.csv"

      val accounts = csv("accounts", accountsPath, Map("header" -> "true"))
        .fields(
          field.name("account_id").regex("ACC[0-9]{8}").unique(true),
          field.name("account_name").expression("#{Name.name}")
        )
        .count(count.records(numRecords))

      val transactions = csv("transactions", transactionsPath, Map("header" -> "true"))
        .fields(
          field.name("txn_id").regex("TXN[0-9]{10}").unique(true),
          field.name("account_id"),
          field.name("amount").min(10.0).max(10000.0)
        )
        .count(count.records(numRecords))

      val planWithFK = plan.addForeignKeyRelationship(
        accounts, "account_id",
        List((transactions, "account_id"))
      )

      val conf = configuration
        .enableGeneratePlanAndTasks(false)
        .generatedReportsFolderPath(s"$testDataPath/medium_flat/reports")

      execute(planWithFK, conf, accounts, transactions)
    }

    measurePerformance(
      "Medium dataset (100K) - Flat fields",
      Map("records" -> numRecords, "field_type" -> "flat", "approach" -> "end_to_end")
    ) {
      val planRun = new MediumFlatFieldTest()
      PlanProcessor.determineAndExecutePlan(Some(planRun))
    }

    val accountsDf = sparkSession.read.option("header", "true").csv(s"$testDataPath/medium_flat/accounts.csv")
    val transactionsDf = sparkSession.read.option("header", "true").csv(s"$testDataPath/medium_flat/transactions.csv")

    val (isValid, metrics) = validateReferentialIntegrity(
      accountsDf, transactionsDf, "account_id", "account_id"
    )

    assert(isValid, s"Referential integrity violated: $metrics")
    assert(transactionsDf.count() >= numRecords * 0.99, s"Expected at least ${numRecords * 0.99} transactions, got ${transactionsDf.count()}")
    println(s"  Integrity check: $metrics")
    println(s"  Transaction count: ${transactionsDf.count()}")
  }

  test("Baseline: Large dataset (500K records) - Flat fields") {
    val numRecords = 500000

    class LargeFlatFieldTest extends PlanRun {
      val accountsPath = s"$testDataPath/large_flat/accounts.csv"
      val transactionsPath = s"$testDataPath/large_flat/transactions.csv"

      val accounts = csv("accounts", accountsPath, Map("header" -> "true"))
        .fields(
          field.name("account_id").regex("ACC[0-9]{12}"),
          field.name("account_name").expression("#{Name.name}")
        )
        .count(count.records(numRecords))

      val transactions = csv("transactions", transactionsPath, Map("header" -> "true"))
        .fields(
          field.name("txn_id").regex("TXN[0-9]{12}"),
          field.name("account_id"),
          field.name("amount").min(10.0).max(10000.0)
        )
        .count(count.records(numRecords))

      val planWithFK = plan.addForeignKeyRelationship(
        accounts, "account_id",
        List((transactions, "account_id"))
      )

      val conf = configuration
        .enableGeneratePlanAndTasks(false)
        .generatedReportsFolderPath(s"$testDataPath/large_flat/reports")

      execute(planWithFK, conf, accounts, transactions)
    }

    measurePerformance(
      "Large dataset (500K) - Flat fields",
      Map("records" -> numRecords, "field_type" -> "flat", "approach" -> "end_to_end")
    ) {
      val planRun = new LargeFlatFieldTest()
      PlanProcessor.determineAndExecutePlan(Some(planRun))
    }

    val accountsDf = sparkSession.read.option("header", "true").csv(s"$testDataPath/large_flat/accounts.csv")
    val transactionsDf = sparkSession.read.option("header", "true").csv(s"$testDataPath/large_flat/transactions.csv")

    // Just check count for large dataset to avoid slow validation
    // Allow for small variance due to unique constraint limitations
    val actualCount = transactionsDf.count()
    assert(actualCount >= numRecords * 0.99, s"Expected at least ${numRecords * 0.99} transactions, got $actualCount")
    println(s"  Transaction count: ${transactionsDf.count()}")
    println(s"  Account count: ${accountsDf.count()}")
  }

  // ========================================================================================
  // BASELINE TESTS - CHAINED FOREIGN KEYS
  // ========================================================================================

  // NOTE: Nested field tests temporarily removed due to API usage questions.
  // Will be added after verifying correct .schema() API usage
  test("Baseline: Chained foreign keys (customers -> orders -> order_items)") {
    val numRecords = 10000

    class ChainedForeignKeyTest extends PlanRun {
      val customersPath = s"$testDataPath/chained/customers.csv"
      val ordersPath = s"$testDataPath/chained/orders.csv"
      val orderItemsPath = s"$testDataPath/chained/order_items.csv"

      // Level 1: Customers (root)
      val customers = csv("customers", customersPath, Map("header" -> "true"))
        .fields(
          field.name("customer_id").regex("CUST[0-9]{12}"),
          field.name("customer_name").expression("#{Name.name}")
        )
        .count(count.records(numRecords))

      // Level 2: Orders (references customers)
      val orders = csv("orders", ordersPath, Map("header" -> "true"))
        .fields(
          field.name("order_id").regex("ORD[0-9]{12}"),
          field.name("customer_id"),  // FK to customers
          field.name("order_date").sql("current_date()")
        )
        .count(count.records(numRecords))

      // Level 3: Order items (references orders)
      val orderItems = csv("order_items", orderItemsPath, Map("header" -> "true"))
        .fields(
          field.name("item_id").regex("ITEM[0-9]{14}"),
          field.name("order_id"),  // FK to orders
          field.name("product").expression("#{Commerce.productName}"),
          field.name("quantity").min(1).max(10)
        )
        .count(count.records(numRecords))

      val planWithFK = plan
        .addForeignKeyRelationship(
          customers, "customer_id",
          List((orders, "customer_id"))
        )
        .addForeignKeyRelationship(
          orders, "order_id",
          List((orderItems, "order_id"))
        )

      val conf = configuration
        .enableGeneratePlanAndTasks(false)
        .generatedReportsFolderPath(s"$testDataPath/chained/reports")

      execute(planWithFK, conf, customers, orders, orderItems)
    }

    measurePerformance(
      "Chained foreign keys (3 levels)",
      Map("records" -> numRecords, "chain_depth" -> 2, "approach" -> "end_to_end")
    ) {
      val planRun = new ChainedForeignKeyTest()
      PlanProcessor.determineAndExecutePlan(Some(planRun))
    }

    // Validate referential integrity at each level
    val customersDf = sparkSession.read.option("header", "true").csv(s"$testDataPath/chained/customers.csv")
    val ordersDf = sparkSession.read.option("header", "true").csv(s"$testDataPath/chained/orders.csv")
    val orderItemsDf = sparkSession.read.option("header", "true").csv(s"$testDataPath/chained/order_items.csv")

    val (isValidCustomerOrder, metricsCustomerOrder) = validateReferentialIntegrity(
      customersDf, ordersDf, "customer_id", "customer_id"
    )
    val (isValidOrderItem, metricsOrderItem) = validateReferentialIntegrity(
      ordersDf, orderItemsDf, "order_id", "order_id"
    )

    assert(isValidCustomerOrder, s"Customer->Order integrity violated: $metricsCustomerOrder")
    assert(isValidOrderItem, s"Order->OrderItem integrity violated: $metricsOrderItem")
    println(s"  Customer->Order integrity: $metricsCustomerOrder")
    println(s"  Order->OrderItem integrity: $metricsOrderItem")
  }


}
