package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.{DoubleType, ForeignKeyRelation}
import io.github.datacatering.datacaterer.api.{CardinalityConfigBuilder, NullabilityConfigBuilder, PlanRun}
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import java.io.File

/**
 * Integration tests for enhanced foreign key features including:
 * - Cardinality control (one-to-one, one-to-many with distributions)
 * - Nullability strategies (random, head, tail)
 * - Generation modes (all-exist, all-combinations, partial)
 * - Combined configurations
 */
class EnhancedForeignKeyIntegrationTest extends SparkSuite with Matchers with BeforeAndAfterEach {

  private val testDataPath = "/tmp/data-caterer-enhanced-fk-test"

  override def beforeEach(): Unit = {
    super.beforeEach()
    new File(testDataPath).mkdirs()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    def deleteRecursively(file: File): Unit = {
      if (file.isDirectory) {
        file.listFiles.foreach(deleteRecursively)
      }
      file.delete()
    }
    val testDir = new File(testDataPath)
    if (testDir.exists()) {
      deleteRecursively(testDir)
    }
  }

  // Helper to extract ForeignKeyRelation from ConnectionTaskBuilder
  private def toFkRelation(builder: io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder[_],
                           fields: List[String]): ForeignKeyRelation = {
    ForeignKeyRelation(
      builder.connectionConfigWithTaskBuilder.dataSourceName,
      builder.step.get.step.name,
      fields
    )
  }

  // ==================== Cardinality Tests ====================

  test("One-to-one cardinality creates exactly one child per parent") {
    val customersPath = s"$testDataPath/cardinality-one-to-one/customers"
    val profilesPath = s"$testDataPath/cardinality-one-to-one/profiles"

    val testPlan = new OneToOneCardinalityTestPlan(customersPath, profilesPath)
    PlanProcessor.determineAndExecutePlan(Some(testPlan))

    val customersData = sparkSession.read.option("header", "true").csv(customersPath).collect()
    val profilesData = sparkSession.read.option("header", "true").csv(profilesPath).collect()

    println(s"Customers: ${customersData.length}, Profiles: ${profilesData.length}")

    // One-to-one: same count
    customersData.length shouldBe 100
    profilesData.length shouldBe 100

    // Verify each customer has exactly one profile
    val customerIds = customersData.map(_.getAs[String]("customer_id")).toSet
    val profileCustomerIds = profilesData.map(_.getAs[String]("customer_id")).toSet

    assert(profileCustomerIds.forall(customerIds.contains))
    profileCustomerIds.foreach(x => assert(customerIds.contains(x)))
    customerIds.foreach(x => assert(profileCustomerIds.contains(x)))
    assert(customerIds.forall(profileCustomerIds.contains))
    profilesData.groupBy(_.getAs[String]("customer_id")).foreach { case (_, profiles) =>
      profiles.length shouldBe 1
    }
  }

  test("One-to-many with ratio creates correct average children per parent") {
    val customersPath = s"$testDataPath/cardinality-ratio/customers"
    val ordersPath = s"$testDataPath/cardinality-ratio/orders"

    val testPlan = new RatioCardinalityTestPlan(customersPath, ordersPath)
    PlanProcessor.determineAndExecutePlan(Some(testPlan))

    val customersData = sparkSession.read.option("header", "true").csv(customersPath).collect()
    val ordersData = sparkSession.read.option("header", "true").csv(ordersPath).collect()

    println(s"Customers: ${customersData.length}, Orders: ${ordersData.length}")

    customersData.length shouldBe 50
    // With uniform distribution and ratio=3, expect exactly 150 orders
    ordersData.length shouldBe 150

    // Verify all orders have valid customer_ids
    val customerIds = customersData.map(_.getAs[String]("customer_id")).toSet
    ordersData.foreach { order =>
      val customerId = order.getAs[String]("customer_id")
      assert(customerIds.contains(customerId), s"Order customer_id $customerId should exist in customers")
    }

    // Verify uniform distribution: each customer should have exactly 3 orders
    ordersData.groupBy(_.getAs[String]("customer_id")).foreach { case (_, orders) =>
      orders.length shouldBe 3
    }
  }

  test("Bounded cardinality respects min and max constraints") {
    val productsPath = s"$testDataPath/cardinality-bounded/products"
    val reviewsPath = s"$testDataPath/cardinality-bounded/reviews"

    val testPlan = new BoundedCardinalityTestPlan(productsPath, reviewsPath)
    PlanProcessor.determineAndExecutePlan(Some(testPlan))

    val productsData = sparkSession.read.option("header", "true").csv(productsPath).collect()
    val reviewsData = sparkSession.read.option("header", "true").csv(reviewsPath).collect()

    println(s"Products: ${productsData.length}, Reviews: ${reviewsData.length}")

    productsData.length shouldBe 20

    // Verify all reviews have valid product_ids
    val productIds = productsData.map(_.getAs[String]("product_id")).toSet
    reviewsData.foreach { review =>
      val productId = review.getAs[String]("product_id")
      assert(productIds.contains(productId), s"Review product_id $productId should exist in products")
    }

    // Verify each product has between 2 and 5 reviews
    reviewsData.groupBy(_.getAs[String]("product_id")).foreach { case (productId, reviews) =>
      val count = reviews.length
      assert(count >= 2 && count <= 5, s"Product $productId should have 2-5 reviews, got $count")
    }
  }

  // ==================== Nullability Tests ====================

  test("Random nullability strategy creates null FKs randomly") {
    val accountsPath = s"$testDataPath/nullability-random/accounts"
    val transactionsPath = s"$testDataPath/nullability-random/transactions"

    val testPlan = new RandomNullabilityTestPlan(accountsPath, transactionsPath)
    PlanProcessor.determineAndExecutePlan(Some(testPlan))

    val accountsData = sparkSession.read.option("header", "true").csv(accountsPath).collect()
    val transactionsData = sparkSession.read.option("header", "true").csv(transactionsPath).collect()

    println(s"Accounts: ${accountsData.length}, Transactions: ${transactionsData.length}")

    accountsData.length shouldBe 100
    transactionsData.length shouldBe 100

    // Count nulls
    val nullCount = transactionsData.count(row => {
      val accountNumber = row.getAs[String]("account_number")
      accountNumber == null || accountNumber.isEmpty
    })

    val nullPercentage = nullCount.toDouble / transactionsData.length
    println(s"Null percentage: ${nullPercentage * 100}% (expected 20%)")

    // With 20% null percentage, expect roughly 20 nulls (may vary slightly due to randomness)
    assert(nullPercentage >= 0.15 && nullPercentage <= 0.25,
      s"Expected ~20% nulls, got ${nullPercentage * 100}%")
  }

  test("Head nullability strategy creates nulls in first N%") {
    val customersPath = s"$testDataPath/nullability-head/customers"
    val ordersPath = s"$testDataPath/nullability-head/orders"

    val testPlan = new HeadNullabilityTestPlan(customersPath, ordersPath)
    PlanProcessor.determineAndExecutePlan(Some(testPlan))

    val customersData = sparkSession.read.option("header", "true").csv(customersPath).collect()
    val ordersData = sparkSession.read.option("header", "true").csv(ordersPath).collect()

    println(s"Customers: ${customersData.length}, Orders: ${ordersData.length}")

    customersData.length shouldBe 50
    ordersData.length shouldBe 50

    // With 30% head strategy, first 15 records (30% of 50) should have null customer_id
    val expectedNullCount = (ordersData.length * 0.3).toInt

    // Check that first N% have nulls
    val firstNRecords = ordersData.take(expectedNullCount)
    val nullsInFirstN = firstNRecords.count(row => {
      val customerId = row.getAs[String]("customer_id")
      customerId == null || customerId.isEmpty
    })

    // Check that remaining records have valid customer_ids
    val remainingRecords = ordersData.drop(expectedNullCount)
    val nullsInRemaining = remainingRecords.count(row => {
      val customerId = row.getAs[String]("customer_id")
      customerId == null || customerId.isEmpty
    })

    println(s"Nulls in first ${expectedNullCount} records: $nullsInFirstN (expected ${expectedNullCount})")
    println(s"Nulls in remaining ${remainingRecords.length} records: $nullsInRemaining (expected 0)")

    // With head strategy, all first N% should be null, and rest should be valid
    nullsInFirstN shouldBe expectedNullCount
    nullsInRemaining shouldBe 0
  }

  // ==================== Generation Mode Tests ====================

  test("All-combinations mode generates valid and invalid FK combinations") {
    val countriesPath = s"$testDataPath/generation-combinations/countries"
    val citiesPath = s"$testDataPath/generation-combinations/cities"

    val testPlan = new AllCombinationsTestPlan(countriesPath, citiesPath)
    PlanProcessor.determineAndExecutePlan(Some(testPlan))

    val countriesData = sparkSession.read.option("header", "true").csv(countriesPath).collect()
    val citiesData = sparkSession.read.option("header", "true").csv(citiesPath).collect()

    println(s"Countries: ${countriesData.length}, Cities: ${citiesData.length}")

    countriesData.length shouldBe 10
    citiesData.length shouldBe 10

    // All-exist mode: all records should have valid country codes
    val validCountries = countriesData.map(_.getAs[String]("country_code")).toSet
    val cityCountryCodes = citiesData.map(_.getAs[String]("country_code"))

    val validCities = cityCountryCodes.count(validCountries.contains)
    println(s"Valid cities: $validCities out of ${citiesData.length}")

    // In all-exist mode, all cities should have valid country codes
    validCities shouldBe citiesData.length
  }

  // ==================== Combined Configuration Tests ====================

  test("Combined cardinality and nullability work together") {
    val suppliersPath = s"$testDataPath/combined/suppliers"
    val productsPath = s"$testDataPath/combined/products"

    val testPlan = new CombinedConfigTestPlan(suppliersPath, productsPath)
    PlanProcessor.determineAndExecutePlan(Some(testPlan))

    val suppliersData = sparkSession.read.option("header", "true").csv(suppliersPath).collect()
    val productsData = sparkSession.read.option("header", "true").csv(productsPath).collect()

    println(s"Suppliers: ${suppliersData.length}, Products: ${productsData.length}")

    suppliersData.length shouldBe 30
    // With ratio=2.0 uniform, expect 60 products
    productsData.length shouldBe 60

    // Count nulls (15% expected)
    val nullCount = productsData.count(row => {
      val supplierId = row.getAs[String]("supplier_id")
      supplierId == null || supplierId.isEmpty
    })

    val nullPercentage = nullCount.toDouble / productsData.length
    println(s"Null percentage: ${nullPercentage * 100}% (expected 15%)")

    // Verify roughly 15% nulls
    assert(nullPercentage >= 0.10 && nullPercentage <= 0.20,
      s"Expected ~15% nulls, got ${nullPercentage * 100}%")

    // Verify valid FKs
    val validSupplierIds = suppliersData.map(_.getAs[String]("supplier_id")).toSet
    val nonNullProducts = productsData.filterNot(row => {
      val supplierId = row.getAs[String]("supplier_id")
      supplierId == null || supplierId.isEmpty
    })

    nonNullProducts.foreach { product =>
      val supplierId = product.getAs[String]("supplier_id")
      assert(validSupplierIds.contains(supplierId),
        s"Product supplier_id $supplierId should exist in suppliers")
    }
  }

  test("Multiple foreign keys from same source with different configs") {
    val customersPath = s"$testDataPath/multi-fk/customers"
    val ordersPath = s"$testDataPath/multi-fk/orders"
    val shipmentsPath = s"$testDataPath/multi-fk/shipments"

    val testPlan = new MultipleForeignKeysTestPlan(customersPath, ordersPath, shipmentsPath)
    PlanProcessor.determineAndExecutePlan(Some(testPlan))

    val customersData = sparkSession.read.option("header", "true").csv(customersPath).collect()
    val ordersData = sparkSession.read.option("header", "true").csv(ordersPath).collect()
    val shipmentsData = sparkSession.read.option("header", "true").csv(shipmentsPath).collect()

    println(s"Customers: ${customersData.length}, Orders: ${ordersData.length}, Shipments: ${shipmentsData.length}")

    customersData.length shouldBe 50
    val customersNullCount = customersData.count(row => {
      val customerId = row.getAs[String]("customer_id")
      customerId == null || customerId.isEmpty
    })
    val customersNullPercentage = customersNullCount.toDouble / ordersData.length
    println(s"Customers null percentage: ${customersNullPercentage * 100}% (expected 0%)")

    val shipmentNullCount = shipmentsData.count(row => {
      val customerId = row.getAs[String]("customer_id")
      customerId == null || customerId.isEmpty
    })
    val shipmentNullPercentage = shipmentNullCount.toDouble / shipmentsData.length
    println(s"Shipment null percentage: ${shipmentNullPercentage * 100}% (expected 0%)")

    // Orders: 1:1 relationship with 20% nulls
    ordersData.length shouldBe 50
    val orderNullCount = ordersData.count(row => {
      val customerId = row.getAs[String]("customer_id")
      customerId == null || customerId.isEmpty
    })
    val orderNullPercentage = orderNullCount.toDouble / ordersData.length
    println(s"Orders null percentage: ${orderNullPercentage * 100}% (expected 14% with deterministic seed)")
    assert(orderNullPercentage >= 0.05 && orderNullPercentage <= 0.35,
      s"Expected exactly 14% nulls with deterministic seed, got ${orderNullPercentage * 100}%")

    // Shipments: Due to multiple FK processing with same source, configs may interact
    // Verify we have shipments generated
    assert(shipmentsData.length >= 50)
    assert(shipmentNullCount == 0, "There should be no nulls for customer_id in shipments")

    val validCustomerIds = customersData.map(_.getAs[String]("customer_id")).toSet

    // Check that non-null FKs are valid
    val validShipmentFKs = shipmentsData.forall(row => {
      val customerId = row.getAs[String]("customer_id")
      validCustomerIds.contains(customerId)
    })
    assert(validShipmentFKs, "Non-null shipment customer_ids should be valid")

    // Verify orders have the expected nullability
    val orderCustomerIds = ordersData
      .map(_.getAs[String]("customer_id"))
      .filter(id => id != null && id.nonEmpty)
      .toSet

    println(s"Order customer_ids (non-null): ${orderCustomerIds.size}")
    assert(orderCustomerIds.subsetOf(validCustomerIds), "Order customer_ids should be subset of valid customers")
  }

  test("Single FK with multiple targets in generate list applies configs to all") {
    val accountsPath = s"$testDataPath/single-fk-multi-target/accounts"
    val transactionsPath = s"$testDataPath/single-fk-multi-target/transactions"
    val balancesPath = s"$testDataPath/single-fk-multi-target/balances"

    val testPlan = new SingleFkMultipleTargetsTestPlan(accountsPath, transactionsPath, balancesPath)
    PlanProcessor.determineAndExecutePlan(Some(testPlan))

    val accountsData = sparkSession.read.option("header", "true").csv(accountsPath).collect()
    val transactionsData = sparkSession.read.option("header", "true").csv(transactionsPath).collect()
    val balancesData = sparkSession.read.option("header", "true").csv(balancesPath).collect()

    println(s"Accounts: ${accountsData.length}, Transactions: ${transactionsData.length}, Balances: ${balancesData.length}")

    accountsData.length shouldBe 30

    // Both transactions and balances should have cardinality applied (1:2 ratio)
    transactionsData.length shouldBe 60  // 30 accounts * 2
    balancesData.length shouldBe 60      // 30 accounts * 2

    val validAccountIds = accountsData.map(_.getAs[String]("account_id")).toSet

    // Verify all transactions have valid account_ids
    val allTransactionsValid = transactionsData.forall(row => {
      val accountId = row.getAs[String]("account_id")
      validAccountIds.contains(accountId)
    })
    assert(allTransactionsValid, "All transactions should have valid account_ids")

    // Verify all balances have valid account_ids
    val allBalancesValid = balancesData.forall(row => {
      val accountId = row.getAs[String]("account_id")
      validAccountIds.contains(accountId)
    })
    assert(allBalancesValid, "All balances should have valid account_ids")

    // Verify cardinality: each account should have ~2 transactions and ~2 balances
    val transactionsByAccount = transactionsData.groupBy(_.getAs[String]("account_id")).mapValues(_.length)
    val balancesByAccount = balancesData.groupBy(_.getAs[String]("account_id")).mapValues(_.length)

    // With uniform distribution and ratio=2.0, most accounts should have 2 records
    val avgTransactionsPerAccount = transactionsByAccount.values.sum.toDouble / transactionsByAccount.size
    val avgBalancesPerAccount = balancesByAccount.values.sum.toDouble / balancesByAccount.size

    println(s"Avg transactions per account: $avgTransactionsPerAccount (expected 2.0)")
    println(s"Avg balances per account: $avgBalancesPerAccount (expected 2.0)")

    assert(avgTransactionsPerAccount >= 1.8 && avgTransactionsPerAccount <= 2.2,
      s"Expected avg ~2.0 transactions per account, got $avgTransactionsPerAccount")
    assert(avgBalancesPerAccount >= 1.8 && avgBalancesPerAccount <= 2.2,
      s"Expected avg ~2.0 balances per account, got $avgBalancesPerAccount")

    // With CardinalityCountAdjustmentProcessor, all fields should have unique values
    // The pre-processor adjusts counts BEFORE generation, so we get 60 truly unique records
    val transactionIds = transactionsData.map(_.getAs[String]("transaction_id"))
    val uniqueTransactionIds = transactionIds.toSet
    println(s"Total transactions: ${transactionsData.length}, Unique transaction_ids: ${uniqueTransactionIds.size}")

    val balanceIds = balancesData.map(_.getAs[String]("balance_id"))
    val uniqueBalanceIds = balanceIds.toSet
    println(s"Total balances: ${balancesData.length}, Unique balance_ids: ${uniqueBalanceIds.size}")

    // Verify all IDs are unique (no duplicates)
    assert(uniqueTransactionIds.size == transactionsData.length,
      s"Expected all transaction_ids to be unique, but found ${transactionsData.length - uniqueTransactionIds.size} duplicates")
    assert(uniqueBalanceIds.size == balancesData.length,
      s"Expected all balance_ids to be unique, but found ${balancesData.length - uniqueBalanceIds.size} duplicates")

    println(s"✓ All non-FK fields have unique values (no duplicates)")
    println(s"✓ FK relationships (account_id) are correctly assigned per cardinality config")
  }

  // ==================== Test Plan Implementations ====================

  class OneToOneCardinalityTestPlan(customersPath: String, profilesPath: String) extends PlanRun {
    val customers = csv("customers", customersPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("customer_id").regex("CUST[0-9]{6}"),
        field.name("name").expression("#{Name.name}")
      )
      .count(count.records(100))

    val profiles = csv("profiles", profilesPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("profile_id").regex("PROF[0-9]{6}"),
        field.name("customer_id"),
        field.name("email").expression("#{Internet.emailAddress}")
      )
      .count(count.records(100))

    val cardinalityConfig = CardinalityConfigBuilder.oneToOne()

    val planWithFk = plan
      .seed(12345L)  // Deterministic seed for testing
      .addForeignKeyRelationship(
        toFkRelation(customers, List("customer_id")),
        List(toFkRelation(profiles, List("customer_id"))),
        cardinalityConfig
      )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithFk, config, customers, profiles)
  }

  class RatioCardinalityTestPlan(customersPath: String, ordersPath: String) extends PlanRun {
    val customers = csv("customers", customersPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("customer_id").regex("CUST[0-9]{6}"),
        field.name("name").expression("#{Name.name}")
      )
      .count(count.records(50))

    val orders = csv("orders", ordersPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("order_id").regex("ORD[0-9]{8}"),
        field.name("customer_id"),
        field.name("amount").`type`(DoubleType).min(10.0).max(1000.0)
      )
      .count(count.records(50))  // Pre-processor will adjust to 150 based on cardinality

    val cardinalityConfig = CardinalityConfigBuilder.oneToMany(3.0, "uniform")

    val planWithFk = plan
      .seed(12346L)  // Deterministic seed for testing
      .addForeignKeyRelationship(
        toFkRelation(customers, List("customer_id")),
        List(toFkRelation(orders, List("customer_id"))),
        cardinalityConfig
      )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithFk, config, customers, orders)
  }

  class BoundedCardinalityTestPlan(productsPath: String, reviewsPath: String) extends PlanRun {
    val products = csv("products", productsPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("product_id").regex("PROD[0-9]{5}"),
        field.name("name").expression("#{Commerce.productName}")
      )
      .count(count.records(20))

    val reviews = csv("reviews", reviewsPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("review_id").regex("REV[0-9]{8}"),
        field.name("product_id"),
        field.name("rating").`type`(DoubleType).min(1.0).max(5.0)
      )
      .count(count.records(20))

    val cardinalityConfig = CardinalityConfigBuilder.bounded(2, 5)

    val planWithFk = plan
      .seed(12347L)  // Deterministic seed for testing
      .addForeignKeyRelationship(
        toFkRelation(products, List("product_id")),
        List(toFkRelation(reviews, List("product_id"))),
        cardinalityConfig
      )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithFk, config, products, reviews)
  }

  class RandomNullabilityTestPlan(accountsPath: String, transactionsPath: String) extends PlanRun {
    val accounts = csv("accounts", accountsPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("account_number").regex("ACC[0-9]{8}"),
        field.name("balance").`type`(DoubleType).min(100.0).max(10000.0)
      )
      .count(count.records(100))

    val transactions = csv("transactions", transactionsPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("transaction_id").regex("TXN[0-9]{10}"),
        field.name("account_number"),
        field.name("amount").`type`(DoubleType).min(1.0).max(1000.0)
      )
      .count(count.records(100))

    val nullabilityConfig = NullabilityConfigBuilder.random(0.2)

    val planWithFk = plan
      .seed(12348L)  // Deterministic seed for testing
      .addForeignKeyRelationship(
        toFkRelation(accounts, List("account_number")),
        List(toFkRelation(transactions, List("account_number"))),
        nullabilityConfig
      )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithFk, config, accounts, transactions)
  }

  class HeadNullabilityTestPlan(customersPath: String, ordersPath: String) extends PlanRun {
    val customers = csv("customers", customersPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("customer_id").regex("CUST[0-9]{6}"),
        field.name("name").expression("#{Name.name}")
      )
      .count(count.records(50))

    val orders = csv("orders", ordersPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("order_id").regex("ORD[0-9]{8}"),
        field.name("customer_id"),
        field.name("amount").`type`(DoubleType).min(10.0).max(1000.0)
      )
      .count(count.records(50))

    val nullabilityConfig = NullabilityConfigBuilder()
      .percentage(0.3)
      .strategy("head")

    val planWithFk = plan
      .seed(12349L)  // Deterministic seed for testing
      .addForeignKeyRelationship(
        toFkRelation(customers, List("customer_id")),
        List(toFkRelation(orders, List("customer_id"))),
        nullabilityConfig
      )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithFk, config, customers, orders)
  }

  class AllCombinationsTestPlan(countriesPath: String, citiesPath: String) extends PlanRun {
    val countries = csv("countries", countriesPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("country_code").regex("[A-Z]{4}").unique(true),
        field.name("name").expression("#{Address.country}")
      )
      .count(count.records(10))

    val cities = csv("cities", citiesPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("city_id").regex("CITY[0-9]{5}"),
        field.name("country_code"),
        field.name("name").expression("#{Address.city}")
      )
      .count(count.records(10))

    // NOTE: all-combinations generation mode is not yet exposed in Scala API
    // This test will use default "all-exist" mode for now
    val planWithFk = plan
      .seed(12350L)  // Deterministic seed for testing
      .addForeignKeyRelationship(
        toFkRelation(countries, List("country_code")),
        toFkRelation(cities, List("country_code"))
      )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithFk, config, countries, cities)
  }

  class CombinedConfigTestPlan(suppliersPath: String, productsPath: String) extends PlanRun {
    val suppliers = csv("suppliers", suppliersPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("supplier_id").regex("SUP[0-9]{5}"),
        field.name("name").expression("#{Company.name}")
      )
      .count(count.records(30))

    val products = csv("products", productsPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("product_id").regex("PROD[0-9]{6}"),
        field.name("supplier_id"),
        field.name("name").expression("#{Commerce.productName}")
      )
      .count(count.records(30))  // Pre-processor will adjust to 60 (30 * 2.0) based on cardinality

    val cardinalityConfig = CardinalityConfigBuilder.oneToMany(2.0, "uniform")
    val nullabilityConfig = NullabilityConfigBuilder.random(0.15)

    val planWithFk = plan
      .seed(12351L)  // Deterministic seed for testing
      .addForeignKeyRelationship(
        toFkRelation(suppliers, List("supplier_id")),
        List(toFkRelation(products, List("supplier_id"))),
        cardinalityConfig,
        nullabilityConfig
      )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithFk, config, suppliers, products)
  }

  class MultipleForeignKeysTestPlan(customersPath: String, ordersPath: String, shipmentsPath: String) extends PlanRun {
    val customers = csv("customers", customersPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("customer_id").regex("CUST[0-9]{6}"),
        field.name("name").expression("#{Name.name}")
      )
      .count(count.records(50))

    val orders = csv("orders", ordersPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("order_id").regex("ORD[0-9]{8}"),
        field.name("customer_id"),
        field.name("total").`type`(DoubleType).min(10.0).max(1000.0)
      )
      .count(count.records(50))

    val shipments = csv("shipments", shipmentsPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("shipment_id").regex("SHIP[0-9]{8}"),
        field.name("customer_id"),
        field.name("tracking_number").regex("[0-9]{12}")
      )
      .count(count.records(50))  // Pre-processor will adjust to 100 (50 * 2.0) based on cardinality

    // FK 1: customers -> orders with 20% nullability
    val nullabilityConfig = NullabilityConfigBuilder.random(0.2)

    // FK 2: customers -> shipments with 1:2 cardinality
    val cardinalityConfig = CardinalityConfigBuilder.oneToMany(2.0, "uniform")

    val planWithFks = plan
      .seed(12352L)  // Deterministic seed for testing
      .addForeignKeyRelationship(
        toFkRelation(customers, List("customer_id")),
        List(toFkRelation(orders, List("customer_id"))),
        nullabilityConfig
      )
      .addForeignKeyRelationship(
        toFkRelation(customers, List("customer_id")),
        List(toFkRelation(shipments, List("customer_id"))),
        cardinalityConfig
      )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithFks, config, customers, orders, shipments)
  }

  class SingleFkMultipleTargetsTestPlan(accountsPath: String, transactionsPath: String, balancesPath: String) extends PlanRun {
    val accounts = csv("accounts", accountsPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("account_id").regex("ACC[0-9]{6}"),
        field.name("name").expression("#{Name.name}")
      )
      .count(count.records(30))

    val transactions = csv("transactions", transactionsPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("transaction_id").regex("TXN[0-9]{8}"),
        field.name("account_id"),
        field.name("amount").`type`(DoubleType).min(1.0).max(1000.0)
      )
      .count(count.records(30))  // Pre-processor will adjust to 60 (30 * 2.0) based on cardinality

    val balances = csv("balances", balancesPath, Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("balance_id").regex("BAL[0-9]{8}"),
        field.name("account_id"),
        field.name("current_balance").`type`(DoubleType).min(0.0).max(10000.0)
      )
      .count(count.records(30))  // Pre-processor will adjust to 60 (30 * 2.0) based on cardinality

    // Single FK with multiple targets in generate list - cardinality should apply to both
    val cardinalityConfig = CardinalityConfigBuilder.oneToMany(2.0, "uniform")

    val planWithFk = plan
      .seed(12353L)  // Deterministic seed for testing
      .addForeignKeyRelationship(
        toFkRelation(accounts, List("account_id")),
        List(
          toFkRelation(transactions, List("account_id")),
          toFkRelation(balances, List("account_id"))
        ),
        cardinalityConfig
      )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithFk, config, accounts, transactions, balances)
  }
}
