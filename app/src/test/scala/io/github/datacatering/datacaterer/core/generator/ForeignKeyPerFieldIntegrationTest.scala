package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.DoubleType
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Paths

/**
 * Integration test to verify that foreign key + perField count works correctly
 * without double-multiplying the record count.
 *
 * This test ensures that when a foreign key relationship exists and the target
 * step has a perField count, the system correctly calculates the total number
 * of records without applying the perField multiplier twice.
 */
class ForeignKeyPerFieldIntegrationTest extends SparkSuite with Matchers with BeforeAndAfterEach {

  private val testDataPath = "/tmp/data-caterer-fk-perfield-test"
  private val balancesPath = s"$testDataPath/balances"
  private val transactionsPath = s"$testDataPath/transactions"

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
    val testDir = new File(testDataPath)
    if (testDir.exists()) {
      deleteRecursively(testDir)
    }
  }

  test("Foreign key with perField should generate correct number of records") {
    // Execute the test plan through PlanProcessor
    val testPlan = new ForeignKeyPerFieldTestPlan()
    PlanProcessor.determineAndExecutePlan(Some(testPlan))

    // Verify the generated data
    val balancesData = sparkSession.read
      .option("header", "true")
      .csv(balancesPath)
      .collect()

    val transactionsData = sparkSession.read
      .option("header", "true")
      .csv(transactionsPath)
      .collect()

    println(s"Generated balances records: ${balancesData.length}")
    println(s"Generated transactions records: ${transactionsData.length}")

    // Verify counts
    balancesData.length shouldBe 1000
    // With perField=5 and 1000 balances, we should get exactly 5000 transactions
    // NOT 25000 (which would be if perField was applied twice)
    transactionsData.length shouldBe 5000

    // Verify foreign key relationship is maintained
    val balancesAccountNumbers = balancesData.map(_.getAs[String]("account_number")).toSet
    val transactionsAccountNumbers = transactionsData.map(_.getAs[String]("account_number")).toSet

    // All transaction account numbers should exist in balances
    transactionsAccountNumbers.foreach { accountNumber =>
      assert(balancesAccountNumbers.contains(accountNumber),
        s"Transaction account number $accountNumber should exist in balances")
    }

    // Verify each account_number has 5 transactions
    transactionsData.groupBy(_.getAs[String]("account_number")).foreach { case (accountNumber, transactions) =>
      assert(transactions.length == 5,
        s"Expected 5 transactions for account $accountNumber, got ${transactions.length}")
    }
  }

  /**
   * Test plan that simulates the YAML configuration from the failing CI test.
   * Source: balances with 1000 records
   * Target: transactions with perField count = 5 per account_number
   */
  class ForeignKeyPerFieldTestPlan extends PlanRun {
    // Source data: balances with 1000 records
    val balances = csv("balances", balancesPath,
      Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("account_number").regex("ACC[0-9]{8}"),
        field.name("balance").`type`(DoubleType).min(100.0).max(10000.0)
      )
      .count(count.records(1000))

    // Target data: transactions with perField count = 5 per account_number
    val transactions = csv("transactions", transactionsPath,
      Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("account_number"),  // Will come from foreign key
        field.name("transaction_id").regex("TXN[0-9]{10}"),
        field.name("amount").`type`(DoubleType).min(1.0).max(1000.0)
      )
      .count(count.recordsPerField(5, "account_number"))

    // Define foreign key relationship
    val planWithForeignKeys = plan.addForeignKeyRelationship(
      balances, List("account_number"),
      List((transactions, List("account_number")))
    )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithForeignKeys, config, balances, transactions)
  }
}
