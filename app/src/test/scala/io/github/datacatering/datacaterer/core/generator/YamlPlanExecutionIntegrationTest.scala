package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Paths}

/**
 * Integration test that runs actual YAML plan and task files through PlanProcessor.executeFromYamlFiles
 * to test the complete end-to-end flow including YAML parsing.
 * 
 * This is different from unit tests that construct Scala/Java objects directly.
 */
class YamlPlanExecutionIntegrationTest extends SparkSuite with Matchers with BeforeAndAfterEach {

  private val testDataPath = "/tmp/data-caterer-yaml-execution-test"
  private val balancesPath = s"$testDataPath/balances"
  private val transactionsPath = s"$testDataPath/transactions"
  private val yamlDir = s"$testDataPath/yaml"
  private val planDir = s"$yamlDir/plan"
  private val taskDir = s"$yamlDir/task"

  override def beforeEach(): Unit = {
    super.beforeEach()
    deleteRecursively(new File(testDataPath))
    new File(testDataPath).mkdirs()
    new File(planDir).mkdirs()
    new File(taskDir).mkdirs()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    deleteRecursively(new File(testDataPath))
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.exists()) {
      if (file.isDirectory) {
        Option(file.listFiles).foreach(_.foreach(deleteRecursively))
      }
      file.delete()
    }
  }

  test("YAML execution: balances should have 1000 records, transactions should have 5000") {
    // Create plan YAML - matches the actual sample plan
    val planYaml =
      s"""name: "account_balance_and_transactions_create_plan"
         |description: "Create balances and transactions in Parquet files"
         |tasks:
         |  - name: "parquet_balance_and_transactions"
         |    dataSourceName: "parquet_ds"
         |
         |sinkOptions:
         |  foreignKeys:
         |    - source:
         |        dataSource: "parquet_ds"
         |        step: "balances"
         |        fields: [ "account_number" ]
         |      generate:
         |        - dataSource: "parquet_ds"
         |          step: "transactions"
         |          fields: [ "account_number" ]
         |""".stripMargin

    // Create task YAML - matches the actual sample task
    val taskYaml =
      s"""name: "parquet_balance_and_transactions"
         |steps:
         |  - name: "balances"
         |    type: "parquet"
         |    count:
         |      records: 1000
         |    options:
         |      path: "$balancesPath"
         |      format: "parquet"
         |    fields:
         |      - name: "account_number"
         |        options:
         |          regex: "ACC1[0-9]{5,10}"
         |          isUnique: true
         |      - name: "create_time"
         |        type: "timestamp"
         |      - name: "account_status"
         |        type: "string"
         |        options:
         |          oneOf:
         |            - "open"
         |            - "closed"
         |            - "suspended"
         |      - name: "balance"
         |        type: "double"
         |  - name: "transactions"
         |    type: "parquet"
         |    count:
         |      perField:
         |        fieldNames:
         |          - "account_number"
         |        count: 5
         |    options:
         |      path: "$transactionsPath"
         |      format: "parquet"
         |    fields:
         |      - name: "account_number"
         |      - name: "create_time"
         |        type: "timestamp"
         |      - name: "transaction_id"
         |        options:
         |          regex: "txn-[0-9]{10}"
         |      - name: "amount"
         |        type: "double"
         |""".stripMargin

    // Write YAML files
    val planFile = Paths.get(planDir, "plan.yaml")
    val taskFile = Paths.get(taskDir, "task.yaml")
    Files.write(planFile, planYaml.getBytes)
    Files.write(taskFile, taskYaml.getBytes)

    println(s"\n=== YAML Execution Test ===")
    println(s"Plan file: $planFile")
    println(s"Task folder: $taskDir")

    // Execute using the actual YAML flow
    val result = PlanProcessor.executeFromYamlFiles(planFile.toString, taskDir)

    println(s"\n=== Execution Results ===")
    println(s"Generation results: ${result.generationResults.size}")
    result.generationResults.foreach { dsr =>
      println(s"  ${dsr.name}: ${dsr.sinkResult}")
    }

    // Read back the generated data and verify counts
    val balancesData = sparkSession.read.parquet(balancesPath).collect()
    val transactionsData = sparkSession.read.parquet(transactionsPath).collect()

    println(s"\n=== Record Counts ===")
    println(s"Balances records: ${balancesData.length}")
    println(s"Transactions records: ${transactionsData.length}")

    // THE KEY ASSERTIONS - this is where the bug would manifest
    balancesData.length shouldBe 1000
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

  test("YAML execution with 500 balances: transactions should have 2500") {
    // Test with different balance count to ensure FK logic works correctly
    val planYaml =
      s"""name: "test_plan_500"
         |description: "Test with 500 balances"
         |tasks:
         |  - name: "test_task"
         |    dataSourceName: "parquet_ds"
         |
         |sinkOptions:
         |  foreignKeys:
         |    - source:
         |        dataSource: "parquet_ds"
         |        step: "balances"
         |        fields: [ "account_number" ]
         |      generate:
         |        - dataSource: "parquet_ds"
         |          step: "transactions"
         |          fields: [ "account_number" ]
         |""".stripMargin

    val taskYaml =
      s"""name: "test_task"
         |steps:
         |  - name: "balances"
         |    type: "parquet"
         |    count:
         |      records: 500
         |    options:
         |      path: "$balancesPath"
         |      format: "parquet"
         |    fields:
         |      - name: "account_number"
         |        options:
         |          regex: "ACC[0-9]{8}"
         |          isUnique: true
         |      - name: "balance"
         |        type: "double"
         |  - name: "transactions"
         |    type: "parquet"
         |    count:
         |      perField:
         |        fieldNames:
         |          - "account_number"
         |        count: 5
         |    options:
         |      path: "$transactionsPath"
         |      format: "parquet"
         |    fields:
         |      - name: "account_number"
         |      - name: "amount"
         |        type: "double"
         |""".stripMargin

    val planFile = Paths.get(planDir, "plan.yaml")
    val taskFile = Paths.get(taskDir, "task.yaml")
    Files.write(planFile, planYaml.getBytes)
    Files.write(taskFile, taskYaml.getBytes)

    println(s"\n=== YAML Execution Test (500 balances) ===")

    val result = PlanProcessor.executeFromYamlFiles(planFile.toString, taskDir)

    val balancesData = sparkSession.read.parquet(balancesPath).collect()
    val transactionsData = sparkSession.read.parquet(transactionsPath).collect()

    println(s"\n=== Record Counts ===")
    println(s"Balances records: ${balancesData.length}")
    println(s"Transactions records: ${transactionsData.length}")

    // Verify counts
    balancesData.length shouldBe 500
    transactionsData.length shouldBe 2500  // 500 * 5 = 2500
  }
}
