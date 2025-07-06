package io.github.datacatering.datacaterer.core.integration

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{StructType, StringType}
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.scalatest.BeforeAndAfterEach

import java.io.File
import java.nio.file.{Files, Paths}

class ReferenceModeIntegrationTest extends SparkSuite with BeforeAndAfterEach {

  private val testDataPath = "/tmp/data-caterer-reference-test"
  private val creditorTablePath = s"$testDataPath/creditor_reference.csv"
  private val outputPath = s"$testDataPath/output"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Create test directory
    new File(testDataPath).mkdirs()
    new File(outputPath).mkdirs()
    
    // Create creditor reference table as mentioned in user's example
    val creditorTableContent = 
      """creditor_name,creditor_account_identification
        |Bank of America,06212345678
        |Citigroup,01148573874
        |Wells Fargo,12345678901
        |Chase Bank,98765432109
        |TD Bank,55566677788""".stripMargin
    Files.write(Paths.get(creditorTablePath), creditorTableContent.getBytes)
  }

  override def afterEach(): Unit = {
    // Clean up test files
    def deleteRecursively(file: File): Unit = {
      if (file.isDirectory) file.listFiles.foreach(deleteRecursively)
      file.delete()
    }
    deleteRecursively(new File(testDataPath))
    super.afterEach()
  }

  test("Can use reference data in foreign key relationships for Swift message generation") {
    val testPlan = new SwiftMessageWithReferenceData()
    PlanProcessor.determineAndExecutePlan(Some(testPlan))
    
    // Verify that data was generated
    val generatedData = sparkSession.read
      .option("multiline", "true")
      .json(s"$outputPath/swift_messages")
      .collect()
    
    assert(generatedData.nonEmpty, "Should generate Swift message data")
    
    // Verify foreign key relationships are maintained
    val creditorData = sparkSession.read
      .option("header", "true")
      .csv(creditorTablePath)
      .collect()
    
    val creditorNames = creditorData.map(_.getAs[String]("creditor_name")).toSet
    val creditorAccounts = creditorData.map(_.getAs[String]("creditor_account_identification")).toSet
    
    generatedData.foreach { row =>
      val creditorName = row.getAs[String]("creditor_name")
      val creditorAccount = row.getAs[String]("creditor_account_identification")
      
      // Both should exist in reference data
      assert(creditorNames.contains(creditorName), s"Creditor name '$creditorName' should exist in reference data")
      assert(creditorAccounts.contains(creditorAccount), s"Creditor account '$creditorAccount' should exist in reference data")
      
      // Find the matching row in reference data to ensure they're from the same record
      val matchingRefRow = creditorData.find(_.getAs[String]("creditor_name") == creditorName)
      assert(matchingRefRow.isDefined, s"Should find matching reference row for creditor name '$creditorName'")
      assert(matchingRefRow.get.getAs[String]("creditor_account_identification") == creditorAccount,
        s"Creditor account should match the reference data for creditor name '$creditorName'")
    }
  }

  test("Reference mode works with multiple field foreign keys") {
    val testPlan = new MultiFieldReferenceTest()
    PlanProcessor.determineAndExecutePlan(Some(testPlan))
    
    // Verify that data was generated
    val generatedData = sparkSession.read
      .option("header", "true")
      .csv(s"$outputPath/transactions")
      .collect()
    
    assert(generatedData.nonEmpty, "Should generate transaction data")
    
    // Verify all generated records have valid reference data
    val referenceData = sparkSession.read
      .option("header", "true")
      .csv(creditorTablePath)
      .collect()
    
    val validCombinations = referenceData.map(row => 
      (row.getAs[String]("creditor_name"), row.getAs[String]("creditor_account_identification"))
    ).toSet
    
    generatedData.foreach { row =>
      val generatedCombination = (row.getAs[String]("bank_name"), row.getAs[String]("account_number"))
      assert(validCombinations.contains(generatedCombination),
        s"Generated combination $generatedCombination should exist in reference data")
    }
  }

  test("Reference mode validation prevents conflicting configurations") {
    val exception = intercept[IllegalArgumentException] {
      val testPlan = new InvalidConfigurationTest()
      PlanProcessor.determineAndExecutePlan(Some(testPlan))
    }
    assert(exception.getMessage.contains("Cannot enable both reference mode and data generation"))
  }

  /**
   * Test plan that simulates the Swift message scenario described by the user
   */
  class SwiftMessageWithReferenceData extends PlanRun {
    // Reference table with creditor information
    val creditorReference = csv("creditor_reference", creditorTablePath, Map("header" -> "true"))
      .fields(
        field.name("creditor_name"),
        field.name("creditor_account_identification")
      )
      .enableReferenceMode(true)

    // Swift message generation with foreign key to creditor reference
    val swiftMessages = json("swift_messages", s"$outputPath/swift_messages", Map("saveMode" -> "overwrite"))
      .fields(
        field.name("message_id").regex("MSG[0-9]{10}"),
        field.name("amount").`type`(StringType).regex("[0-9]{1,6}\\.[0-9]{2}"),
        field.name("currency").oneOf("USD", "EUR", "GBP"),
        field.name("creditor_name"),  // This should come from reference
        field.name("creditor_account_identification"),  // This should come from reference
        field.name("debtor_name").expression("#{Name.name}"),
        field.name("debtor_account").regex("[0-9]{10,12}")
      )
      .count(count.records(20))

    // Define foreign key relationship
    val planWithForeignKeys = plan.addForeignKeyRelationship(
      creditorReference, List("creditor_name", "creditor_account_identification"),
      List((swiftMessages, List("creditor_name", "creditor_account_identification")))
    )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithForeignKeys, config, creditorReference, swiftMessages)
  }

  /**
   * Test plan with multiple field foreign key relationships
   */
  class MultiFieldReferenceTest extends PlanRun {
    // Reference table
    val bankReference = csv("bank_reference", creditorTablePath, Map("header" -> "true"))
      .fields(
        field.name("creditor_name"),
        field.name("creditor_account_identification")
      )
      .enableReferenceMode(true)

    // Transaction data that references bank information
    val transactions = csv("transactions", s"$outputPath/transactions", 
      Map("saveMode" -> "overwrite", "header" -> "true"))
      .fields(
        field.name("transaction_id").regex("TXN[0-9]{8}"),
        field.name("amount").regex("[0-9]{1,4}\\.[0-9]{2}"),
        field.name("bank_name"),  // Maps to creditor_name
        field.name("account_number"),  // Maps to creditor_account_identification
        field.name("transaction_date").`type`(StringType).sql("CURRENT_DATE()")
      )
      .count(count.records(15))

    // Define foreign key relationship with field mapping
    val planWithForeignKeys = plan.addForeignKeyRelationship(
      bankReference, List("creditor_name", "creditor_account_identification"),
      List((transactions, List("bank_name", "account_number")))
    )

    val config = configuration
      .enableGenerateData(true)
      .enableValidation(false)
      .generatedReportsFolderPath(s"$testDataPath/reports")

    execute(planWithForeignKeys, config, bankReference, transactions)
  }

  /**
   * Test plan with invalid configuration (both reference mode and data generation enabled)
   */
  class InvalidConfigurationTest extends PlanRun {
    val invalidStep = csv("invalid_step", creditorTablePath, Map("header" -> "true"))
      .fields(
        field.name("creditor_name"),
        field.name("creditor_account_identification")
      )
      .enableReferenceMode(true)
      .enableDataGeneration(true)  // This should cause validation to fail

    val config = configuration.enableGenerateData(true)

    execute(config, invalidStep)
  }
} 