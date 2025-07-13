package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.scalatest.BeforeAndAfterEach

import java.io.File
import java.nio.file.{Files, Paths}

class ReferenceModeSimpleTest extends SparkSuite with BeforeAndAfterEach {

  private val testDataPath = "/tmp/data-caterer-reference-simple-test"
  private val referenceCSVPath = s"$testDataPath/reference.csv"
  private val outputJSONPath = s"$testDataPath/output.json"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Create test directory
    new File(testDataPath).mkdirs()
    
    // Create reference CSV file
    val csvContent = "name,email\nAlice,alice@test.com\nBob,bob@test.com\nCharlie,charlie@test.com"
    Files.write(Paths.get(referenceCSVPath), csvContent.getBytes)
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
    deleteRecursively(new File(testDataPath))
  }

  test("Reference mode should read CSV data and use it in foreign key relationship") {
    
    class TestReferenceModeSimple extends PlanRun {
      // Reference table with enableReferenceMode
      val referenceTable = csv("reference_data", referenceCSVPath, Map("header" -> "true"))
        .fields(
          field.name("name"),
          field.name("email")
        )
        .enableReferenceMode(true)

      // Main data source that references the CSV data
      val mainData = json("main_data", outputJSONPath, Map("saveMode" -> "overwrite"))
        .fields(
          field.name("user_name"),  // Will be populated from reference_data.name
          field.name("user_email"), // Will be populated from reference_data.email
          field.name("order_id").regex("ORD[0-9]{6}")
        )
        .count(count.records(3))  // Will match the reference data count

      // Create foreign key relationship
      val relation = plan.addForeignKeyRelationship(
        referenceTable, 
        List("name", "email"), 
        List((mainData, List("user_name", "user_email")))
      )

      val conf = configuration
        .enableGeneratePlanAndTasks(false)
        .generatedReportsFolderPath("/tmp/data-caterer-reference-simple-report")

      execute(relation, conf, mainData, referenceTable)
    }

    val planRun = new TestReferenceModeSimple()
    
    // Execute the plan - this will throw exception if it fails
    PlanProcessor.determineAndExecutePlan(Some(planRun))
    
    // Verify the output directory was created (Spark creates a directory, not a single file)
    assert(Files.exists(Paths.get(outputJSONPath)), "Output JSON directory should exist")
    
    // Read and verify the generated data
    val outputData = sparkSession.read.json(outputJSONPath)  // Remove multiline option - not needed for line-delimited JSON
    val collectedData = outputData.collect()
    
    // Debug: Print actual data for inspection
    println(s"Found ${collectedData.length} records:")
    collectedData.foreach(row => println(s"  Row: ${row.mkString}"))
    
    // Should have 3 records (one for each reference data record)
    assert(collectedData.length == 3, s"Expected 3 records, got ${collectedData.length}")
    
    // Verify all user_name values come from reference data
    val expectedNames = Set("Alice", "Bob", "Charlie")
    val actualNames = collectedData.map(_.getAs[String]("user_name")).toSet
    assert(actualNames.subsetOf(expectedNames), s"Generated names $actualNames should be subset of reference names $expectedNames")
    
    // Verify corresponding emails are correct
    val nameEmailMap = Map("Alice" -> "alice@test.com", "Bob" -> "bob@test.com", "Charlie" -> "charlie@test.com")
    collectedData.foreach { row =>
      val name = row.getAs[String]("user_name")
      val email = row.getAs[String]("user_email")
      val expectedEmail = nameEmailMap(name)
      assert(email == expectedEmail, s"For name $name, expected email $expectedEmail but got $email")
    }
    
    println("✅ Reference mode test passed!")
  }
} 