package io.github.datacatering.datacaterer.core.transformer

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.io.Source
import scala.collection.JavaConverters._

class TestUpperCaseTransformer {
  def transform(record: String): String = record.toUpperCase
}

class TestWholeFileTransformer {
  def transformFile(inputPath: String, outputPath: String): String = {
    val lines = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(inputPath))
    val uppercasedLines = lines.asScala.map(_.toUpperCase)
    java.nio.file.Files.write(java.nio.file.Paths.get(outputPath), uppercasedLines.asJava)
    outputPath
  }
}

class TestTransformationPlan extends PlanRun {
  val perRecordTask = csv("uppercase_accounts", s"/tmp/transformation/uppercase")
    .fields(
      field.name("account_id").regex("ACC[0-9]{8}"),
      field.name("name").expression("#{Name.name}"),
      field.name("status").oneOf("active", "inactive", "pending")
    )
    .count(count.records(10))
    .transformationPerRecord(
      "io.github.datacatering.datacaterer.core.transformer.TestUpperCaseTransformer",
      "transform"
    )

  execute(perRecordTask)
}

class TestWholeFileTransformationPlan extends PlanRun {
  val wholeFileTask = json("json_array_accounts", s"/tmp/transformation/whole_file_test")
    .fields(
      field.name("id").regex("[0-9]{10}"),
      field.name("name").expression("#{Name.name}")
    )
    .count(count.records(5))
    .transformationWholeFile(
      "io.github.datacatering.datacaterer.core.transformer.TestWholeFileTransformer",
      "transformFile"
    )

  execute(wholeFileTask)
}

class TransformationIntegrationTest extends SparkSuite with BeforeAndAfterAll with Matchers {

  // Use unique directory for this test to avoid conflicts with other tests
  private val tempTestDirectory = s"/tmp/data-caterer-transformation-test-${java.util.UUID.randomUUID().toString.take(8)}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create output directory
    new File(tempTestDirectory).mkdirs()
  }

  override def afterAll(): Unit = {
    cleanupTestFiles()
  }

  test("PerRecordTransformer should handle directory output and transform files within it") {
    // This test verifies that PerRecordTransformer correctly handles directory inputs
    // (which contain Spark part files) instead of failing with "Is a directory" error

    // Execute the plan
    val planRun = new TestTransformationPlan()
    PlanProcessor.determineAndExecutePlan(Some(planRun))

    // Verify output exists and is transformed
    val outputDir = new File(s"/tmp/transformation/uppercase")
    outputDir.exists() shouldBe true
    outputDir.isDirectory shouldBe true

    // Find transformed file (should be at /tmp/transformation/uppercase.transformed)
    val transformedFile = new File(s"/tmp/transformation/uppercase.transformed")
    transformedFile.exists() shouldBe true
    transformedFile.isFile shouldBe true

    // Verify content is uppercased (our TestUpperCaseTransformer converts to uppercase)
    val transformedContent = Source.fromFile(transformedFile).getLines().mkString("\n")
    // The content should be uppercase since our transformer uppercases everything
    transformedContent shouldBe transformedContent.toUpperCase
    // And it should contain actual data (not be empty)
    transformedContent.length should be > 0
  }

  test("WholeFileTransformer should handle directory output and consolidate files before transformation") {
    // This test verifies that WholeFileTransformer correctly handles directory inputs
    // by consolidating part files before calling the user transformation method

    // Execute the plan
    val planRun = new TestWholeFileTransformationPlan()
    PlanProcessor.determineAndExecutePlan(Some(planRun))

    // Verify output exists and is transformed
    val outputDir = new File(s"/tmp/transformation/whole_file_test")
    outputDir.exists() shouldBe true
    outputDir.isDirectory shouldBe true

    // Find transformed file (should be at /tmp/transformation/whole_file_test.transformed)
    val transformedFile = new File(s"/tmp/transformation/whole_file_test.transformed")
    transformedFile.exists() shouldBe true
    transformedFile.isFile shouldBe true

    // Verify content is uppercased (our TestWholeFileTransformer converts to uppercase)
    val transformedContent = Source.fromFile(transformedFile).getLines().mkString("\n")
    // The content should be uppercase since our transformer uppercases everything
    transformedContent shouldBe transformedContent.toUpperCase
    // And it should contain actual data (not be empty)
    transformedContent.length should be > 0
    // Should contain JSON objects
    transformedContent should include("\"ID\"")
    transformedContent should include("\"NAME\"")
  }

  private def cleanupTestFiles(): Unit = {
    // Clean up the temp directory
    val tempDir = new File(tempTestDirectory)
    if (tempDir.exists()) {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
