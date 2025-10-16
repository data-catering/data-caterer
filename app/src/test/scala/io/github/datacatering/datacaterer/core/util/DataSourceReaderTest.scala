package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.Step
import org.scalatest.BeforeAndAfterEach

import java.io.File
import java.nio.file.{Files, Paths}

class DataSourceReaderTest extends SparkSuite with BeforeAndAfterEach {

  private val testDataPath = "/tmp/data-caterer-test"
  private val csvTestFile = s"$testDataPath/reference.csv"
  private val jsonTestFile = s"$testDataPath/reference.json"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Create test directory
    new File(testDataPath).mkdirs()
    
    // Create test CSV file
    val csvContent = "name,email\nAlice,alice@example.com\nBob,bob@example.com\nCharlie,charlie@example.com"
    Files.write(Paths.get(csvTestFile), csvContent.getBytes)
    
    // Create test JSON file
    val jsonContent = """{"name": "Alice", "email": "alice@example.com"}
                        |{"name": "Bob", "email": "bob@example.com"}
                        |{"name": "Charlie", "email": "charlie@example.com"}""".stripMargin
    Files.write(Paths.get(jsonTestFile), jsonContent.getBytes)
  }

  override def afterEach(): Unit = {
    // Clean up test files
    new File(testDataPath).listFiles().foreach(_.delete())
    new File(testDataPath).delete()
    super.afterEach()
  }

  test("Can read CSV reference data successfully") {
    val step = Step(
      name = "csv_reference_step",
      options = Map(
        FORMAT -> CSV,
        PATH -> csvTestFile,
        "header" -> "true",
        ENABLE_REFERENCE_MODE -> "true"
      )
    )
    val connectionConfig = Map[String, String]()

    val df = DataSourceReader.readDataFromSource("csv_reference", step, connectionConfig)
    
    assert(df.schema.nonEmpty, "DataFrame should have a schema")
    assert(df.count() == 3, "Should read 3 records from CSV")
    
    val columns = df.columns
    assert(columns.contains("name"), "Should contain 'name' column")
    assert(columns.contains("email"), "Should contain 'email' column")
    
    val firstRow = df.collect().head
    assert(firstRow.getAs[String]("name") == "Alice")
    assert(firstRow.getAs[String]("email") == "alice@example.com")
  }

  test("Can read JSON reference data successfully") {
    val step = Step(
      name = "json_reference_step",
      options = Map(
        FORMAT -> JSON,
        PATH -> jsonTestFile,
        ENABLE_REFERENCE_MODE -> "true"
      )
    )
    val connectionConfig = Map[String, String]()

    val df = DataSourceReader.readDataFromSource("json_reference", step, connectionConfig)
    
    assert(df.schema.nonEmpty, "DataFrame should have a schema")
    assert(df.count() == 3, "Should read 3 records from JSON")
    
    val columns = df.columns
    assert(columns.contains("name"), "Should contain 'name' column")
    assert(columns.contains("email"), "Should contain 'email' column")
  }

  test("Validation should fail when both reference mode and data generation are enabled") {
    val step = Step(
      name = "invalid_step",
      options = Map(
        FORMAT -> CSV,
        PATH -> csvTestFile,
        ENABLE_REFERENCE_MODE -> "true",
        ENABLE_DATA_GENERATION -> "true"
      )
    )
    val connectionConfig = Map[String, String]()

    val exception = intercept[IllegalArgumentException] {
      DataSourceReader.validateReferenceMode(step, connectionConfig)
    }
    assert(exception.getMessage.contains("Cannot enable both reference mode and data generation"))
  }

  test("Validation should fail when format is missing for reference mode") {
    val step = Step(
      name = "no_format_step",
      options = Map(
        ENABLE_REFERENCE_MODE -> "true",
        ENABLE_DATA_GENERATION -> "false"  // Explicitly disable data generation
        // FORMAT missing
      )
    )
    val connectionConfig = Map[String, String]()

    val exception = intercept[IllegalArgumentException] {
      DataSourceReader.validateReferenceMode(step, connectionConfig)
    }
    assert(exception.getMessage.contains("Format must be specified"))
  }

  test("Validation should fail when path is missing for file-based reference mode") {
    val step = Step(
      name = "no_path_step",
      options = Map(
        FORMAT -> CSV,
        ENABLE_REFERENCE_MODE -> "true",
        ENABLE_DATA_GENERATION -> "false"  // Explicitly disable data generation
        // PATH missing
      )
    )
    val connectionConfig = Map[String, String]()

    val exception = intercept[IllegalArgumentException] {
      DataSourceReader.validateReferenceMode(step, connectionConfig)
    }
    assert(exception.getMessage.contains("Path must be specified"))
  }

  test("Should throw exception for unsupported format in reference mode") {
    val step = Step(
      name = "unsupported_format_step",
      options = Map(
        FORMAT -> "unsupported_format",
        PATH -> csvTestFile,
        ENABLE_REFERENCE_MODE -> "true"
      )
    )
    val connectionConfig = Map[String, String]()

    val exception = intercept[RuntimeException] {
      DataSourceReader.readDataFromSource("unsupported_reference", step, connectionConfig)
    }
    assert(exception.getMessage.contains("Reference mode not supported for format: unsupported_format"))
  }

  test("Should throw exception when format is missing") {
    val step = Step(
      name = "no_format_step",
      options = Map(
        PATH -> csvTestFile,
        ENABLE_REFERENCE_MODE -> "true"
      )
    )
    val connectionConfig = Map[String, String]()

    val exception = intercept[IllegalArgumentException] {
      DataSourceReader.readDataFromSource("no_format_reference", step, connectionConfig)
    }
    assert(exception.getMessage.contains("No format specified for reference data source"))
  }

  test("Should handle JDBC validation correctly") {
    val step = Step(
      name = "jdbc_step",
      options = Map(
        FORMAT -> JDBC,
        URL -> "jdbc:h2:mem:testdb",
        DRIVER -> "org.h2.Driver",
        ENABLE_REFERENCE_MODE -> "true",
        ENABLE_DATA_GENERATION -> "false"  // Explicitly disable data generation
        // Missing both JDBC_TABLE and JDBC_QUERY
      )
    )
    val connectionConfig = Map[String, String]()

    val exception = intercept[IllegalArgumentException] {
      DataSourceReader.validateReferenceMode(step, connectionConfig)
    }
    assert(exception.getMessage.contains("Either 'dbtable' or 'query' must be specified"))
  }

  test("Should handle Cassandra validation correctly") {
    val step = Step(
      name = "cassandra_step",
      options = Map(
        FORMAT -> CASSANDRA,
        ENABLE_REFERENCE_MODE -> "true",
        ENABLE_DATA_GENERATION -> "false"  // Explicitly disable data generation
        // Missing CASSANDRA_KEYSPACE and CASSANDRA_TABLE
      )
    )
    val connectionConfig = Map[String, String]()

    val exception = intercept[IllegalArgumentException] {
      DataSourceReader.validateReferenceMode(step, connectionConfig)
    }
    assert(exception.getMessage.contains("Missing required Cassandra options"))
  }

  test("Should pass validation when reference mode is disabled") {
    val step = Step(
      name = "disabled_reference_step",
      options = Map(
        FORMAT -> CSV,
        ENABLE_REFERENCE_MODE -> "false"
        // PATH missing, but reference mode is disabled
      )
    )
    val connectionConfig = Map[String, String]()

    // Should not throw exception
    DataSourceReader.validateReferenceMode(step, connectionConfig)
  }

  test("Should pass validation when reference mode is not specified (defaults to false)") {
    val step = Step(
      name = "default_reference_step",
      options = Map(
        FORMAT -> CSV
        // ENABLE_REFERENCE_MODE not specified, PATH missing
      )
    )
    val connectionConfig = Map[String, String]()

    // Should not throw exception
    DataSourceReader.validateReferenceMode(step, connectionConfig)
  }

  test("Should handle connection config override for options") {
    val step = Step(
      name = "connection_override_step",
      options = Map(
        FORMAT -> CSV,
        ENABLE_REFERENCE_MODE -> "true",
        "header" -> "true",
        ENABLE_DATA_GENERATION -> "false"  // Explicitly disable data generation
        // PATH not in step options
      )
    )
    val connectionConfig = Map(
      PATH -> csvTestFile  // PATH provided in connection config
    )

    // Should not throw exception since PATH is available in connection config
    DataSourceReader.validateReferenceMode(step, connectionConfig)
    
    // Should be able to read data successfully
    val df = DataSourceReader.readDataFromSource("connection_override", step, connectionConfig)
    assert(df.count() == 3, "Should read 3 records from CSV using connection config")
  }

  test("Should log warning when no records are found") {
    // Create empty CSV file
    val emptyTestFile = s"$testDataPath/empty.csv"
    Files.write(Paths.get(emptyTestFile), "name,email".getBytes)  // Only header
    
    val step = Step(
      name = "empty_reference_step",
      options = Map(
        FORMAT -> CSV,
        PATH -> emptyTestFile,
        "header" -> "true",
        ENABLE_REFERENCE_MODE -> "true"
      )
    )
    val connectionConfig = Map[String, String]()

    val df = DataSourceReader.readDataFromSource("empty_reference", step, connectionConfig)
    
    assert(df.schema.nonEmpty, "DataFrame should have a schema")
    assert(df.count() == 0, "Should read 0 records from empty CSV")
  }
} 