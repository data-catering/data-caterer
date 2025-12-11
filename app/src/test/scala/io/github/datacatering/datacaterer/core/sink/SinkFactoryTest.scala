package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Constants.{CSV, DELTA, FORMAT, ICEBERG, JSON, PARQUET, PATH, SAVE_MODE, TABLE}
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, FoldersConfig, MetadataConfig, Step}
import io.github.datacatering.datacaterer.core.util.{SparkSuite, Transaction}

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Date
import java.time.LocalDateTime
import scala.reflect.io.Directory

class SinkFactoryTest extends SparkSuite {

  // Helper to create temp file path with given suffix
  private def createTempFilePath(prefix: String, suffix: String): String = {
    val tempDir = Files.createTempDirectory(prefix)
    s"${tempDir.toString}/$prefix$suffix"
  }

  // Helper to create temp directory path
  private def createTempDirPath(prefix: String): String = {
    Files.createTempDirectory(prefix).toString
  }

  private val sampleData = Seq(
    Transaction("acc123", "peter", "txn1", Date.valueOf("2020-01-01"), 10.0),
    Transaction("acc123", "peter", "txn2", Date.valueOf("2020-01-01"), 50.0),
    Transaction("acc123", "peter", "txn3", Date.valueOf("2020-01-01"), 200.0),
    Transaction("acc123", "peter", "txn4", Date.valueOf("2020-01-01"), 500.0)
  )
  private val df = sparkSession.createDataFrame(sampleData)

  test("Can save data in Iceberg format") {
    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> ICEBERG, TABLE -> "local.account.transactions"))
    val res = sinkFactory.pushToSink(df, "iceberg-data-source", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(4)(res.count)
    assertResult(ICEBERG)(res.format)
    assert(res.exception.isEmpty)
  }

  test("Can save data in Delta Lake format") {
    val path = createTempDirPath("delta-test")
    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> DELTA, PATH -> path))
    val res = sinkFactory.pushToSink(df, "delta-data-source", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(4)(res.count)
    assertResult(DELTA)(res.format)
    assert(res.exception.isEmpty)
  }

  test("Should provide helpful error message when format is missing from step options") {
    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val testPath = createTempDirPath("test-path")
    val stepWithoutFormat = Step(options = Map(PATH -> testPath, SAVE_MODE -> "overwrite"))
    
    val exception = intercept[IllegalArgumentException] {
      sinkFactory.pushToSink(df, "test-data-source", stepWithoutFormat, LocalDateTime.now())
    }
    
    assert(exception.getMessage.contains("No format specified for data source: test-data-source"))
    assert(exception.getMessage.contains("step: "))
    assert(exception.getMessage.contains("Available options: path, saveMode"))
  }

  ignore("Can overwrite existing Iceberg data") {
    sparkSession.sql("DELETE FROM local.account.transactions_overwrite").count()
    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val options = Map(FORMAT -> ICEBERG, TABLE -> "local.account.transactions_overwrite")
    val step = Step(options = options)
    val existingDataRes = sinkFactory.pushToSink(df, "iceberg-data-source", step, LocalDateTime.now())

    assert(existingDataRes.isSuccess)
    assertResult(4)(existingDataRes.count)
    assertResult(ICEBERG)(existingDataRes.format)
    assert(existingDataRes.exception.isEmpty)
    assertResult(4)(sparkSession.table("local.account.transactions_overwrite").count())

    val newStep = Step(options = options ++ Map(SAVE_MODE -> "overwrite"))
    val res = sinkFactory.pushToSink(df, "iceberg-data-source", newStep, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(4)(res.count)
    assertResult(ICEBERG)(res.format)
    assert(res.exception.isEmpty)
    assertResult(4)(sparkSession.table("local.account.transactions_overwrite").count())
  }

  test("Should consolidate part files into single JSON file when path has .json suffix") {
    val filePath = createTempFilePath("output_test", ".json")
    val path = Paths.get(filePath)

    // Clean up any existing file
    if (Files.exists(path)) {
      Files.delete(path)
    }

    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> JSON, PATH -> filePath))
    val res = sinkFactory.pushToSink(df, "json-single-file", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(4)(res.count)
    assertResult(JSON)(res.format)
    assert(res.exception.isEmpty)

    // Verify that a single file exists (not a directory)
    assert(Files.exists(path), s"File should exist at $filePath")
    assert(Files.isRegularFile(path), s"Path should be a regular file, not a directory: $filePath")

    // Verify no temporary directory remains
    val tempDirPattern = s"${filePath}_spark_temp_"
    val parentDir = path.getParent
    val tempDirs = Files.list(parentDir)
      .filter(p => p.getFileName.toString.startsWith(new File(filePath).getName + "_spark_temp_"))
      .toArray()
    assert(tempDirs.isEmpty, "Temporary Spark directories should be cleaned up")

    // Verify content can be read back
    val readBack = sparkSession.read.json(filePath)
    assertResult(4)(readBack.count())

    // Clean up
    Files.delete(path)
  }

  test("Should consolidate part files into single CSV file when path has .csv suffix") {
    val filePath = createTempFilePath("output_test", ".csv")
    val path = Paths.get(filePath)

    // Clean up any existing file
    if (Files.exists(path)) {
      Files.delete(path)
    }

    // Create simple data without complex types for CSV compatibility
    val simpleCsvData = Seq(
      ("acc123", "peter", "txn1", 10.0),
      ("acc123", "peter", "txn2", 50.0),
      ("acc123", "peter", "txn3", 200.0),
      ("acc123", "peter", "txn4", 500.0)
    )
    val csvDf = sparkSession.createDataFrame(simpleCsvData).toDF("account", "name", "transaction", "amount")

    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> CSV, PATH -> filePath, "header" -> "true"))
    val res = sinkFactory.pushToSink(csvDf, "csv-single-file", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(4)(res.count)
    assertResult(CSV)(res.format)
    assert(res.exception.isEmpty)

    // Verify that a single file exists (not a directory)
    assert(Files.exists(path), s"File should exist at $filePath")
    assert(Files.isRegularFile(path), s"Path should be a regular file, not a directory: $filePath")

    // Verify no temporary directory remains
    val tempDirPattern = s"${filePath}_spark_temp_"
    val parentDir = path.getParent
    val tempDirs = Files.list(parentDir)
      .filter(p => p.getFileName.toString.startsWith(new File(filePath).getName + "_spark_temp_"))
      .toArray()
    assert(tempDirs.isEmpty, "Temporary Spark directories should be cleaned up")

    // Verify content can be read back
    val readBack = sparkSession.read.option("header", "true").csv(filePath)
    assertResult(4)(readBack.count())

    // Clean up
    Files.delete(path)
  }

  test("Should consolidate part files from multiple batches into single JSON file") {
    val filePath = createTempFilePath("multibatch_output_test", ".json")
    val path = Paths.get(filePath)

    // Clean up any existing file
    if (Files.exists(path)) {
      Files.delete(path)
    }

    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> JSON, PATH -> filePath))
    
    // Simulate multi-batch scenario: first batch
    val batch1Data = sampleData.take(2)
    val df1 = sparkSession.createDataFrame(batch1Data)
    val res1 = sinkFactory.pushToSink(df1, "json-multi-batch", step, LocalDateTime.now(), isMultiBatch = true, isLastBatch = false)
    
    // Verify first batch success but no final file created yet
    assert(res1.isSuccess)
    assertResult(2)(res1.count)
    assert(!Files.exists(path), "Final file should not exist after first batch")
    
    // Second batch (last batch)
    val batch2Data = sampleData.drop(2)
    val df2 = sparkSession.createDataFrame(batch2Data)
    val res2 = sinkFactory.pushToSink(df2, "json-multi-batch", step, LocalDateTime.now(), isMultiBatch = true, isLastBatch = true)
    
    // Verify second batch success and final file exists
    assert(res2.isSuccess)
    assertResult(2)(res2.count)
    assert(Files.exists(path), s"Final consolidated file should exist at $filePath")
    assert(Files.isRegularFile(path), s"Path should be a regular file: $filePath")

    // Verify no temporary directories remain
    val parentDir = path.getParent
    val tempDirs = Files.list(parentDir)
      .filter(p => p.getFileName.toString.contains("multibatch_temp_"))
      .toArray()
    assert(tempDirs.isEmpty, "Temporary multi-batch directories should be cleaned up")

    // Verify all data is present in the consolidated file
    val readBack = sparkSession.read.json(filePath)
    assertResult(4)(readBack.count())

    // Clean up
    Files.delete(path)
  }

  test("Should consolidate part files into single Parquet file when path has .parquet suffix") {
    val filePath = createTempFilePath("output_test", ".parquet")
    val path = Paths.get(filePath)

    // Clean up any existing file
    if (Files.exists(path)) {
      Files.delete(path)
    }

    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> PARQUET, PATH -> filePath))
    val res = sinkFactory.pushToSink(df, "parquet-single-file", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(4)(res.count)
    assertResult(PARQUET)(res.format)
    assert(res.exception.isEmpty)

    // Verify that a single file exists (not a directory)
    assert(Files.exists(path), s"File should exist at $filePath")
    assert(Files.isRegularFile(path), s"Path should be a regular file, not a directory: $filePath")

    // Verify no temporary directory remains
    val parentDir = path.getParent
    val tempDirs = Files.list(parentDir)
      .filter(p => p.getFileName.toString.startsWith(new File(filePath).getName + "_spark_temp_"))
      .toArray()
    assert(tempDirs.isEmpty, "Temporary Spark directories should be cleaned up")

    // Verify content can be read back
    val readBack = sparkSession.read.parquet(filePath)
    assertResult(4)(readBack.count())

    // Clean up
    Files.delete(path)
  }

  test("Should handle finalizePendingConsolidations when batches are incomplete") {
    val filePath = createTempFilePath("incomplete_batch_test", ".json")
    val path = Paths.get(filePath)

    // Clean up any existing file
    if (Files.exists(path)) {
      Files.delete(path)
    }

    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> JSON, PATH -> filePath))
    
    // Simulate incomplete multi-batch scenario: only first batch, no last batch call
    val batch1Data = sampleData.take(2)
    val df1 = sparkSession.createDataFrame(batch1Data)
    val res1 = sinkFactory.pushToSink(df1, "json-incomplete-batch", step, LocalDateTime.now(), isMultiBatch = true, isLastBatch = false)
    
    // Verify first batch success but no final file created yet
    assert(res1.isSuccess)
    assertResult(2)(res1.count)
    assert(!Files.exists(path), "Final file should not exist after incomplete batch")
    
    // Call finalizePendingConsolidations to handle incomplete batches
    sinkFactory.finalizePendingConsolidations()
    
    // Verify final file exists after finalization
    assert(Files.exists(path), s"Final consolidated file should exist after finalization at $filePath")
    assert(Files.isRegularFile(path), s"Path should be a regular file: $filePath")

    // Verify all data from the one batch is present
    val readBack = sparkSession.read.json(filePath)
    assertResult(2)(readBack.count())

    // Clean up
    Files.delete(path)
  }

  test("Should NOT consolidate when path has no file suffix (directory mode)") {
    val dirPath = createTempDirPath("output_test_directory")
    val directory = new Directory(new File(dirPath))

    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> JSON, PATH -> dirPath))
    val res = sinkFactory.pushToSink(df, "json-directory", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(4)(res.count)
    assertResult(JSON)(res.format)
    assert(res.exception.isEmpty)

    // Verify that a directory exists with part files (Spark's default behavior)
    val path = Paths.get(dirPath)
    assert(Files.exists(path), s"Directory should exist at $dirPath")
    assert(Files.isDirectory(path), s"Path should be a directory: $dirPath")

    // Verify part files exist in the directory
    val partFiles = Files.list(path)
      .filter(p => p.getFileName.toString.startsWith("part-"))
      .toArray()
    assert(partFiles.nonEmpty, "Part files should exist in the directory")

    // Clean up
    directory.deleteRecursively()
  }

  test("Should consolidate CSV with headers and only include header once") {
    val filePath = createTempFilePath("output_test_with_headers", ".csv")
    val path = Paths.get(filePath)

    // Clean up any existing file
    if (Files.exists(path)) {
      Files.delete(path)
    }

    // Create larger dataset to force multiple partitions
    val largeCsvData = (1 to 20).map(i =>
      (s"acc$i", s"user$i", s"txn$i", i * 10.0)
    )
    val largeCsvDf = sparkSession.createDataFrame(largeCsvData).toDF("account", "name", "transaction", "amount")

    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    // Force multiple partitions with repartition
    val step = Step(options = Map(
      FORMAT -> CSV,
      PATH -> filePath,
      "header" -> "true",
      "partitions" -> "3"  // Force 3 partitions to test header consolidation
    ))
    val res = sinkFactory.pushToSink(largeCsvDf, "csv-with-headers", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(20)(res.count)
    assertResult(CSV)(res.format)
    assert(res.exception.isEmpty)

    // Verify that a single file exists
    assert(Files.exists(path), s"File should exist at $filePath")
    assert(Files.isRegularFile(path), s"Path should be a regular file, not a directory: $filePath")

    // Read the file and verify header appears only once
    val lines = Files.readAllLines(path)
    assert(lines.size() > 0, "File should not be empty")

    // Count how many times the header appears
    val headerLine = "account,name,transaction,amount"
    val headerCount = lines.toArray().count(line => line.toString == headerLine)
    assertResult(1, s"Header should appear exactly once, but found $headerCount times")(headerCount)

    // Verify we have header + 20 data rows
    assertResult(21, "Should have 1 header row + 20 data rows")(lines.size())

    // Verify content can be read back correctly
    val readBack = sparkSession.read.option("header", "true").csv(filePath)
    assertResult(20)(readBack.count())

    // Verify all original data is present
    val readBackData = readBack.collect().sortBy(_.getString(0))
    assertResult(20)(readBackData.length)

    // Clean up
    Files.delete(path)
  }

  test("Should handle CSV without headers when consolidating multiple part files") {
    val filePath = createTempFilePath("output_test_no_headers", ".csv")
    val path = Paths.get(filePath)

    // Clean up any existing file
    if (Files.exists(path)) {
      Files.delete(path)
    }

    // Create dataset with multiple partitions
    val csvData = (1 to 15).map(i =>
      (s"acc$i", s"user$i", s"txn$i", i * 10.0)
    )
    val csvDf = sparkSession.createDataFrame(csvData).toDF("account", "name", "transaction", "amount")

    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(
      FORMAT -> CSV,
      PATH -> filePath,
      "header" -> "false",
      "partitions" -> "3"  // Force 3 partitions
    ))
    val res = sinkFactory.pushToSink(csvDf, "csv-no-headers", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(15)(res.count)
    assertResult(CSV)(res.format)
    assert(res.exception.isEmpty)

    // Verify that a single file exists
    assert(Files.exists(path), s"File should exist at $filePath")
    assert(Files.isRegularFile(path), s"Path should be a regular file, not a directory: $filePath")

    // Verify we have exactly 15 data rows (no header)
    val lines = Files.readAllLines(path)
    assertResult(15, "Should have exactly 15 data rows (no header)")(lines.size())

    // Verify content can be read back correctly
    val readBack = sparkSession.read.option("header", "false").csv(filePath)
    assertResult(15)(readBack.count())

    // Clean up
    Files.delete(path)
  }

  test("Should handle CSV with headers when only single partition exists") {
    val filePath = createTempFilePath("output_test_single_partition", ".csv")
    val path = Paths.get(filePath)

    // Clean up any existing file
    if (Files.exists(path)) {
      Files.delete(path)
    }

    // Create small dataset that will fit in single partition
    val smallCsvData = Seq(
      ("acc1", "user1", "txn1", 10.0),
      ("acc2", "user2", "txn2", 20.0)
    )
    val smallCsvDf = sparkSession.createDataFrame(smallCsvData).toDF("account", "name", "transaction", "amount")

    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> CSV, PATH -> filePath, "header" -> "true"))
    val res = sinkFactory.pushToSink(smallCsvDf, "csv-single-partition", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(2)(res.count)
    assertResult(CSV)(res.format)
    assert(res.exception.isEmpty)

    // Verify that a single file exists
    assert(Files.exists(path), s"File should exist at $filePath")
    assert(Files.isRegularFile(path), s"Path should be a regular file, not a directory: $filePath")

    // Verify we have header + 2 data rows
    val lines = Files.readAllLines(path)
    assertResult(3, "Should have 1 header row + 2 data rows")(lines.size())

    // Verify header is present
    val headerLine = "account,name,transaction,amount"
    assertResult(headerLine)(lines.get(0))

    // Clean up
    Files.delete(path)
  }
}
