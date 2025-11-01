package io.github.datacatering.datacaterer.core.transformer

import io.github.datacatering.datacaterer.api.model.TransformationConfig
import io.github.datacatering.datacaterer.core.util.SparkSuite

import java.nio.file.{Files, Paths}
import scala.io.Source

class PerRecordTransformerTest extends SparkSuite {

  test("Can transform per-record with simple transformer") {
    // Create a test transformer class at runtime
    val inputPath = Files.createTempFile("test-input", ".csv").toString
    Files.write(Paths.get(inputPath), "line1\nline2\nline3".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestSimpleTransformer",
      methodName = "transform",
      mode = "per-record"
    )

    val outputPath = PerRecordTransformer.transform(config, inputPath, "csv")

    // Read and verify output
    val result = Source.fromFile(outputPath, "UTF-8").mkString
    assertResult("LINE1\nLINE2\nLINE3")(result)

    // Cleanup
    Files.delete(Paths.get(inputPath))
    Files.delete(Paths.get(outputPath))
  }

  test("Can transform per-record with options") {
    val inputPath = Files.createTempFile("test-input", ".csv").toString
    Files.write(Paths.get(inputPath), "hello\nworld".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestOptionsTransformer",
      methodName = "transform",
      mode = "per-record",
      options = Map("prefix" -> "PREFIX_", "suffix" -> "_SUFFIX")
    )

    val outputPath = PerRecordTransformer.transform(config, inputPath, "csv")

    val result = Source.fromFile(outputPath, "UTF-8").mkString
    assertResult("PREFIX_hello_SUFFIX\nPREFIX_world_SUFFIX")(result)

    Files.delete(Paths.get(inputPath))
    Files.delete(Paths.get(outputPath))
  }

  test("Can specify custom output path") {
    val inputPath = Files.createTempFile("test-input", ".csv").toString
    Files.write(Paths.get(inputPath), "test".getBytes("UTF-8"))

    val customOutputPath = Files.createTempDirectory("test-output").resolve("custom.csv").toString

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestSimpleTransformer",
      methodName = "transform",
      mode = "per-record",
      outputPath = Some(customOutputPath)
    )

    val resultPath = PerRecordTransformer.transform(config, inputPath, "csv")

    assertResult(customOutputPath)(resultPath)
    assert(Files.exists(Paths.get(customOutputPath)))

    Files.delete(Paths.get(inputPath))
    Files.delete(Paths.get(customOutputPath))
  }

  test("Can delete original file after transformation") {
    val inputPath = Files.createTempFile("test-input", ".csv").toString
    Files.write(Paths.get(inputPath), "test".getBytes("UTF-8"))

    val customOutputPath = Files.createTempDirectory("test-output").resolve("custom.csv").toString

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestSimpleTransformer",
      methodName = "transform",
      mode = "per-record",
      outputPath = Some(customOutputPath),
      deleteOriginal = true
    )

    val resultPath = PerRecordTransformer.transform(config, inputPath, "csv")

    assert(!Files.exists(Paths.get(inputPath)), "Original file should be deleted")
    assert(Files.exists(Paths.get(resultPath)), "Output file should exist")
    assertResult(customOutputPath)(resultPath)

    Files.delete(Paths.get(resultPath))
  }

  test("Should handle empty file") {
    val inputPath = Files.createTempFile("test-input", ".csv").toString
    Files.write(Paths.get(inputPath), "".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestSimpleTransformer",
      methodName = "transform",
      mode = "per-record"
    )

    val outputPath = PerRecordTransformer.transform(config, inputPath, "csv")

    val result = Source.fromFile(outputPath, "UTF-8").mkString
    assertResult("")(result)

    Files.delete(Paths.get(inputPath))
    Files.delete(Paths.get(outputPath))
  }

  test("Should handle transformer class not found") {
    val inputPath = Files.createTempFile("test-input", ".csv").toString
    Files.write(Paths.get(inputPath), "test".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "com.nonexistent.NonexistentTransformer",
      methodName = "transform",
      mode = "per-record"
    )

    val thrown = intercept[TransformationException] {
      PerRecordTransformer.transform(config, inputPath, "csv")
    }

    assert(thrown.getCause.isInstanceOf[ClassNotFoundException])
    assert(thrown.getMessage.contains("NonexistentTransformer"))
    Files.delete(Paths.get(inputPath))
  }

  test("Should handle method not found") {
    val inputPath = Files.createTempFile("test-input", ".csv").toString
    Files.write(Paths.get(inputPath), "test".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestSimpleTransformer",
      methodName = "nonexistentMethod",
      mode = "per-record"
    )

    val thrown = intercept[TransformationException] {
      PerRecordTransformer.transform(config, inputPath, "csv")
    }

    assert(thrown.getCause.isInstanceOf[NoSuchMethodException])
    assert(thrown.getMessage.contains("nonexistentMethod"))
    Files.delete(Paths.get(inputPath))
  }
}

/**
 * Test transformer that converts text to uppercase
 */
class TestSimpleTransformer {
  def transform(record: String): String = {
    record.toUpperCase
  }
}

/**
 * Test transformer that adds prefix and suffix from options
 */
class TestOptionsTransformer {
  def transform(record: String, options: Map[String, String]): String = {
    val prefix = options.getOrElse("prefix", "")
    val suffix = options.getOrElse("suffix", "")
    prefix + record + suffix
  }
}

