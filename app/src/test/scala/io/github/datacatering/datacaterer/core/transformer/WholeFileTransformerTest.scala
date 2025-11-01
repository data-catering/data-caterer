package io.github.datacatering.datacaterer.core.transformer

import io.github.datacatering.datacaterer.api.model.TransformationConfig
import io.github.datacatering.datacaterer.core.util.SparkSuite

import java.nio.file.{Files, Paths}
import scala.io.Source

class WholeFileTransformerTest extends SparkSuite {

  test("Can transform whole file with simple transformer") {
    val inputPath = Files.createTempFile("test-input", ".json").toString
    Files.write(Paths.get(inputPath), "{\"id\":1}\n{\"id\":2}".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestWholeFileTransformer",
      methodName = "transformFile",
      mode = "whole-file"
    )

    val outputPath = WholeFileTransformer.transform(config, inputPath, "json")

    val result = Source.fromFile(outputPath, "UTF-8").mkString
    assertResult("[{\"id\":1},{\"id\":2}]")(result)

    Files.delete(Paths.get(inputPath))
    Files.delete(Paths.get(outputPath))
  }

  test("Can transform whole file with options") {
    val inputPath = Files.createTempFile("test-input", ".json").toString
    Files.write(Paths.get(inputPath), "{\"id\":1}\n{\"id\":2}".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestWholeFileOptionsTransformer",
      methodName = "transformFile",
      mode = "whole-file",
      options = Map("wrapper" -> "data", "format" -> "json")
    )

    val outputPath = WholeFileTransformer.transform(config, inputPath, "json")

    val result = Source.fromFile(outputPath, "UTF-8").mkString
    assertResult("{\"data\":[{\"id\":1},{\"id\":2}]}")(result)

    Files.delete(Paths.get(inputPath))
    Files.delete(Paths.get(outputPath))
  }

  test("Can specify custom output path") {
    val inputPath = Files.createTempFile("test-input", ".json").toString
    Files.write(Paths.get(inputPath), "{\"test\":true}".getBytes("UTF-8"))

    val customOutputPath = Files.createTempDirectory("test-output").resolve("custom.json").toString

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestWholeFileTransformer",
      methodName = "transformFile",
      mode = "whole-file",
      outputPath = Some(customOutputPath)
    )

    val resultPath = WholeFileTransformer.transform(config, inputPath, "json")

    assertResult(customOutputPath)(resultPath)
    assert(Files.exists(Paths.get(customOutputPath)))

    Files.delete(Paths.get(inputPath))
    Files.delete(Paths.get(customOutputPath))
  }

  test("Can delete original file after transformation") {
    val inputPath = Files.createTempFile("test-input", ".json").toString
    Files.write(Paths.get(inputPath), "{\"test\":true}".getBytes("UTF-8"))

    val customOutputPath = Files.createTempDirectory("test-output").resolve("custom.json").toString

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestWholeFileTransformer",
      methodName = "transformFile",
      mode = "whole-file",
      outputPath = Some(customOutputPath),
      deleteOriginal = true
    )

    val resultPath = WholeFileTransformer.transform(config, inputPath, "json")

    assert(!Files.exists(Paths.get(inputPath)), "Original file should be deleted")
    assert(Files.exists(Paths.get(resultPath)), "Output file should exist")
    assertResult(customOutputPath)(resultPath)

    Files.delete(Paths.get(resultPath))
  }

  test("Should handle empty file") {
    val inputPath = Files.createTempFile("test-input", ".json").toString
    Files.write(Paths.get(inputPath), "".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestWholeFileTransformer",
      methodName = "transformFile",
      mode = "whole-file"
    )

    val outputPath = WholeFileTransformer.transform(config, inputPath, "json")

    val result = Source.fromFile(outputPath, "UTF-8").mkString
    assertResult("[]")(result)

    Files.delete(Paths.get(inputPath))
    Files.delete(Paths.get(outputPath))
  }

  test("Should handle transformer class not found") {
    val inputPath = Files.createTempFile("test-input", ".json").toString
    Files.write(Paths.get(inputPath), "{\"test\":true}".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "com.nonexistent.NonexistentTransformer",
      methodName = "transformFile",
      mode = "whole-file"
    )

    val thrown = intercept[TransformationException] {
      WholeFileTransformer.transform(config, inputPath, "json")
    }

    assert(thrown.getCause.isInstanceOf[ClassNotFoundException])
    assert(thrown.getMessage.contains("NonexistentTransformer"))
    Files.delete(Paths.get(inputPath))
  }

  test("Should handle method not found") {
    val inputPath = Files.createTempFile("test-input", ".json").toString
    Files.write(Paths.get(inputPath), "{\"test\":true}".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestWholeFileTransformer",
      methodName = "nonexistentMethod",
      mode = "whole-file"
    )

    val thrown = intercept[TransformationException] {
      WholeFileTransformer.transform(config, inputPath, "json")
    }

    assert(thrown.getCause.isInstanceOf[NoSuchMethodException])
    assert(thrown.getMessage.contains("nonexistentMethod"))
    Files.delete(Paths.get(inputPath))
  }

  test("Should support two-parameter method signature") {
    val inputPath = Files.createTempFile("test-input", ".json").toString
    Files.write(Paths.get(inputPath), "{\"id\":1}".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestWholeFileSimpleTransformer",
      methodName = "transformFile",
      mode = "whole-file"
    )

    val outputPath = WholeFileTransformer.transform(config, inputPath, "json")

    val result = Source.fromFile(outputPath, "UTF-8").mkString
    assertResult("[{\"id\":1}]")(result)

    Files.delete(Paths.get(inputPath))
    Files.delete(Paths.get(outputPath))
  }

  test("Should handle directory input by consolidating part files") {
    // Create a temporary directory with multiple part files (simulating Spark output)
    val tempDir = Files.createTempDirectory("test-spark-output")
    val partFile1 = tempDir.resolve("part-00000.json")
    val partFile2 = tempDir.resolve("part-00001.json")

    // Write JSON lines to part files
    Files.write(partFile1, "{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}".getBytes("UTF-8"))
    Files.write(partFile2, "{\"id\":3,\"name\":\"Charlie\"}".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestWholeFileTransformer",
      methodName = "transformFile",
      mode = "whole-file"
    )

    val outputPath = WholeFileTransformer.transform(config, tempDir.toString, "json")

    // Verify the output contains all consolidated records in JSON array format
    val result = Source.fromFile(outputPath, "UTF-8").mkString
    assertResult("[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"},{\"id\":3,\"name\":\"Charlie\"}]")(result)

    // Verify original directory is cleaned up (since deleteOriginal defaults to false in unit tests)
    assert(Files.exists(tempDir), "Original directory should still exist")
    assert(Files.exists(partFile1), "Part files should still exist")
    assert(Files.exists(partFile2), "Part files should still exist")

    // Clean up
    Files.delete(partFile1)
    Files.delete(partFile2)
    Files.delete(tempDir)
    Files.delete(Paths.get(outputPath))
  }

  test("Should handle directory input and delete original when configured") {
    // Create a temporary directory with part files
    val tempDir = Files.createTempDirectory("test-spark-output")
    val partFile1 = tempDir.resolve("part-00000.json")

    // Write JSON lines to part file
    Files.write(partFile1, "{\"id\":1,\"name\":\"Test\"}".getBytes("UTF-8"))

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestWholeFileTransformer",
      methodName = "transformFile",
      mode = "whole-file",
      deleteOriginal = true
    )

    val outputPath = WholeFileTransformer.transform(config, tempDir.toString, "json")

    // Verify the output is correct
    val result = Source.fromFile(outputPath, "UTF-8").mkString
    assertResult("[{\"id\":1,\"name\":\"Test\"}]")(result)

    // Verify original directory and files are deleted
    assert(!Files.exists(tempDir), "Original directory should be deleted")
    assert(!Files.exists(partFile1), "Part files should be deleted")

    // Clean up output file
    Files.delete(Paths.get(outputPath))
  }

  test("Should handle empty directory input") {
    // Create an empty temporary directory
    val tempDir = Files.createTempDirectory("test-empty-spark-output")

    val config = TransformationConfig(
      className = "io.github.datacatering.datacaterer.core.transformer.TestWholeFileTransformer",
      methodName = "transformFile",
      mode = "whole-file"
    )

    val outputPath = WholeFileTransformer.transform(config, tempDir.toString, "json")

    // Verify the output is an empty array
    val result = Source.fromFile(outputPath, "UTF-8").mkString
    assertResult("[]")(result)

    // Clean up
    Files.delete(tempDir)
    Files.delete(Paths.get(outputPath))
  }
}

/**
 * Test transformer that wraps JSON lines in an array (4-parameter signature)
 */
class TestWholeFileTransformer {
  def transformFile(inputPath: String, outputPath: String, format: String, options: Map[String, String]): String = {
    val content = Source.fromFile(inputPath, "UTF-8").mkString.trim
    val lines = if (content.isEmpty) Array.empty[String] else content.split("\n")
    val result = if (lines.isEmpty) "[]" else s"[${lines.mkString(",")}]"
    Files.write(Paths.get(outputPath), result.getBytes("UTF-8"))
    outputPath
  }
}

/**
 * Test transformer with 2-parameter signature
 */
class TestWholeFileSimpleTransformer {
  def transformFile(inputPath: String, outputPath: String): String = {
    val content = Source.fromFile(inputPath, "UTF-8").mkString.trim
    val lines = if (content.isEmpty) Array.empty[String] else content.split("\n")
    val result = if (lines.isEmpty) "[]" else s"[${lines.mkString(",")}]"
    Files.write(Paths.get(outputPath), result.getBytes("UTF-8"))
    outputPath
  }
}

/**
 * Test transformer that wraps content in a JSON object with options
 */
class TestWholeFileOptionsTransformer {
  def transformFile(inputPath: String, outputPath: String, format: String, options: Map[String, String]): String = {
    val content = Source.fromFile(inputPath, "UTF-8").mkString.trim
    val lines = if (content.isEmpty) Array.empty[String] else content.split("\n")
    val wrapper = options.getOrElse("wrapper", "data")
    val result = if (lines.isEmpty) {
      s"""{"$wrapper":[]}"""
    } else {
      s"""{"$wrapper":[${lines.mkString(",")}]}"""
    }
    Files.write(Paths.get(outputPath), result.getBytes("UTF-8"))
    outputPath
  }
}

