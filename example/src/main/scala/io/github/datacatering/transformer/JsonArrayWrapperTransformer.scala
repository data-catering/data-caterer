package io.github.datacatering.transformer

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/**
 * Example whole-file transformer that wraps JSON lines into a JSON array.
 * 
 * This demonstrates whole-file transformation where the entire output file
 * is read and transformed as a unit. Useful for converting between formats
 * or restructuring entire files.
 * 
 * Converts from JSON lines format:
 * {{{
 * {"id": 1, "name": "Alice"}
 * {"id": 2, "name": "Bob"}
 * }}}
 * 
 * To JSON array format:
 * {{{
 * [
 *   {"id": 1, "name": "Alice"},
 *   {"id": 2, "name": "Bob"}
 * ]
 * }}}
 * 
 * Usage in Data Caterer:
 * {{{
 * val task = json("accounts", "/tmp/accounts")
 *   .fields(
 *     field.name("id"),
 *     field.name("name")
 *   )
 *   .transformationWholeFile(
 *     "io.github.datacatering.transformer.JsonArrayWrapperTransformer",
 *     "transformFile"
 *   )
 * }}}
 */
class JsonArrayWrapperTransformer {
  
  /**
   * Transform the entire file by wrapping JSON lines in a JSON array.
   * 
   * @param inputPath Path to the input file (JSON lines)
   * @param outputPath Path where the transformed file should be written
   * @return The path to the transformed file
   */
  def transformFile(inputPath: String, outputPath: String): String = {
    // Read all lines from input
    val lines = Files.readAllLines(Paths.get(inputPath)).asScala.toList
    
    // Filter out empty lines
    val nonEmptyLines = lines.filter(_.trim.nonEmpty)
    
    // Wrap in JSON array format
    val jsonArray = if (nonEmptyLines.isEmpty) {
      "[]"
    } else {
      val joinedLines = nonEmptyLines.mkString(",\n  ")
      s"[\n  $joinedLines\n]"
    }
    
    // Write to output file
    Files.write(Paths.get(outputPath), jsonArray.getBytes("UTF-8"))
    
    outputPath
  }
  
  /**
   * Transform the entire file with options support.
   * 
   * @param inputPath Path to the input file
   * @param outputPath Path where the transformed file should be written
   * @param format The original format (e.g., "json", "csv")
   * @param options Configuration options (e.g., "indent" -> "2", "minify" -> "true")
   * @return The path to the transformed file
   */
  def transformFileWithOptions(
    inputPath: String,
    outputPath: String,
    format: String,
    options: Map[String, String]
  ): String = {
    val lines = Files.readAllLines(Paths.get(inputPath)).asScala.toList
    val nonEmptyLines = lines.filter(_.trim.nonEmpty)
    
    val minify = options.getOrElse("minify", "false").toBoolean
    val indent = options.getOrElse("indent", "2").toInt
    
    val jsonArray = if (nonEmptyLines.isEmpty) {
      "[]"
    } else if (minify) {
      // Minified format - no whitespace
      s"[${nonEmptyLines.mkString(",")}]"
    } else {
      // Pretty format with custom indentation
      val indentStr = " " * indent
      val joinedLines = nonEmptyLines.mkString(s",\n$indentStr")
      s"[\n$indentStr$joinedLines\n]"
    }
    
    Files.write(Paths.get(outputPath), jsonArray.getBytes("UTF-8"))
    outputPath
  }
}

