package io.github.datacatering.datacaterer.core.transformer

import io.github.datacatering.datacaterer.api.model.TransformationConfig
import org.apache.log4j.Logger

import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Applies per-record transformation to files.
 * Reads file line-by-line, transforms each record as a string, and writes to output.
 */
object PerRecordTransformer {
  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Transform file record-by-record.
   * Each line/record is read as a string, transformed via the user-provided method, and written to output.
   *
   * @param config      Transformation configuration
   * @param inputPath   Path to input file (written by Data Caterer)
   * @param format      Original format (csv, json, etc.)
   * @return Path to transformed file
   */
  def transform(config: TransformationConfig, inputPath: String, format: String): String = {
    val outputPath = config.outputPath.getOrElse(s"${inputPath}.transformed")

    LOGGER.info(s"Applying per-record transformation: class=${config.className}, method=${config.methodName}, " +
      s"input=$inputPath, output=$outputPath")

    Try {
      // Load transformer class
      val clazz = Class.forName(config.className)
      val instance = clazz.getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]
      val method = findMethod(clazz, config.methodName)

      val inputFile = new java.io.File(inputPath)

      if (inputFile.isDirectory) {
        // Handle directory input (e.g., Spark output directory with part files)
        LOGGER.debug(s"Input path is a directory: $inputPath")
        transformDirectory(config, inputPath, outputPath, instance, method)
      } else {
        // Handle single file input
        LOGGER.debug(s"Input path is a file: $inputPath")
        transformSingleFile(config, inputPath, outputPath, instance, method)
      }

    } match {
      case Success(path) => path.asInstanceOf[String]
      case Failure(exception) =>
        LOGGER.error(s"Per-record transformation failed for file: $inputPath", exception)
        throw new TransformationException(s"Per-record transformation failed: ${exception.getMessage}", exception)
    }
  }

  private def transformSingleFile(
                                   config: TransformationConfig,
                                   inputPath: String,
                                   outputPath: String,
                                   instance: AnyRef,
                                   method: java.lang.reflect.Method
                                 ): String = {
    // Read file line by line, transform each record
    val source = Source.fromFile(inputPath, "UTF-8")
    val transformedLines = try {
      source.getLines().map { line =>
        val transformedLine = if (method.getParameterCount == 2) {
          method.invoke(instance, line, config.options).asInstanceOf[String]
        } else {
          method.invoke(instance, line).asInstanceOf[String]
        }
        transformedLine
      }.toList
    } finally {
      source.close()
    }

    LOGGER.debug(s"Transformed ${transformedLines.size} records from single file")

    // Write transformed records to output
    Files.write(
      Paths.get(outputPath),
      transformedLines.mkString("\n").getBytes("UTF-8")
    )

    // Delete original if configured and output is different from input
    if (config.deleteOriginal && outputPath != inputPath) {
      Try(Files.delete(Paths.get(inputPath))) match {
        case Success(_) =>
          LOGGER.info(s"Deleted original file: $inputPath")
        case Failure(ex) =>
          LOGGER.warn(s"Failed to delete original file: $inputPath", ex)
      }
    }

    LOGGER.info(s"Per-record transformation complete: ${transformedLines.size} records transformed from single file")
    outputPath
  }

  private def transformDirectory(
                                  config: TransformationConfig,
                                  inputPath: String,
                                  outputPath: String,
                                  instance: AnyRef,
                                  method: java.lang.reflect.Method
                                ): String = {
    val inputDir = new java.io.File(inputPath)

    // Find all files in the directory (typically Spark part files)
    val filesToTransform = inputDir.listFiles()
      .filter(_.isFile)
      .filterNot(_.getName.startsWith(".")) // Skip hidden files like .crc files
      .filterNot(_.getName.endsWith(".crc")) // Skip Spark CRC files
      .sortBy(_.getName) // Sort for consistent processing

    if (filesToTransform.isEmpty) {
      LOGGER.warn(s"No files found in directory: $inputPath")
      return outputPath
    }

    LOGGER.debug(s"Found ${filesToTransform.length} files to transform in directory: $inputPath")

    // Transform each file and collect all results
    val allTransformedLines = filesToTransform.flatMap { file =>
      LOGGER.debug(s"Transforming file: ${file.getAbsolutePath}")
      transformSingleFileContent(config, file.getAbsolutePath, instance, method)
    }

    LOGGER.debug(s"Transformed ${allTransformedLines.size} total records from ${filesToTransform.length} files")

    // Write all transformed records to output
    Files.write(
      Paths.get(outputPath),
      allTransformedLines.mkString("\n").getBytes("UTF-8")
    )

    // Delete original directory if configured and output is different from input
    if (config.deleteOriginal && outputPath != inputPath) {
      Try {
        // Delete all files in directory first
        filesToTransform.foreach(f => Files.delete(f.toPath))
        // Then delete the directory
        Files.delete(Paths.get(inputPath))
        LOGGER.info(s"Deleted original directory: $inputPath")
      } match {
        case Success(_) =>
          LOGGER.info(s"Deleted original directory: $inputPath")
        case Failure(ex) =>
          LOGGER.warn(s"Failed to delete original directory: $inputPath", ex)
      }
    }

    LOGGER.info(s"Per-record transformation complete: ${allTransformedLines.size} records transformed from directory")
    outputPath
  }

  private def transformSingleFileContent(
                                          config: TransformationConfig,
                                          filePath: String,
                                          instance: AnyRef,
                                          method: java.lang.reflect.Method
                                        ): List[String] = {
    val source = Source.fromFile(filePath, "UTF-8")
    try {
      source.getLines().map { line =>
        val transformedLine = if (method.getParameterCount == 2) {
          method.invoke(instance, line, config.options).asInstanceOf[String]
        } else {
          method.invoke(instance, line).asInstanceOf[String]
        }
        transformedLine
      }.toList
    } finally {
      source.close()
    }
  }

  /**
   * Find the transform method using reflection.
   * Supports two signatures:
   * 1. (String, Map[String, String]) => String
   * 2. (String) => String
   */
  private def findMethod(clazz: Class[_], methodName: String): java.lang.reflect.Method = {
    val methods = clazz.getMethods.filter(_.getName == methodName)

    if (methods.isEmpty) {
      throw new NoSuchMethodException(s"Method '$methodName' not found in class ${clazz.getName}")
    }

    // Find method with signature: (String, Map[String, String]) => String
    val optMethod = methods.find { m =>
      m.getParameterCount == 2 &&
        m.getParameterTypes()(0) == classOf[String] &&
        m.getParameterTypes()(1).isAssignableFrom(classOf[Map[String, String]]) &&
        m.getReturnType == classOf[String]
    }

    optMethod.orElse {
      // Try simple signature: String => String
      methods.find { m =>
        m.getParameterCount == 1 &&
          m.getParameterTypes()(0) == classOf[String] &&
          m.getReturnType == classOf[String]
      }
    }.getOrElse {
      throw new NoSuchMethodException(
        s"No compatible method '$methodName' found in ${clazz.getName}. Expected signatures:\n" +
          s"  - def $methodName(record: String, options: Map[String, String]): String\n" +
          s"  - def $methodName(record: String): String"
      )
    }
  }
}
