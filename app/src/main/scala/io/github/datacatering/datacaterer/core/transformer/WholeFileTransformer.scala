package io.github.datacatering.datacaterer.core.transformer

import io.github.datacatering.datacaterer.api.model.TransformationConfig
import org.apache.log4j.Logger

import java.nio.file.{Files, Paths}
import scala.util.{Failure, Success, Try}

/**
 * Applies whole-file transformation to files.
 * Transforms the entire file as a unit via user-provided method.
 */
object WholeFileTransformer {
  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Transform entire file as a unit.
   * The user-provided transformer method receives input path, output path, format, and options.
   *
   * @param config      Transformation configuration
   * @param inputPath   Path to input file (written by Data Caterer)
   * @param format      Original format (csv, json, etc.)
   * @return Path to transformed file
   */
  def transform(config: TransformationConfig, inputPath: String, format: String): String = {
    val outputPath = config.outputPath.getOrElse(s"${inputPath}.transformed")

    LOGGER.info(s"Applying whole-file transformation: class=${config.className}, method=${config.methodName}, " +
      s"input=$inputPath, output=$outputPath, format=$format")

    Try {
      // Load transformer class
      val clazz = Class.forName(config.className)
      val instance = clazz.getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]
      val method = findMethod(clazz, config.methodName)

      val inputFile = new java.io.File(inputPath)

      // Handle directory inputs (Spark part files) by consolidating them first
      val (actualInputPath, shouldCleanupTempFile) = if (inputFile.isDirectory) {
        LOGGER.debug(s"Input path is a directory, consolidating part files: $inputPath")
        val tempFile = consolidateDirectory(inputPath, format)
        (tempFile.getAbsolutePath, true)
      } else {
        LOGGER.debug(s"Input path is a file: $inputPath")
        (inputPath, false)
      }

      // Invoke transformation with the consolidated file path
      val resultPath = if (method.getParameterCount == 4) {
        method.invoke(instance, actualInputPath, outputPath, format, config.options).asInstanceOf[String]
      } else {
        method.invoke(instance, actualInputPath, outputPath).asInstanceOf[String]
      }

      val finalPath = resultPath
      LOGGER.debug(s"Transformation returned path: $finalPath")

      // Clean up temporary consolidated file if we created one
      if (shouldCleanupTempFile) {
        Try(Files.delete(Paths.get(actualInputPath))) match {
          case Success(_) =>
            LOGGER.debug(s"Deleted temporary consolidated file: $actualInputPath")
          case Failure(ex) =>
            LOGGER.warn(s"Failed to delete temporary consolidated file: $actualInputPath", ex)
        }
      }

      // Delete original if configured and output is different from input
      if (config.deleteOriginal && finalPath != inputPath) {
        Try {
          if (inputFile.isDirectory) {
            // Delete directory and all its contents
            deleteDirectoryRecursively(inputPath)
          } else {
            Files.delete(Paths.get(inputPath))
          }
        } match {
          case Success(_) =>
            LOGGER.info(s"Deleted original input: $inputPath")
          case Failure(ex) =>
            LOGGER.warn(s"Failed to delete original input: $inputPath", ex)
        }
      }

      LOGGER.info(s"Whole-file transformation complete: $finalPath")
      finalPath

    } match {
      case Success(path) => path
      case Failure(exception) =>
        LOGGER.error(s"Whole-file transformation failed for file: $inputPath", exception)
        throw new TransformationException(s"Whole-file transformation failed: ${exception.getMessage}", exception)
    }
  }

  /**
   * Consolidates all files in a directory into a single temporary file.
   * This is needed when Spark writes part files to a directory but the transformation
   * expects a single file input.
   */
  private def consolidateDirectory(directoryPath: String, format: String): java.io.File = {
    val inputDir = new java.io.File(directoryPath)

    // Find all files in the directory (typically Spark part files)
    val filesToConsolidate = inputDir.listFiles()
      .filter(_.isFile)
      .filterNot(_.getName.startsWith(".")) // Skip hidden files
      .filterNot(_.getName.endsWith(".crc")) // Skip Spark CRC files
      .sortBy(_.getName) // Sort for consistent processing

    // Create a temporary file for consolidation
    val tempFile = java.io.File.createTempFile("data-caterer-consolidated-", s".$format")
    tempFile.deleteOnExit() // Clean up on JVM exit

    if (filesToConsolidate.isEmpty) {
      LOGGER.debug(s"No files found in directory: $directoryPath, creating empty consolidated file")
      // Create an empty file
      tempFile.createNewFile()
    } else {
      LOGGER.debug(s"Consolidating ${filesToConsolidate.length} files from $directoryPath into ${tempFile.getAbsolutePath}")

      // Consolidate all files into the temporary file
      val writer = new java.io.PrintWriter(new java.io.FileOutputStream(tempFile))

      try {
        filesToConsolidate.foreach { file =>
          LOGGER.debug(s"Reading file: ${file.getAbsolutePath}")
          val source = scala.io.Source.fromFile(file, "UTF-8")
          try {
            source.getLines().foreach(line => writer.println(line))
          } finally {
            source.close()
          }
        }
      } finally {
        writer.close()
      }
    }

    LOGGER.debug(s"Successfully consolidated ${filesToConsolidate.length} files into ${tempFile.getAbsolutePath}")
    tempFile
  }

  /**
   * Recursively deletes a directory and all its contents.
   */
  private def deleteDirectoryRecursively(path: String): Unit = {
    val file = new java.io.File(path)
    if (file.isDirectory) {
      file.listFiles().foreach { child =>
        if (child.isDirectory) {
          deleteDirectoryRecursively(child.getAbsolutePath)
        } else {
          Files.delete(child.toPath)
        }
      }
    }
    Files.delete(Paths.get(path))
  }

  /**
   * Find the transform method using reflection.
   * Supports two signatures:
   * 1. (String, String, String, Map[String, String]) => String
   * 2. (String, String) => String
   */
  private def findMethod(clazz: Class[_], methodName: String): java.lang.reflect.Method = {
    val methods = clazz.getMethods.filter(_.getName == methodName)

    if (methods.isEmpty) {
      throw new NoSuchMethodException(s"Method '$methodName' not found in class ${clazz.getName}")
    }

    // Find 4-parameter method: (String, String, String, Map[String, String]) => String
    val optMethod = methods.find { m =>
      m.getParameterCount == 4 &&
        m.getParameterTypes()(0) == classOf[String] &&
        m.getParameterTypes()(1) == classOf[String] &&
        m.getParameterTypes()(2) == classOf[String] &&
        m.getParameterTypes()(3).isAssignableFrom(classOf[Map[String, String]]) &&
        m.getReturnType == classOf[String]
    }

    optMethod.orElse {
      // Try 2-parameter method: (String, String) => String
      methods.find { m =>
        m.getParameterCount == 2 &&
          m.getParameterTypes()(0) == classOf[String] &&
          m.getParameterTypes()(1) == classOf[String] &&
          m.getReturnType == classOf[String]
      }
    }.getOrElse {
      throw new NoSuchMethodException(
        s"No compatible method '$methodName' found in ${clazz.getName}. Expected signatures:\n" +
          s"  - def $methodName(inputPath: String, outputPath: String, format: String, options: Map[String, String]): String\n" +
          s"  - def $methodName(inputPath: String, outputPath: String): String"
      )
    }
  }
}

/**
 * Exception thrown when transformation fails
 */
class TransformationException(message: String, cause: Throwable)
  extends RuntimeException(message, cause)
