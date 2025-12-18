package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Constants.CSV
import org.apache.log4j.Logger

import java.nio.file.{Files, Paths, StandardCopyOption}

/**
 * Handles consolidation of Spark-generated part files into single files.
 * This is useful when users specify a path with a file extension (e.g., output.json)
 * but Spark generates a directory with multiple part files.
 */
class FileConsolidator {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Detects if a path contains a file suffix (e.g., .json, .csv, .parquet, etc.)
   * @param path The file path to check
   * @return Some(extension) if a file suffix is detected, None otherwise
   */
  def detectFileSuffix(path: String): Option[String] = {
    val supportedExtensions = List(".json", ".csv", ".parquet", ".orc", ".xml", ".txt")
    supportedExtensions.find(ext => path.toLowerCase.endsWith(ext))
  }

  /**
   * Consolidates Spark-generated part files into a single file with the specified name.
   *
   * @param sparkOutputPath The directory path where Spark wrote the part files
   * @param targetFilePath The desired single file path
   * @param format The file format (used for special handling like CSV headers)
   * @param connectionConfig Connection configuration that may contain format-specific options
   */
  def consolidatePartFiles(
    sparkOutputPath: String,
    targetFilePath: String,
    format: String,
    connectionConfig: Map[String, String] = Map()
  ): Unit = {
    val outputDir = Paths.get(sparkOutputPath)

    if (Files.exists(outputDir) && Files.isDirectory(outputDir)) {
      LOGGER.info(s"Consolidating part files from $sparkOutputPath to $targetFilePath")

      val partFiles = Files.list(outputDir)
        .filter(p => {
          val fileName = p.getFileName.toString
          fileName.startsWith("part-") && !fileName.endsWith(".crc") && fileName != "_SUCCESS"
        })
        .toArray()
        .map(_.asInstanceOf[java.nio.file.Path])
        .sortBy(_.getFileName.toString)

      if (partFiles.isEmpty) {
        LOGGER.warn(s"No part files found in $sparkOutputPath")
        return
      }

      val targetPath = Paths.get(targetFilePath)
      val targetDir = targetPath.getParent
      if (targetDir != null && !Files.exists(targetDir)) {
        Files.createDirectories(targetDir)
      }

      // If there's only one part file, just move/rename it
      if (partFiles.length == 1) {
        Files.move(partFiles.head, targetPath, StandardCopyOption.REPLACE_EXISTING)
        LOGGER.info(s"Moved single part file to $targetFilePath")
      } else {
        // Multiple part files need to be concatenated
        LOGGER.info(s"Concatenating ${partFiles.length} part files into $targetFilePath")

        // Check if we need special handling for CSV with headers
        val hasHeaders = format.equalsIgnoreCase(CSV) && connectionConfig.get("header").exists(_.equalsIgnoreCase("true"))

        if (hasHeaders) {
          consolidateCsvWithHeaders(partFiles, targetPath)
        } else {
          // Standard concatenation for other formats
          val targetFile = Files.newOutputStream(targetPath)
          try {
            partFiles.foreach { partFile =>
              Files.copy(partFile, targetFile)
            }
          } finally {
            targetFile.close()
          }
        }
      }

      // Clean up the Spark-generated directory
      cleanupDirectory(outputDir)
      LOGGER.info(s"Cleaned up temporary directory: $sparkOutputPath")
    } else {
      LOGGER.warn(s"Spark output directory does not exist or is not a directory: $sparkOutputPath")
    }
  }

  /**
   * Consolidates CSV part files while handling headers correctly.
   * The header from the first file is kept, and headers from subsequent files are skipped.
   *
   * @param partFiles Sorted array of part file paths
   * @param targetPath The target file path
   */
  def consolidateCsvWithHeaders(partFiles: Array[java.nio.file.Path], targetPath: java.nio.file.Path): Unit = {
    LOGGER.debug(s"Consolidating CSV files with header handling")
    val targetFile = Files.newOutputStream(targetPath)
    val lineSeparator = System.lineSeparator().getBytes("UTF-8")

    try {
      var headerWritten = false

      partFiles.foreach { partFile =>
        val lines = Files.readAllLines(partFile)

        if (lines.isEmpty) {
          LOGGER.debug(s"Skipping empty part file: ${partFile.getFileName}")
        } else if (!headerWritten) {
          // First non-empty file: write all lines including header
          for (i <- 0 until lines.size()) {
            targetFile.write(lines.get(i).getBytes("UTF-8"))
            if (i < lines.size() - 1 || partFiles.length > 1) {
              // Write line separator except for the last line of the last file
              targetFile.write(lineSeparator)
            }
          }
          headerWritten = true
        } else {
          // Subsequent files: skip first line (header) and write the rest
          if (lines.size() > 1) {
            for (i <- 1 until lines.size()) {
              targetFile.write(lines.get(i).getBytes("UTF-8"))
              if (i < lines.size() - 1 || partFiles.indexOf(partFile) < partFiles.length - 1) {
                // Write line separator except for the last line of the last file
                targetFile.write(lineSeparator)
              }
            }
          }
        }
      }
    } finally {
      targetFile.close()
    }
  }

  /**
   * Recursively deletes a directory and all its contents
   * @param directory The directory path to delete
   */
  def cleanupDirectory(directory: java.nio.file.Path): Unit = {
    if (Files.exists(directory)) {
      Files.walk(directory)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }
}

/**
 * Companion object providing a shared instance
 */
object FileConsolidator {
  private val instance = new FileConsolidator()

  def apply(): FileConsolidator = instance
}
