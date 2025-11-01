package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.transformer.{PerRecordTransformer, WholeFileTransformer}
import io.github.datacatering.datacaterer.core.ui.model.{SampleMetadata, SampleResponse, SchemaInfo}
import io.github.datacatering.datacaterer.core.util.{DataFrameOmitUtil, ObjectMapperUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.jdk.CollectionConverters._

/**
 * Handles conversion of DataFrames to various output formats for sample data
 *
 * This class extracts and consolidates all DataFrame conversion logic from FastSampleGenerator,
 * including:
 * - Converting DataFrames to raw bytes in various formats (JSON, CSV, Parquet, ORC)
 * - Applying transformations to generated data
 * - Converting DataFrames to API response format
 * - Determining content types for different formats
 *
 * Responsibilities:
 * - DataFrame serialization to file formats
 * - Transformation application (per-record and whole-file)
 * - Schema extraction and conversion
 * - Response object creation
 */
object SampleDataConverter {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Converts a DataFrame to raw bytes in the specified format
   *
   * This method:
   * 1. Writes the DataFrame to a temp file using Spark's native writers
   * 2. Applies any configured transformations
   * 3. Reads the file back as raw bytes
   * 4. Cleans up temporary files
   *
   * @param df DataFrame to convert
   * @param format Output format (json, csv, parquet, orc)
   * @param step Step configuration (may contain transformation settings)
   * @return Raw bytes of the formatted and transformed data
   */
  def dataFrameToRawBytes(df: DataFrame, format: String, step: Step): Array[Byte] = {
    val tempDir = Files.createTempDirectory("sample_output_")
    val tempPath = tempDir.resolve(s"output.$format").toString

    try {
      // Write to temp file using Spark's native writers
      format.toLowerCase match {
        case "json" =>
          // For JSON, write as a single file and read back
          df.coalesce(1).write.mode("overwrite").json(tempPath)

        case "csv" =>
          // For CSV, write with header
          df.coalesce(1).write.mode("overwrite")
            .option("header", "true")
            .csv(tempPath)

        case "parquet" =>
          df.coalesce(1).write.mode("overwrite").parquet(tempPath)

        case "orc" =>
          df.coalesce(1).write.mode("overwrite").orc(tempPath)

        case _ =>
          throw new IllegalArgumentException(s"Unsupported format for raw output: $format")
      }

      // Find the part file (Spark creates part-* files)
      val partFiles = Files.list(Paths.get(tempPath))
        .filter(p => {
          val fileName = p.getFileName.toString
          fileName.startsWith("part-") && !fileName.endsWith(".crc")
        })
        .toArray()
        .map(_.asInstanceOf[java.nio.file.Path])

      if (partFiles.isEmpty) {
        throw new RuntimeException(s"No output file generated in $tempPath")
      }

      // Apply transformation if configured
      val finalFile = step.transformation match {
        case Some(config) if config.isEnabled =>
          LOGGER.debug(s"Applying ${config.mode} transformation to sample data")
          val transformedPath = if (config.isPerRecord) {
            PerRecordTransformer.transform(config, partFiles.head.toString, format)
          } else {
            WholeFileTransformer.transform(config, partFiles.head.toString, format)
          }
          Paths.get(transformedPath)

        case _ =>
          partFiles.head
      }

      // Read the content as bytes
      val bytes = Files.readAllBytes(finalFile)
      bytes

    } finally {
      // Clean up temp directory
      try {
        Files.walk(tempDir)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.delete)
      } catch {
        case ex: Exception =>
          LOGGER.warn(s"Failed to clean up temp directory: $tempDir", ex)
      }
    }
  }

  /**
   * Gets the Content-Type header for a given format
   *
   * @param format File format (json, csv, parquet, orc)
   * @return HTTP Content-Type string
   */
  def contentTypeForFormat(format: String): String = {
    format.toLowerCase match {
      case "json" => "application/json"
      case "csv" => "text/csv"
      case "parquet" => "application/octet-stream"
      case "orc" => "application/octet-stream"
      case _ => "application/octet-stream"
    }
  }

  /**
   * Convert a DataFrame to SampleResponse format
   *
   * This method:
   * 1. Removes omitted fields from the DataFrame
   * 2. Converts rows to Map[String, Any] format
   * 3. Extracts schema information
   * 4. Creates a SampleResponse with metadata
   *
   * @param df DataFrame to convert
   * @param sampleSize The effective sample size (for metadata)
   * @param fastMode Whether fast mode was enabled
   * @param format Output format
   * @param enableRelationships Whether relationships were enabled
   * @param step The step configuration
   * @return SampleResponseWithDataFrame containing response and optional DataFrame
   */
  def convertDataFrameToSampleResponse(
    df: DataFrame,
    sampleSize: Int,
    fastMode: Boolean,
    format: String,
    enableRelationships: Boolean,
    step: Step
  )(implicit sparkSession: org.apache.spark.sql.SparkSession): FastSampleGenerator.SampleResponseWithDataFrame = {
    val startTime = System.currentTimeMillis()
    val executionId = generateId()

    // Remove omitted fields from DataFrame first (single pass)
    val cleanDf = DataFrameOmitUtil.removeOmitFields(df)
    val cleanSchema = cleanDf.schema

    // Convert to response format (omitted fields already removed)
    val sampleData = cleanDf.collect().map(row => {
      val jsonString = row.json
      val jsonNode = ObjectMapperUtil.jsonObjectMapper.readTree(jsonString)
      ObjectMapperUtil.jsonObjectMapper.convertValue(jsonNode, classOf[java.util.Map[String, Any]])
        .asInstanceOf[java.util.Map[String, Any]]
        .asScala
        .toMap
    }).toList

    val schemaInfo = SchemaInfo.fromSparkSchema(cleanSchema)
    val generationTime = System.currentTimeMillis() - startTime

    val response = SampleResponse(
      success = true,
      executionId = executionId,
      schema = Some(schemaInfo),
      sampleData = Some(sampleData),
      metadata = Some(SampleMetadata(
        sampleSize = sampleSize,
        actualRecords = sampleData.length,
        generatedInMs = generationTime,
        fastModeEnabled = fastMode,
        relationshipsEnabled = enableRelationships
      )),
      format = Some(format)
    )

    FastSampleGenerator.SampleResponseWithDataFrame(response, Some(cleanDf), Some(step))
  }

  /**
   * Generate a unique execution ID for sample responses
   *
   * @return Short UUID (first segment only)
   */
  def generateId(): String = UUID.randomUUID().toString.split("-").head
}
