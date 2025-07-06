package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.Step
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Utility for reading data from data sources in reference mode.
 * This is used when a data source has enableReferenceMode=true and should read existing data
 * instead of generating new data.
 */
object DataSourceReader {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Read data from a data source based on the step configuration.
   *
   * @param dataSourceName    Name of the data source
   * @param step             Step configuration containing connection details
   * @param connectionConfig Additional connection configuration
   * @param sparkSession     Spark session
   * @return DataFrame containing the read data
   */
  def readDataFromSource(
                          dataSourceName: String,
                          step: Step,
                          connectionConfig: Map[String, String]
                        )(implicit sparkSession: SparkSession): DataFrame = {
    val format = step.options.getOrElse(FORMAT, connectionConfig.getOrElse(FORMAT, ""))
    val allOptions = connectionConfig ++ step.options
    
    if (format.isEmpty) {
      throw new IllegalArgumentException(s"No format specified for reference data source: $dataSourceName")
    }

    LOGGER.info(s"Reading reference data from data source, data-source-name=$dataSourceName, format=$format, step-name=${step.name}")
    
    try {
      val df = format.toLowerCase match {
        case CSV | JSON | PARQUET | ORC | DELTA | ICEBERG =>
          readFileBasedDataSource(format, allOptions)
        case JDBC =>
          readJdbcDataSource(allOptions)
        case CASSANDRA | CASSANDRA_NAME =>
          readCassandraDataSource(allOptions)
        case KAFKA =>
          readKafkaDataSource(allOptions)
        case unsupportedFormat =>
          throw new UnsupportedOperationException(
            s"Reference mode not supported for format: $unsupportedFormat. " +
              s"Supported formats: csv, json, parquet, orc, delta, iceberg, jdbc, cassandra, kafka"
          )
      }

      if (!df.storageLevel.useMemory) df.cache()
      
      val recordCount = if (df.schema.nonEmpty) {
        val count = df.count()
        LOGGER.debug(s"Successfully read reference data, data-source-name=$dataSourceName, format=$format, num-records=$count")
        count
      } else {
        LOGGER.warn(s"Reference data source has empty schema, data-source-name=$dataSourceName, format=$format")
        0
      }

      if (recordCount == 0) {
        LOGGER.warn(s"Reference data source contains no records. This may cause issues with foreign key relationships, " +
          s"data-source-name=$dataSourceName, format=$format")
      }

      df
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to read reference data from data source, data-source-name=$dataSourceName, format=$format, " +
          s"step-name=${step.name}, error=${ex.getMessage}")
        throw new RuntimeException(s"Failed to read reference data from $dataSourceName: ${ex.getMessage}", ex)
    }
  }

  /**
   * Read data from file-based data sources (CSV, JSON, Parquet, ORC, Delta, Iceberg)
   */
  private def readFileBasedDataSource(format: String, options: Map[String, String])(implicit sparkSession: SparkSession): DataFrame = {
    val path = options.getOrElse(PATH, throw new IllegalArgumentException(s"Path not specified for file-based data source, format=$format"))
    
    format.toLowerCase match {
      case ICEBERG =>
        // Iceberg requires special handling
        val tableName = options.getOrElse(TABLE, throw new IllegalArgumentException("Table name required for Iceberg data source"))
        val tableNameWithCatalog = if (tableName.split("\\.").length == 2) s"iceberg.$tableName" else tableName
        
        // Set Iceberg-specific configurations
        options.filter(_._1.startsWith("spark.sql"))
          .foreach(conf => sparkSession.sqlContext.setConf(conf._1, conf._2))
        
        sparkSession.read.options(options).table(tableNameWithCatalog)
      case _ =>
        // Remove path from options to avoid Spark conflict when both path option and load(path) are used
        val optionsWithoutPath = options - PATH
        sparkSession.read.format(format).options(optionsWithoutPath).load(path)
    }
  }

  /**
   * Read data from JDBC data sources
   */
  private def readJdbcDataSource(options: Map[String, String])(implicit sparkSession: SparkSession): DataFrame = {
    val requiredOptions = Set(URL, DRIVER)
    val missingOptions = requiredOptions.filterNot(options.contains)
    if (missingOptions.nonEmpty) {
      throw new IllegalArgumentException(s"Missing required JDBC options: ${missingOptions.mkString(", ")}")
    }

    // Check for table or query
    if (!options.contains(JDBC_TABLE) && !options.contains(JDBC_QUERY)) {
      throw new IllegalArgumentException("Either 'dbtable' or 'query' must be specified for JDBC data source")
    }

    sparkSession.read.format(JDBC).options(options).load()
  }

  /**
   * Read data from Cassandra data sources
   */
  private def readCassandraDataSource(options: Map[String, String])(implicit sparkSession: SparkSession): DataFrame = {
    val requiredOptions = Set(CASSANDRA_KEYSPACE, CASSANDRA_TABLE)
    val missingOptions = requiredOptions.filterNot(options.contains)
    if (missingOptions.nonEmpty) {
      throw new IllegalArgumentException(s"Missing required Cassandra options: ${missingOptions.mkString(", ")}")
    }

    sparkSession.read.format(CASSANDRA).options(options).load()
  }

  /**
   * Read data from Kafka data sources
   * Note: This reads the latest available data from the topic
   */
  private def readKafkaDataSource(options: Map[String, String])(implicit sparkSession: SparkSession): DataFrame = {
    val requiredOptions = Set("kafka.bootstrap.servers", KAFKA_TOPIC)
    val missingOptions = requiredOptions.filterNot(key => 
      options.contains(key) || options.contains(key.replace("kafka.", ""))
    )
    if (missingOptions.nonEmpty) {
      LOGGER.warn(s"Some Kafka options may be missing: ${missingOptions.mkString(", ")}. " +
        "Ensure bootstrap servers and topic are configured correctly.")
    }

    // Add default options for reading existing data
    val kafkaOptions = options ++ Map(
      "startingOffsets" -> "earliest",
      "endingOffsets" -> "latest"
    )

    sparkSession.read.format(KAFKA).options(kafkaOptions).load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
  }

  /**
   * Validate that reference mode configuration is correct
   */
  def validateReferenceMode(step: Step, connectionConfig: Map[String, String]): Unit = {
    val isReferenceMode = step.options.get(ENABLE_REFERENCE_MODE).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_REFERENCE_MODE)
    val isDataGeneration = step.options.get(ENABLE_DATA_GENERATION).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_GENERATE_DATA)

    if (isReferenceMode && isDataGeneration) {
      throw new IllegalArgumentException(
        s"Cannot enable both reference mode and data generation for the same step: ${step.name}. " +
          "Please enable only one mode."
      )
    }

    if (isReferenceMode) {
      val format = step.options.getOrElse(FORMAT, connectionConfig.getOrElse(FORMAT, ""))
      if (format.isEmpty) {
        throw new IllegalArgumentException(s"Format must be specified for reference mode step: ${step.name}")
      }

      // Validate required options for each format
      format.toLowerCase match {
        case CSV | JSON | PARQUET | ORC | DELTA =>
          if (!step.options.contains(PATH) && !connectionConfig.contains(PATH)) {
            throw new IllegalArgumentException(s"Path must be specified for file-based reference data source: ${step.name}")
          }
        case ICEBERG =>
          if (!step.options.contains(TABLE) && !connectionConfig.contains(TABLE)) {
            throw new IllegalArgumentException(s"Table name must be specified for Iceberg reference data source: ${step.name}")
          }
        case JDBC =>
          val hasTable = step.options.contains(JDBC_TABLE) || connectionConfig.contains(JDBC_TABLE)
          val hasQuery = step.options.contains(JDBC_QUERY) || connectionConfig.contains(JDBC_QUERY)
          if (!hasTable && !hasQuery) {
            throw new IllegalArgumentException(s"Either 'dbtable' or 'query' must be specified for JDBC reference data source: ${step.name}")
          }
        case CASSANDRA | CASSANDRA_NAME =>
          val requiredOptions = Set(CASSANDRA_KEYSPACE, CASSANDRA_TABLE)
          val missingOptions = requiredOptions.filterNot(key => 
            step.options.contains(key) || connectionConfig.contains(key)
          )
          if (missingOptions.nonEmpty) {
            throw new IllegalArgumentException(
              s"Missing required Cassandra options for reference data source ${step.name}: ${missingOptions.mkString(", ")}"
            )
          }
        case _ => // Other formats may not have specific validation requirements
      }
    }
  }
} 