package io.github.datacatering.datacaterer.core.config

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}
import io.github.datacatering.datacaterer.api.model.Constants.{DRIVER, FORMAT, JDBC, MYSQL, MYSQL_DRIVER, POSTGRES, POSTGRES_DRIVER}
import io.github.datacatering.datacaterer.api.model.{AlertConfig, DataCatererConfiguration, FlagsConfig, FoldersConfig, GenerationConfig, MetadataConfig, StreamingConfig, ValidationConfig}
import io.github.datacatering.datacaterer.core.model.Constants.{APPLICATION_CONFIG_PATH, RUNTIME_MASTER, SUPPORTED_CONNECTION_FORMATS}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger

import java.io.File
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.util.Try

object ConfigParser {

  private val LOGGER = Logger.getLogger(getClass.getName)

  lazy val config: Config = getConfig
  lazy val flagsConfig: FlagsConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("flags").unwrapped(), classOf[FlagsConfig])
  lazy val foldersConfig: FoldersConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("folders").unwrapped(), classOf[FoldersConfig])
  lazy val metadataConfig: MetadataConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("metadata").unwrapped(), classOf[MetadataConfig])
  lazy val generationConfig: GenerationConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("generation").unwrapped(), classOf[GenerationConfig])
  lazy val validationConfig: ValidationConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("validation").unwrapped(), classOf[ValidationConfig])
  lazy val streamingConfig: StreamingConfig = Try(
    ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("streaming").unwrapped(), classOf[StreamingConfig])
  ).getOrElse {
    LOGGER.debug("No streaming configuration found in application.conf, using defaults")
    StreamingConfig()
  }
  lazy val alertConfig: AlertConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("alert").unwrapped(), classOf[AlertConfig])
  lazy val baseRuntimeConfig: Map[String, String] = ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("runtime.config").unwrapped(), classOf[Map[String, String]])
  lazy val master: String = config.getString(RUNTIME_MASTER)
  lazy val connectionConfigsByName: Map[String, Map[String, String]] = getConnectionConfigsByName
  lazy val sparkConnectionConfig: Map[String, String] = getSparkConnectionConfig

  def getConfig: Config = {
    val appConfEnv = System.getenv(APPLICATION_CONFIG_PATH)
    val appConfProp = System.getProperty(APPLICATION_CONFIG_PATH)
    val applicationConfPath = (appConfEnv, appConfProp) match {
      case (null, null) => "application.conf"
      case (env, _) if env != null => env
      case (_, prop) if prop != null => prop
      case _ => "application.conf"
    }
    LOGGER.debug(s"Using application config file path, path=$applicationConfPath")
    val applicationConfFile = new File(applicationConfPath)
    if (!applicationConfFile.exists()) {
      val confFromClassPath = getClass.getClassLoader.getResource(applicationConfPath)
      ConfigFactory.parseURL(confFromClassPath).resolve()
    } else {
      ConfigFactory.parseFile(applicationConfFile).resolve()
    }
  }

  def getConnectionConfigsByName: Map[String, Map[String, String]] = {
    // Map of connection types that need special format/driver handling
    // These are not directly supported Spark formats but need to be mapped to JDBC
    val jdbcConnectionTypes: Map[String, Map[String, String]] = Map(
      POSTGRES -> Map(FORMAT -> JDBC, DRIVER -> POSTGRES_DRIVER),
      MYSQL -> Map(FORMAT -> JDBC, DRIVER -> MYSQL_DRIVER)
    )
    
    // Parse standard connection formats (csv, json, jdbc, etc.)
    val standardConnections = SUPPORTED_CONNECTION_FORMATS.map(format => {
      val tryBaseConfig = Try(config.getConfig(format))
      tryBaseConfig.map(baseConfig => {
        val connectionNames = baseConfig.root().keySet().asScala
        connectionNames.flatMap(name => {
          baseConfig.getValue(name).valueType() match {
            case ConfigValueType.OBJECT =>
              val connectionConfig = baseConfig.getConfig(name)
              val configValueMap = connectionConfig.entrySet().asScala
                .map(e => (e.getKey, e.getValue.render().replaceAll("\"", "")))
                .toMap
              Map(name -> (configValueMap ++ Map(FORMAT -> format)))
            case _ => Map[String, Map[String, String]]()
          }
        }).toMap
      }).getOrElse(Map())
    }).reduce((x, y) => x ++ y)
    
    // Parse JDBC-based connection types (postgres, mysql) that need format/driver mapping
    val jdbcBasedConnections = jdbcConnectionTypes.keys.toList.map(connType => {
      val tryBaseConfig = Try(config.getConfig(connType))
      tryBaseConfig.map(baseConfig => {
        val connectionNames = baseConfig.root().keySet().asScala
        connectionNames.flatMap(name => {
          baseConfig.getValue(name).valueType() match {
            case ConfigValueType.OBJECT =>
              val connectionConfig = baseConfig.getConfig(name)
              val configValueMap = connectionConfig.entrySet().asScala
                .map(e => (e.getKey, e.getValue.render().replaceAll("\"", "")))
                .toMap
              // Add format and driver from the mapping, but allow config to override driver if specified
              val additionalConfig = jdbcConnectionTypes(connType)
              val finalConfig = if (configValueMap.contains(DRIVER)) {
                configValueMap ++ Map(FORMAT -> JDBC)
              } else {
                configValueMap ++ additionalConfig
              }
              Map(name -> finalConfig)
            case _ => Map[String, Map[String, String]]()
          }
        }).toMap
      }).getOrElse(Map())
    }).foldLeft(Map[String, Map[String, String]]())((x, y) => x ++ y)
    
    standardConnections ++ jdbcBasedConnections
  }

  def getSparkConnectionConfig: Map[String, String] = {
    connectionConfigsByName.flatMap(connectionConf => connectionConf._2.filter(_._1.startsWith("spark")))
  }

  def toDataCatererConfiguration: DataCatererConfiguration = {
    val (optimizedFlags, optimizedGeneration, optimizedRuntime) = applyFastGenerationOptimizations(flagsConfig, generationConfig, baseRuntimeConfig)
    DataCatererConfiguration(
      optimizedFlags,
      foldersConfig,
      metadataConfig,
      optimizedGeneration,
      validationConfig,
      streamingConfig,
      alertConfig,
      connectionConfigsByName,
      optimizedRuntime ++ sparkConnectionConfig,
      master
    )
  }

  /**
   * Create a DataCatererConfiguration with fresh config parsing.
   * This method is useful for testing when config files need to be reloaded.
   */
  def toDataCatererConfigurationWithReload: DataCatererConfiguration = {
    val freshConfig = getConfig
    val freshFlagsConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(freshConfig.getObject("flags").unwrapped(), classOf[FlagsConfig])
    val freshFoldersConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(freshConfig.getObject("folders").unwrapped(), classOf[FoldersConfig])
    val freshMetadataConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(freshConfig.getObject("metadata").unwrapped(), classOf[MetadataConfig])
    val freshGenerationConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(freshConfig.getObject("generation").unwrapped(), classOf[GenerationConfig])
    val freshValidationConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(freshConfig.getObject("validation").unwrapped(), classOf[ValidationConfig])
    val freshStreamingConfig = Try(
      ObjectMapperUtil.jsonObjectMapper.convertValue(freshConfig.getObject("streaming").unwrapped(), classOf[StreamingConfig])
    ).getOrElse {
      LOGGER.debug("No streaming configuration found in application.conf (reload), using defaults")
      StreamingConfig()
    }
    val freshAlertConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(freshConfig.getObject("alert").unwrapped(), classOf[AlertConfig])
    val freshBaseRuntimeConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(freshConfig.getObject("runtime.config").unwrapped(), classOf[Map[String, String]])
    val freshMaster = freshConfig.getString(RUNTIME_MASTER)
    val freshConnectionConfigsByName = getConnectionConfigsByName
    val freshSparkConnectionConfig = getSparkConnectionConfig

    val (optimizedFlags, optimizedGeneration, optimizedRuntime) = applyFastGenerationOptimizations(freshFlagsConfig, freshGenerationConfig, freshBaseRuntimeConfig)
    DataCatererConfiguration(
      optimizedFlags,
      freshFoldersConfig,
      freshMetadataConfig,
      optimizedGeneration,
      freshValidationConfig,
      freshStreamingConfig,
      freshAlertConfig,
      freshConnectionConfigsByName,
      optimizedRuntime ++ freshSparkConnectionConfig,
      freshMaster
    )
  }

  /**
   * Apply optimizations when fast generation mode is enabled.
   * Disables features that slow down data generation in favor of maximum speed.
   */
  private def applyFastGenerationOptimizations(
    flags: FlagsConfig, 
    generation: GenerationConfig,
    runtime: Map[String, String]
  ): (FlagsConfig, GenerationConfig, Map[String, String]) = {
    if (flags.enableFastGeneration) {
      LOGGER.info("Fast generation mode enabled - applying performance optimizations")
      
      // Disable slow features for maximum speed
      val optimizedFlags = flags.copy(
        enableRecordTracking = false,
        enableCount = false,
        enableSinkMetadata = false,
        enableUniqueCheck = false,
        enableUniqueCheckOnlyInBatch = false,
        enableSaveReports = false,
        enableValidation = false,
        enableGenerateValidations = false,
        enableAlerts = false
      )
      
      // Optimize generation settings for speed
      val optimizedGeneration = generation.copy(
        numRecordsPerBatch = math.max(generation.numRecordsPerBatch, 1000000L), // Increase batch size for better throughput
        uniqueBloomFilterNumItems = 100000L, // Reduce bloom filter size (not used anyway when unique check disabled)
        uniqueBloomFilterFalsePositiveProbability = 0.1 // Higher false positive rate for smaller filter
      )
      
      // Optimize Spark runtime settings for speed
      val optimizedRuntime = runtime ++ Map(
        "spark.sql.shuffle.partitions" -> "20", // Increase from default of 10 for better parallelism
        "spark.sql.adaptive.coalescePartitions.enabled" -> "true", // Enable partition coalescing
        "spark.sql.adaptive.skewJoin.enabled" -> "true", // Handle data skew
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer", // Faster serialization
        "spark.sql.cbo.enabled" -> "true", // Enable cost-based optimization
        "spark.sql.adaptive.enabled" -> "true", // Enable adaptive query execution
      )
      
      LOGGER.info("Fast generation optimizations applied: " +
        "disabled record tracking, count, sink metadata, unique checks, reports, validation, and alerts. " +
        s"Increased batch size to ${optimizedGeneration.numRecordsPerBatch}. " +
        "Optimized Spark settings for performance.")
      
      (optimizedFlags, optimizedGeneration, optimizedRuntime)
    } else {
      (flags, generation, runtime)
    }
  }

}