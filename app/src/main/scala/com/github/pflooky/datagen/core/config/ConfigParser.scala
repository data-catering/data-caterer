package com.github.pflooky.datagen.core.config

import com.github.pflooky.datacaterer.api.model.Constants.FORMAT
import com.github.pflooky.datacaterer.api.model.{AlertConfig, DataCatererConfiguration, FlagsConfig, FoldersConfig, GenerationConfig, MetadataConfig, ValidationConfig}
import com.github.pflooky.datagen.core.model.Constants.{APPLICATION_CONFIG_PATH, RUNTIME_MASTER, SUPPORTED_CONNECTION_FORMATS}
import com.github.pflooky.datagen.core.util.ObjectMapperUtil
import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}
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
    SUPPORTED_CONNECTION_FORMATS.map(format => {
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
  }

  def getSparkConnectionConfig: Map[String, String] = {
    connectionConfigsByName.flatMap(connectionConf => connectionConf._2.filter(_._1.startsWith("spark")))
  }

  def toDataCatererConfiguration: DataCatererConfiguration = {
    DataCatererConfiguration(
      flagsConfig,
      foldersConfig,
      metadataConfig,
      generationConfig,
      validationConfig,
      alertConfig,
      connectionConfigsByName,
      baseRuntimeConfig ++ sparkConnectionConfig,
      master
    )
  }

}