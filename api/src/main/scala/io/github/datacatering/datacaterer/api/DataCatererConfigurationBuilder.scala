package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.converter.Converters.toScalaMap
import io.github.datacatering.datacaterer.api.model.Constants._
import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api.connection.{CassandraBuilder, ConnectionTaskBuilder, FileBuilder, HttpBuilder, KafkaBuilder, MySqlBuilder, PostgresBuilder, SolaceBuilder}
import io.github.datacatering.datacaterer.api.model.DataCatererConfiguration

import scala.annotation.varargs

case class DataCatererConfigurationBuilder(build: DataCatererConfiguration = DataCatererConfiguration()) {
  def this() = this(DataCatererConfiguration())

  def master(master: String): DataCatererConfigurationBuilder =
    this.modify(_.build.master).setTo(master)

  def runtimeConfig(conf: Map[String, String]): DataCatererConfigurationBuilder =
    this.modify(_.build.runtimeConfig)(_ ++ conf)

  def runtimeConfig(conf: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    runtimeConfig(toScalaMap(conf))

  def addRuntimeConfig(conf: (String, String)): DataCatererConfigurationBuilder =
    this.modify(_.build.runtimeConfig)(_ ++ Map(conf))

  def addRuntimeConfig(key: String, value: String): DataCatererConfigurationBuilder =
    addRuntimeConfig(key -> value)


  def connectionConfig(connectionConfigByName: Map[String, Map[String, String]]): DataCatererConfigurationBuilder =
    this.modify(_.build.connectionConfigByName)(_ ++ connectionConfigByName)

  def connectionConfig(connectionConfigByName: java.util.Map[String, java.util.Map[String, String]]): DataCatererConfigurationBuilder = {
    val scalaConf = toScalaMap(connectionConfigByName)
    val mappedConf = scalaConf.map(c => (c._1, toScalaMap(c._2)))
    connectionConfig(mappedConf)
  }

  def addConnectionConfig(name: String, format: String, connectionConfig: Map[String, String]): DataCatererConfigurationBuilder =
    this.modify(_.build.connectionConfigByName)(_ ++ Map(name -> (connectionConfig ++ Map(FORMAT -> format))))

  def addConnectionConfigJava(name: String, format: String, connectionConfig: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    addConnectionConfig(name, format, toScalaMap(connectionConfig))

  def addConnectionConfig(name: String, format: String, path: String, connectionConfig: Map[String, String]): DataCatererConfigurationBuilder = {
    val pathConf = if (path.nonEmpty) Map(PATH -> path) else Map()
    this.modify(_.build.connectionConfigByName)(_ ++ Map(name -> (connectionConfig ++ Map(FORMAT -> format) ++ pathConf)))
  }

  def addConnectionConfigJava(name: String, format: String, path: String, connectionConfig: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    addConnectionConfig(name, format, path, toScalaMap(connectionConfig))

  def csv(name: String, path: String, options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, CSV, path, options)

  def csv(name: String, path: String, options: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    csv(name, path, toScalaMap(options))

  def parquet(name: String, path: String, options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, PARQUET, path, options)

  def parquet(name: String, path: String, options: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    parquet(name, path, toScalaMap(options))

  def orc(name: String, path: String, options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, ORC, path, options)

  def orc(name: String, path: String, options: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    orc(name, path, toScalaMap(options))

  def json(name: String, path: String, options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, JSON, path, options)

  def json(name: String, path: String, options: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    json(name, path, toScalaMap(options))

  def hudi(name: String, path: String, tableName: String, options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, HUDI, path, options ++ Map(HUDI_TABLE_NAME -> tableName))

  def hudi(name: String, path: String, tableName: String, options: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    hudi(name, path, tableName, toScalaMap(options))

  def delta(name: String, path: String = "", options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, DELTA, path, options)

  def delta(name: String, path: String, options: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    delta(name, path, toScalaMap(options))

  def iceberg(name: String, path: String = "", options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, ICEBERG, path, options)

  def iceberg(name: String, path: String, options: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    iceberg(name, path, toScalaMap(options))

  def postgres(
                name: String,
                url: String = DEFAULT_POSTGRES_URL,
                username: String = DEFAULT_POSTGRES_USERNAME,
                password: String = DEFAULT_POSTGRES_PASSWORD,
                options: Map[String, String] = Map()
              ): DataCatererConfigurationBuilder =
    addConnection(name, JDBC, url, username, password, options ++ Map(DRIVER -> POSTGRES_DRIVER))

  def postgres(
                name: String,
                url: String,
                username: String,
                password: String,
                options: java.util.Map[String, String]
              ): DataCatererConfigurationBuilder =
    postgres(name, url, username, password, toScalaMap(options))

  def postgres(
                name: String,
                url: String,
                options: java.util.Map[String, String]
              ): DataCatererConfigurationBuilder =
    postgres(name, url, options = toScalaMap(options))

  def postgres(
                name: String,
                url: String
              ): DataCatererConfigurationBuilder =
    postgres(name, url, DEFAULT_POSTGRES_USERNAME)

  def mysql(
             name: String,
             url: String = DEFAULT_MYSQL_URL,
             username: String = DEFAULT_MYSQL_USERNAME,
             password: String = DEFAULT_MYSQL_PASSWORD,
             options: Map[String, String] = Map()
           ): DataCatererConfigurationBuilder =
    addConnection(name, JDBC, url, username, password, options ++ Map(DRIVER -> MYSQL_DRIVER))

  def mysql(
             name: String,
             url: String,
             username: String,
             password: String,
             options: java.util.Map[String, String]
           ): DataCatererConfigurationBuilder =
    mysql(name, url, username, password, toScalaMap(options))

  def mysql(
             name: String,
             url: String,
             options: java.util.Map[String, String]
           ): DataCatererConfigurationBuilder =
    mysql(name, url, options = toScalaMap(options))

  def mysql(
             name: String,
             url: String
           ): DataCatererConfigurationBuilder =
    mysql(name, url, DEFAULT_MYSQL_USERNAME)

  def cassandra(
                 name: String,
                 url: String = DEFAULT_CASSANDRA_URL,
                 username: String = DEFAULT_CASSANDRA_USERNAME,
                 password: String = DEFAULT_CASSANDRA_PASSWORD,
                 options: Map[String, String] = Map()
               ): DataCatererConfigurationBuilder = {
    val sptUrl = url.split(":")
    assert(sptUrl.size == 2, "url should have format '<host>:<port>'")
    val allOptions = Map(
      "spark.cassandra.connection.host" -> sptUrl.head,
      "spark.cassandra.connection.port" -> sptUrl.last,
      "spark.cassandra.auth.username" -> username,
      "spark.cassandra.auth.password" -> password,
    ) ++ options
    addConnectionConfig(name, CASSANDRA, allOptions)
  }

  def cassandra(
                 name: String,
                 url: String,
                 username: String,
                 password: String,
                 options: java.util.Map[String, String]
               ): DataCatererConfigurationBuilder =
    cassandra(name, url, username, password, toScalaMap(options))

  def cassandra(
                 name: String,
                 url: String,
                 options: java.util.Map[String, String]
               ): DataCatererConfigurationBuilder =
    cassandra(name, url, options = toScalaMap(options))

  def cassandra(
                 name: String,
                 url: String
               ): DataCatererConfigurationBuilder =
    cassandra(name, url, DEFAULT_CASSANDRA_USERNAME)

  def jms(name: String, url: String, username: String, password: String, options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnection(name, JMS, url, username, password, options)

  def solace(
              name: String,
              url: String = DEFAULT_SOLACE_URL,
              username: String = DEFAULT_SOLACE_USERNAME,
              password: String = DEFAULT_SOLACE_PASSWORD,
              vpnName: String = DEFAULT_SOLACE_VPN_NAME,
              connectionFactory: String = DEFAULT_SOLACE_CONNECTION_FACTORY,
              initialContextFactory: String = DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY,
              options: Map[String, String] = Map()
            ): DataCatererConfigurationBuilder =
    jms(name, url, username, password, Map(
      JMS_VPN_NAME -> vpnName,
      JMS_CONNECTION_FACTORY -> connectionFactory,
      JMS_INITIAL_CONTEXT_FACTORY -> initialContextFactory,
    ) ++ options)

  def solace(
              name: String,
              url: String,
              username: String,
              password: String,
              vpnName: String,
              connectionFactory: String,
              initialContextFactory: String,
              options: java.util.Map[String, String]
            ): DataCatererConfigurationBuilder =
    solace(name, url, username, password, vpnName, connectionFactory, initialContextFactory, toScalaMap(options))

  def solace(
              name: String,
              url: String,
              username: String,
              password: String,
              vpnName: String
            ): DataCatererConfigurationBuilder =
    solace(name, url, username, password, vpnName, DEFAULT_SOLACE_CONNECTION_FACTORY)

  def solace(
              name: String,
              url: String
            ): DataCatererConfigurationBuilder =
    solace(name, url, DEFAULT_SOLACE_USERNAME)

  def kafka(name: String, url: String = DEFAULT_KAFKA_URL, options: Map[String, String] = Map()): DataCatererConfigurationBuilder = {
    addConnectionConfig(name, KAFKA, Map(
      "kafka.bootstrap.servers" -> url,
    ) ++ options)
  }

  def kafka(name: String, url: String, options: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    kafka(name, url, toScalaMap(options))

  def http(name: String, username: String = "", password: String = "", options: Map[String, String] = Map()): DataCatererConfigurationBuilder = {
    val authOptions = if (username.nonEmpty && password.nonEmpty) Map(USERNAME -> username, PASSWORD -> password) else Map()
    addConnectionConfig(name, HTTP, authOptions ++ options)
  }

  def http(name: String, username: String, password: String, options: java.util.Map[String, String]): DataCatererConfigurationBuilder =
    http(name, username, password, toScalaMap(options))

  private def addConnection(name: String, format: String, url: String, username: String,
                            password: String, options: Map[String, String]): DataCatererConfigurationBuilder = {
    addConnectionConfig(name, format, Map(
      URL -> url,
      USERNAME -> username,
      PASSWORD -> password
    ) ++ options)
  }


  def enableGenerateData(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableGenerateData).setTo(enable)

  def enableCount(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableCount).setTo(enable)

  def enableValidation(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableValidation).setTo(enable)

  def enableFailOnError(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableFailOnError).setTo(enable)

  def enableUniqueCheck(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableUniqueCheck).setTo(enable)

  def enableSaveReports(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableSaveReports).setTo(enable)

  def enableSinkMetadata(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableSinkMetadata).setTo(enable)

  def enableDeleteGeneratedRecords(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableDeleteGeneratedRecords).setTo(enable)

  def enableGeneratePlanAndTasks(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableGeneratePlanAndTasks).setTo(enable)

  def enableRecordTracking(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableRecordTracking).setTo(enable)

  def enableGenerateValidations(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableGenerateValidations).setTo(enable)


  def planFilePath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.planFilePath).setTo(path)

  def taskFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.taskFolderPath).setTo(path)

  def recordTrackingFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.recordTrackingFolderPath).setTo(path)

  def validationFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.validationFolderPath).setTo(path)

  def generatedReportsFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.generatedReportsFolderPath).setTo(path)

  def generatedPlanAndTaskFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.generatedPlanAndTaskFolderPath).setTo(path)

  def recordTrackingForValidationFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.recordTrackingForValidationFolderPath).setTo(path)


  def numRecordsFromDataSourceForDataProfiling(numRecords: Int): DataCatererConfigurationBuilder =
    this.modify(_.build.metadataConfig.numRecordsFromDataSource).setTo(numRecords)

  def numRecordsForAnalysisForDataProfiling(numRecords: Int): DataCatererConfigurationBuilder =
    this.modify(_.build.metadataConfig.numRecordsForAnalysis).setTo(numRecords)

  def numGeneratedSamples(numSamples: Int): DataCatererConfigurationBuilder =
    this.modify(_.build.metadataConfig.numGeneratedSamples).setTo(numSamples)

  def oneOfMinCount(minCount: Long): DataCatererConfigurationBuilder =
    this.modify(_.build.metadataConfig.oneOfMinCount).setTo(minCount)

  def oneOfDistinctCountVsCountThreshold(threshold: Double): DataCatererConfigurationBuilder =
    this.modify(_.build.metadataConfig.oneOfDistinctCountVsCountThreshold).setTo(threshold)


  def numRecordsPerBatch(numRecords: Long): DataCatererConfigurationBuilder =
    this.modify(_.build.generationConfig.numRecordsPerBatch).setTo(numRecords)

  def numRecordsPerStep(numRecords: Long): DataCatererConfigurationBuilder =
    this.modify(_.build.generationConfig.numRecordsPerStep).setTo(Some(numRecords))


  def numErrorSampleRecords(numRecords: Int): DataCatererConfigurationBuilder =
    this.modify(_.build.validationConfig.numSampleErrorRecords).setTo(numRecords)


  def slackAlertToken(token: String): DataCatererConfigurationBuilder =
    this.modify(_.build.alertConfig.slackAlertConfig.token).setTo(token)

  @varargs def slackAlertChannels(channels: String*): DataCatererConfigurationBuilder =
    this.modify(_.build.alertConfig.slackAlertConfig.channels).setTo(channels.toList)
}

final case class ConnectionConfigWithTaskBuilder(
                                                  dataSourceName: String = DEFAULT_DATA_SOURCE_NAME,
                                                  options: Map[String, String] = Map()
                                                ) {
  def this() = this(DEFAULT_DATA_SOURCE_NAME, Map())

  def file(name: String, format: String, path: String = "", options: Map[String, String] = Map()): FileBuilder = {
    val configBuilder = DataCatererConfigurationBuilder()
    val fileConnectionConfig = format match {
      case CSV => configBuilder.csv(name, path, options)
      case JSON => configBuilder.json(name, path, options)
      case ORC => configBuilder.orc(name, path, options)
      case PARQUET => configBuilder.parquet(name, path, options)
      case HUDI =>
        options.get(HUDI_TABLE_NAME) match {
          case Some(value) => configBuilder.hudi(name, path, value, options)
          case None => throw new IllegalArgumentException(s"Missing $HUDI_TABLE_NAME from options: $options")
        }
      case DELTA => configBuilder.delta(name, path, options)
      case ICEBERG => configBuilder.iceberg(name, path, options)
      case _ => throw new IllegalArgumentException(s"Unsupported file format: $format")
    }
    setConnectionConfig(name, fileConnectionConfig, FileBuilder())
  }

  def postgres(
                name: String,
                url: String,
                username: String,
                password: String,
                options: Map[String, String] = Map()
              ): PostgresBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().postgres(name, url, username, password, options)
    setConnectionConfig(name, configBuilder, PostgresBuilder())
  }

  def mysql(
             name: String,
             url: String,
             username: String,
             password: String,
             options: Map[String, String] = Map()
           ): MySqlBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().mysql(name, url, username, password, options)
    setConnectionConfig(name, configBuilder, MySqlBuilder())
  }

  def cassandra(
                 name: String,
                 url: String,
                 username: String,
                 password: String,
                 options: Map[String, String] = Map()
               ): CassandraBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().cassandra(name, url, username, password, options)
    setConnectionConfig(name, configBuilder, CassandraBuilder())
  }

  def solace(
              name: String,
              url: String,
              username: String,
              password: String,
              vpnName: String,
              connectionFactory: String,
              initialContextFactory: String,
              options: Map[String, String] = Map()
            ): SolaceBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().solace(name, url, username, password, vpnName, connectionFactory, initialContextFactory, options)
    setConnectionConfig(name, configBuilder, SolaceBuilder())
  }

  def kafka(name: String, url: String, options: Map[String, String] = Map()): KafkaBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().kafka(name, url, options)
    setConnectionConfig(name, configBuilder, KafkaBuilder())
  }

  def http(name: String, username: String, password: String, options: Map[String, String] = Map()): HttpBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().http(name, username, password, options)
    setConnectionConfig(name, configBuilder, HttpBuilder())
  }

  def options(options: Map[String, String]): ConnectionConfigWithTaskBuilder = {
    this.modify(_.options)(_ ++ options)
  }

  def metadataSource(metadataSourceBuilder: MetadataSourceBuilder): ConnectionConfigWithTaskBuilder = {
    this.modify(_.options)(_ ++ metadataSourceBuilder.metadataSource.allOptions)
  }

  private def setConnectionConfig[T <: ConnectionTaskBuilder[_]](name: String, configBuilder: DataCatererConfigurationBuilder, connectionBuilder: T): T = {
    val modifiedConnectionConfig = this.modify(_.dataSourceName).setTo(name)
      .modify(_.options).setTo(configBuilder.build.connectionConfigByName(name))
    connectionBuilder.connectionConfigWithTaskBuilder = modifiedConnectionConfig
    connectionBuilder
  }
}

