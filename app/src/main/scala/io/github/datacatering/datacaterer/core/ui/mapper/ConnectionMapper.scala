package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.ConnectionConfigWithTaskBuilder
import io.github.datacatering.datacaterer.api.connection.{ConnectionTaskBuilder, FileBuilder, JdbcBuilder}
import io.github.datacatering.datacaterer.api.model.Constants.{CASSANDRA_KEYSPACE, CASSANDRA_NAME, CASSANDRA_TABLE, CSV, DELTA, HTTP, ICEBERG, ICEBERG_CATALOG_GLUE, ICEBERG_CATALOG_HADOOP, ICEBERG_CATALOG_HIVE, ICEBERG_CATALOG_REST, ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_URI, JMS_CONNECTION_FACTORY, JMS_DESTINATION_NAME, JMS_INITIAL_CONTEXT_FACTORY, JMS_VPN_NAME, JSON, KAFKA, KAFKA_TOPIC, MYSQL, ORC, PARQUET, PASSWORD, PATH, POSTGRES, SCHEMA, SOLACE, SPARK_ICEBERG_CATALOG_TYPE, SPARK_ICEBERG_CATALOG_URI, SPARK_ICEBERG_CATALOG_WAREHOUSE, TABLE, URL, USERNAME}
import io.github.datacatering.datacaterer.core.ui.mapper.UiMapper.checkOptions
import io.github.datacatering.datacaterer.core.ui.model.DataSourceRequest

object ConnectionMapper {

  def connectionMapping(dataSourceRequest: DataSourceRequest): ConnectionTaskBuilder[_] = {
    dataSourceRequest.`type` match {
      case Some(CASSANDRA_NAME) => createCassandraConnection(dataSourceRequest)
      case Some(POSTGRES) => createJdbcConnection(dataSourceRequest, POSTGRES)
      case Some(MYSQL) => createJdbcConnection(dataSourceRequest, MYSQL)
      case Some(CSV) => createFileConnection(dataSourceRequest, CSV)
      case Some(JSON) => createFileConnection(dataSourceRequest, JSON)
      case Some(PARQUET) => createFileConnection(dataSourceRequest, PARQUET)
      case Some(ORC) => createFileConnection(dataSourceRequest, ORC)
      case Some(DELTA) => createFileConnection(dataSourceRequest, DELTA)
      case Some(ICEBERG) => createIcebergConnection(dataSourceRequest)
      case Some(SOLACE) =>
        val opt = dataSourceRequest.options.getOrElse(Map())
        checkOptions(dataSourceRequest.name, List(URL, USERNAME, PASSWORD, JMS_DESTINATION_NAME, JMS_VPN_NAME, JMS_CONNECTION_FACTORY, JMS_INITIAL_CONTEXT_FACTORY), opt)
        ConnectionConfigWithTaskBuilder().solace(dataSourceRequest.name, opt(URL), opt(USERNAME), opt(PASSWORD),
          opt(JMS_VPN_NAME), opt(JMS_CONNECTION_FACTORY), opt(JMS_INITIAL_CONTEXT_FACTORY), opt)
      case Some(KAFKA) =>
        val opt = dataSourceRequest.options.getOrElse(Map())
        checkOptions(dataSourceRequest.name, List(URL, KAFKA_TOPIC), opt)
        ConnectionConfigWithTaskBuilder().kafka(dataSourceRequest.name, opt(URL), opt)
      case Some(HTTP) =>
        val opt = dataSourceRequest.options.getOrElse(Map())
        ConnectionConfigWithTaskBuilder().http(dataSourceRequest.name, opt.getOrElse(USERNAME, ""), opt.getOrElse(PASSWORD, ""), opt)
      case Some(x) =>
        throw new IllegalArgumentException(s"Unsupported data source from UI, data-source-type=$x")
      case _ =>
        throw new IllegalArgumentException(s"No data source type defined, unable to create connections, " +
          s"data-source-name=${dataSourceRequest.name}, task-name=${dataSourceRequest.taskName}")
    }
  }

  private def createFileConnection(dataSourceRequest: DataSourceRequest, format: String): FileBuilder = {
    val opt = dataSourceRequest.options.getOrElse(Map())
    checkOptions(dataSourceRequest.name, List(PATH), opt)
    ConnectionConfigWithTaskBuilder().file(dataSourceRequest.name, format, opt(PATH), opt)
  }

  private def createIcebergConnection(dataSourceRequest: DataSourceRequest): FileBuilder = {
    val opt = dataSourceRequest.options.getOrElse(Map())
    val name = dataSourceRequest.name
    checkOptions(name, List(ICEBERG_CATALOG_TYPE, TABLE), opt)
    val baseSparkOpts = Map(
      SPARK_ICEBERG_CATALOG_TYPE -> opt(ICEBERG_CATALOG_TYPE),
      TABLE -> opt(TABLE)
    )
    val sparkOpts = opt(ICEBERG_CATALOG_TYPE) match {
      case ICEBERG_CATALOG_HADOOP | ICEBERG_CATALOG_GLUE =>
        checkOptions(name, List(PATH), opt)
        Map(SPARK_ICEBERG_CATALOG_WAREHOUSE -> opt(PATH))
      case ICEBERG_CATALOG_HIVE | ICEBERG_CATALOG_REST =>
        checkOptions(name, List(ICEBERG_CATALOG_URI), opt)
        Map(SPARK_ICEBERG_CATALOG_URI -> opt(ICEBERG_CATALOG_URI))
      case _ => Map()
    }
    ConnectionConfigWithTaskBuilder().file(name, ICEBERG, opt.getOrElse(PATH, ""), baseSparkOpts ++ sparkOpts)
  }

  private def createJdbcConnection(dataSourceRequest: DataSourceRequest, format: String): JdbcBuilder[_] = {
    val opt = dataSourceRequest.options.getOrElse(Map())
    checkOptions(dataSourceRequest.name, List(URL, USERNAME, PASSWORD), opt)
    val connectionConfigWithTaskBuilder = ConnectionConfigWithTaskBuilder()

    val baseConnection = format match {
      case POSTGRES => connectionConfigWithTaskBuilder.postgres(dataSourceRequest.name, opt(URL), opt(USERNAME), opt(PASSWORD), opt)
      case MYSQL => connectionConfigWithTaskBuilder.mysql(dataSourceRequest.name, opt(URL), opt(USERNAME), opt(PASSWORD), opt)
      case x => throw new IllegalArgumentException(s"Unsupported connection format, format=$x")
    }

    (opt.get(SCHEMA), opt.get(TABLE)) match {
      case (Some(schema), Some(table)) => baseConnection.table(schema, table)
      case (Some(schema), None) =>
        assert(schema.nonEmpty, s"Empty schema name for $format connection, data-source-name=${dataSourceRequest.name}")
        throw new IllegalArgumentException(s"Missing table name for $format connection, data-source-name=${dataSourceRequest.name}, schema=$schema")
      case (None, Some(table)) =>
        assert(table.nonEmpty, s"Empty table name for $format connection, data-source-name=${dataSourceRequest.name}")
        throw new IllegalArgumentException(s"Missing schema name for $format connection, data-source-name=${dataSourceRequest.name}, table=$table")
      case (None, None) => baseConnection // TODO this is allowed only when there is metadata collection enabled
    }
  }

  private def createCassandraConnection(dataSourceRequest: DataSourceRequest) = {
    val opt = dataSourceRequest.options.getOrElse(Map())
    checkOptions(dataSourceRequest.name, List(URL, USERNAME, PASSWORD), opt)

    val cassandraConnection = ConnectionConfigWithTaskBuilder().cassandra(dataSourceRequest.name, opt(URL), opt(USERNAME), opt(PASSWORD), opt)
    (opt.get(CASSANDRA_KEYSPACE), opt.get(CASSANDRA_TABLE)) match {
      case (Some(keyspace), Some(table)) => cassandraConnection.table(keyspace, table)
      case (Some(keyspace), None) =>
        assert(keyspace.nonEmpty, s"Empty keyspace name for Cassandra connection, data-source-name=${dataSourceRequest.name}")
        throw new IllegalArgumentException(s"Missing table name for Cassandra connection, data-source-name=${dataSourceRequest.name}, keyspace=$keyspace")
      case (None, Some(table)) =>
        assert(table.nonEmpty, s"Empty table name for Cassandra connection, data-source-name=${dataSourceRequest.name}")
        throw new IllegalArgumentException(s"Missing keyspace name for Cassandra connection, data-source-name=${dataSourceRequest.name}, table=$table")
      case (None, None) => cassandraConnection // TODO this is allowed only when there is metadata collection enabled
    }
  }

}
