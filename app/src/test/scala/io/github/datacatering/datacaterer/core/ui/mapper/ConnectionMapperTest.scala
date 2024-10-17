package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.model.Constants.{CASSANDRA, CASSANDRA_KEYSPACE, CASSANDRA_NAME, CASSANDRA_TABLE, CSV, DRIVER, FORMAT, HTTP, JDBC, JMS, JMS_CONNECTION_FACTORY, JMS_DESTINATION_NAME, JMS_INITIAL_CONTEXT_FACTORY, JMS_VPN_NAME, JSON, KAFKA, KAFKA_TOPIC, MYSQL, MYSQL_DRIVER, ORC, PARQUET, PARTITIONS, PARTITION_BY, PASSWORD, PATH, POSTGRES, POSTGRES_DRIVER, SCHEMA, SOLACE, TABLE, URL, USERNAME}
import io.github.datacatering.datacaterer.core.ui.model.DataSourceRequest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConnectionMapperTest extends AnyFunSuite {

  test("Can convert UI connection mapping for Cassandra") {
    val dataSourceRequest = DataSourceRequest("cassandra-name", "task-1", Some(CASSANDRA_NAME), Some(Map(URL -> "localhost:9092", USERNAME -> "cassandra", PASSWORD -> "cassandra")))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult("cassandra-name")(res.dataSourceName)
    assertResult(8)(res.options.size)
    assertResult(Some(CASSANDRA))(res.options.get(FORMAT))
    assertResult(Some("localhost:9092"))(res.options.get(URL))
    assertResult(Some("localhost"))(res.options.get("spark.cassandra.connection.host"))
    assertResult(Some("9092"))(res.options.get("spark.cassandra.connection.port"))
    assertResult(Some("cassandra"))(res.options.get(USERNAME))
    assertResult(Some("cassandra"))(res.options.get(PASSWORD))
    assertResult(Some("cassandra"))(res.options.get("spark.cassandra.auth.username"))
    assertResult(Some("cassandra"))(res.options.get("spark.cassandra.auth.password"))
  }

  test("Can convert UI connection mapping for Cassandra with keyspace and table") {
    val dataSourceRequest = DataSourceRequest("cassandra-name", "task-1", Some(CASSANDRA_NAME), Some(Map(URL -> "localhost:9092", USERNAME -> "cassandra", PASSWORD -> "cassandra", CASSANDRA_KEYSPACE -> "account", CASSANDRA_TABLE -> "accounts")))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult("cassandra-name")(res.dataSourceName)
    assertResult(10)(res.options.size)
    assertResult(Some(CASSANDRA))(res.options.get(FORMAT))
    assertResult(Some("localhost:9092"))(res.options.get(URL))
    assertResult(Some("account"))(res.options.get(CASSANDRA_KEYSPACE))
    assertResult(Some("accounts"))(res.options.get(CASSANDRA_TABLE))
  }

  test("Throw error if only keyspace or table is defined for Cassandra") {
    val dataSourceRequest = DataSourceRequest("cassandra-name", "task-1", Some(CASSANDRA_NAME), Some(Map(URL -> "localhost:9092", USERNAME -> "cassandra", PASSWORD -> "cassandra", CASSANDRA_KEYSPACE -> "account")))
    val dataSourceRequest1 = DataSourceRequest("cassandra-name", "task-1", Some(CASSANDRA_NAME), Some(Map(URL -> "localhost:9092", USERNAME -> "cassandra", PASSWORD -> "cassandra", CASSANDRA_TABLE -> "accounts")))
    assertThrows[IllegalArgumentException](ConnectionMapper.connectionMapping(dataSourceRequest))
    assertThrows[IllegalArgumentException](ConnectionMapper.connectionMapping(dataSourceRequest1))
  }

  test("Can convert UI connection mapping for Postgres") {
    val dataSourceRequest = DataSourceRequest("postgres-name", "task-1", Some(POSTGRES), Some(Map(URL -> "localhost:5432", USERNAME -> "postgres", PASSWORD -> "postgres", DRIVER -> POSTGRES_DRIVER)))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult("postgres-name")(res.dataSourceName)
    assertResult(5)(res.options.size)
    assertResult(Some(JDBC))(res.options.get(FORMAT))
    assertResult(Some(POSTGRES_DRIVER))(res.options.get(DRIVER))
    assertResult(Some("localhost:5432"))(res.options.get(URL))
    assertResult(Some("postgres"))(res.options.get(USERNAME))
    assertResult(Some("postgres"))(res.options.get(PASSWORD))
  }

  test("Can convert UI connection mapping for MySQL") {
    val dataSourceRequest = DataSourceRequest("mysql-name", "task-1", Some(MYSQL), Some(Map(URL -> "localhost:5432", USERNAME -> "root", PASSWORD -> "root", DRIVER -> MYSQL_DRIVER)))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult("mysql-name")(res.dataSourceName)
    assertResult(5)(res.options.size)
    assertResult(Some(JDBC))(res.options.get(FORMAT))
    assertResult(Some(MYSQL_DRIVER))(res.options.get(DRIVER))
    assertResult(Some("localhost:5432"))(res.options.get(URL))
    assertResult(Some("root"))(res.options.get(USERNAME))
    assertResult(Some("root"))(res.options.get(PASSWORD))
  }

  test("Can convert UI connection mapping for Postgres with schema and table") {
    val dataSourceRequest = DataSourceRequest("postgres-name", "task-1", Some(POSTGRES), Some(Map(URL -> "localhost:5432", USERNAME -> "postgres", PASSWORD -> "postgres", DRIVER -> POSTGRES_DRIVER, SCHEMA -> "account", TABLE -> "accounts")))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult("postgres-name")(res.dataSourceName)
    assertResult(7)(res.options.size)
    assertResult(Some(JDBC))(res.options.get(FORMAT))
    assertResult(Some(POSTGRES_DRIVER))(res.options.get(DRIVER))
    assertResult(Some("localhost:5432"))(res.options.get(URL))
    assertResult(Some("postgres"))(res.options.get(USERNAME))
    assertResult(Some("postgres"))(res.options.get(PASSWORD))
    assertResult(Some("account"))(res.options.get(SCHEMA))
    assertResult(Some("accounts"))(res.options.get(TABLE))
  }

  test("Throw error if only schema or table is defined for Postgres") {
    val dataSourceRequest = DataSourceRequest("postgres-name", "task-1", Some(POSTGRES), Some(Map(URL -> "localhost:5432", USERNAME -> "postgres", PASSWORD -> "postgres", DRIVER -> POSTGRES_DRIVER, SCHEMA -> "account")))
    val dataSourceRequest1 = DataSourceRequest("postgres-name", "task-1", Some(POSTGRES), Some(Map(URL -> "localhost:5432", USERNAME -> "postgres", PASSWORD -> "postgres", DRIVER -> POSTGRES_DRIVER, TABLE -> "accounts")))
    assertThrows[IllegalArgumentException](ConnectionMapper.connectionMapping(dataSourceRequest))
    assertThrows[IllegalArgumentException](ConnectionMapper.connectionMapping(dataSourceRequest1))
  }

  test("Can convert UI connection mapping for CSV") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", Some(CSV), Some(Map(PATH -> "/tmp/my-csv")))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(2)(res.options.size)
    assertResult(Some(CSV))(res.options.get(FORMAT))
    assertResult(Some("/tmp/my-csv"))(res.options.get(PATH))
  }

  test("Can convert UI connection mapping for CSV with partitions and partitionBy") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", Some(CSV), Some(Map(PATH -> "/tmp/my-csv", PARTITIONS -> "2", PARTITION_BY -> "account_id,year")))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(4)(res.options.size)
    assertResult(Some(CSV))(res.options.get(FORMAT))
    assertResult(Some("/tmp/my-csv"))(res.options.get(PATH))
    assertResult(Some("2"))(res.options.get(PARTITIONS))
    assertResult(Some("account_id,year"))(res.options.get(PARTITION_BY))
  }

  test("Can convert UI connection mapping for JSON") {
    val dataSourceRequest = DataSourceRequest("json-name", "task-1", Some(JSON), Some(Map(PATH -> "/tmp/my-json")))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(2)(res.options.size)
    assertResult(Some(JSON))(res.options.get(FORMAT))
    assertResult(Some("/tmp/my-json"))(res.options.get(PATH))
  }

  test("Can convert UI connection mapping for Parquet") {
    val dataSourceRequest = DataSourceRequest("parquet-name", "task-1", Some(PARQUET), Some(Map(PATH -> "/tmp/my-parquet")))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(2)(res.options.size)
    assertResult(Some(PARQUET))(res.options.get(FORMAT))
    assertResult(Some("/tmp/my-parquet"))(res.options.get(PATH))
  }

  test("Can convert UI connection mapping for ORC") {
    val dataSourceRequest = DataSourceRequest("orc-name", "task-1", Some(ORC), Some(Map(PATH -> "/tmp/my-orc")))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(2)(res.options.size)
    assertResult(Some(ORC))(res.options.get(FORMAT))
    assertResult(Some("/tmp/my-orc"))(res.options.get(PATH))
  }

  test("Can convert UI connection mapping for Solace") {
    val dataSourceRequest = DataSourceRequest("solace-name", "task-1", Some(SOLACE), Some(Map(URL -> "localhost:55554", USERNAME -> "solace", PASSWORD -> "solace", JMS_DESTINATION_NAME -> "/JNDI/my-queue", JMS_VPN_NAME -> "default", JMS_CONNECTION_FACTORY -> "jms-connection", JMS_INITIAL_CONTEXT_FACTORY -> "jms-init")))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(8)(res.options.size)
    assertResult(Some(JMS))(res.options.get(FORMAT))
    assertResult(Some("localhost:55554"))(res.options.get(URL))
    assertResult(Some("solace"))(res.options.get(USERNAME))
    assertResult(Some("solace"))(res.options.get(PASSWORD))
    assertResult(Some("/JNDI/my-queue"))(res.options.get(JMS_DESTINATION_NAME))
    assertResult(Some("default"))(res.options.get(JMS_VPN_NAME))
    assertResult(Some("jms-connection"))(res.options.get(JMS_CONNECTION_FACTORY))
    assertResult(Some("jms-init"))(res.options.get(JMS_INITIAL_CONTEXT_FACTORY))
  }

  test("Can convert UI connection mapping for Kafka") {
    val dataSourceRequest = DataSourceRequest("kafka-name", "task-1", Some(KAFKA), Some(Map(URL -> "localhost:1234", KAFKA_TOPIC -> "my-topic")))
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(4)(res.options.size)
    assertResult(Some(KAFKA))(res.options.get(FORMAT))
    assertResult(Some("localhost:1234"))(res.options.get(URL))
    assertResult(Some("localhost:1234"))(res.options.get("kafka.bootstrap.servers"))
    assertResult(Some("my-topic"))(res.options.get(KAFKA_TOPIC))
  }

  test("Can convert UI connection mapping for HTTP") {
    val dataSourceRequest = DataSourceRequest(
      "http-name",
      "task-1",
      Some(HTTP),
      Some(Map(USERNAME -> "root", PASSWORD -> "root"))
    )
    val res = ConnectionMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(3)(res.options.size)
    assertResult(Some(HTTP))(res.options.get(FORMAT))
    assertResult(Some("root"))(res.options.get(USERNAME))
    assertResult(Some("root"))(res.options.get(PASSWORD))
  }

  test("Throw exception if provided unknown data source") {
    val dataSourceRequest = DataSourceRequest("unknown-name", "task-1", Some("unknown"))
    assertThrows[IllegalArgumentException](ConnectionMapper.connectionMapping(dataSourceRequest))
  }

  test("Throw exception if no data source type provided") {
    val dataSourceRequest = DataSourceRequest("unknown-name", "task-1", None)
    assertThrows[IllegalArgumentException](ConnectionMapper.connectionMapping(dataSourceRequest))
  }
}
