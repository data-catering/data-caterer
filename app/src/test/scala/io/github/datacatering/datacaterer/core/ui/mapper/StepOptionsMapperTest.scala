package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.model.Constants._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StepOptionsMapperTest extends AnyFunSuite with Matchers {

  test("JDBC: should convert schema and table to dbtable") {
    val options = Map("schema" -> "public", "table" -> "users")
    val result = StepOptionsMapper.mapStepOptions(options, JDBC)
    
    result should contain key JDBC_TABLE
    result(JDBC_TABLE) should be ("public.users")
    result.contains("schema") should be (false)
    result.contains("table") should be (false)
  }

  test("JDBC: should convert table only to dbtable when schema is missing") {
    val options = Map("table" -> "users")
    val result = StepOptionsMapper.mapStepOptions(options, JDBC)
    
    result should contain key JDBC_TABLE
    result(JDBC_TABLE) should be ("users")
    result.contains("table") should be (false)
  }

  test("JDBC: should preserve dbtable if already provided") {
    val options = Map(JDBC_TABLE -> "public.users", "schema" -> "other", "table" -> "other_table")
    val result = StepOptionsMapper.mapStepOptions(options, JDBC)
    
    result should contain key JDBC_TABLE
    result(JDBC_TABLE) should be ("public.users")
  }

  test("JDBC: should preserve other options") {
    val options = Map(
      "schema" -> "public",
      "table" -> "users",
      "url" -> "jdbc:postgresql://localhost:5432/test",
      "user" -> "postgres",
      "password" -> "secret"
    )
    val result = StepOptionsMapper.mapStepOptions(options, JDBC)
    
    result should contain key JDBC_TABLE
    result(JDBC_TABLE) should be ("public.users")
    result should contain key "url"
    result should contain key "user"
    result should contain key "password"
  }

  test("JDBC: should handle empty schema") {
    val options = Map("schema" -> "", "table" -> "users")
    val result = StepOptionsMapper.mapStepOptions(options, JDBC)
    
    result should contain key JDBC_TABLE
    result(JDBC_TABLE) should be (".users")
  }

  test("JDBC: should not modify options when neither schema, table, nor dbtable provided") {
    val options = Map("url" -> "jdbc:postgresql://localhost:5432/test")
    val result = StepOptionsMapper.mapStepOptions(options, JDBC)
    
    result should be (options)
  }

  test("MySQL: should convert schema and table to dbtable") {
    val options = Map("schema" -> "mydb", "table" -> "customers")
    val result = StepOptionsMapper.mapStepOptions(options, JDBC)
    
    result should contain key JDBC_TABLE
    result(JDBC_TABLE) should be ("mydb.customers")
  }

  test("Cassandra: should preserve keyspace and table") {
    val options = Map(CASSANDRA_KEYSPACE -> "my_keyspace", CASSANDRA_TABLE -> "my_table")
    val result = StepOptionsMapper.mapStepOptions(options, CASSANDRA)
    
    result should contain key CASSANDRA_KEYSPACE
    result should contain key CASSANDRA_TABLE
    result(CASSANDRA_KEYSPACE) should be ("my_keyspace")
    result(CASSANDRA_TABLE) should be ("my_table")
  }

  test("Iceberg: should preserve table option") {
    val options = Map(TABLE -> "database.table_name")
    val result = StepOptionsMapper.mapStepOptions(options, ICEBERG)
    
    result should contain key TABLE
    result(TABLE) should be ("database.table_name")
  }

  test("BigQuery: should preserve table option") {
    val options = Map(TABLE -> "project.dataset.table_name")
    val result = StepOptionsMapper.mapStepOptions(options, BIGQUERY)
    
    result should contain key TABLE
    result(TABLE) should be ("project.dataset.table_name")
  }

  test("CSV: should not modify options") {
    val options = Map("path" -> "/tmp/data.csv", "header" -> "true")
    val result = StepOptionsMapper.mapStepOptions(options, CSV)
    
    result should be (options)
  }

  test("JSON: should not modify options") {
    val options = Map("path" -> "/tmp/data.json")
    val result = StepOptionsMapper.mapStepOptions(options, JSON)
    
    result should be (options)
  }

  test("Parquet: should not modify options") {
    val options = Map("path" -> "/tmp/data.parquet")
    val result = StepOptionsMapper.mapStepOptions(options, PARQUET)
    
    result should be (options)
  }

  test("Delta: should not modify options") {
    val options = Map("path" -> "/tmp/delta-table")
    val result = StepOptionsMapper.mapStepOptions(options, DELTA)
    
    result should be (options)
  }

  test("ORC: should not modify options") {
    val options = Map("path" -> "/tmp/data.orc")
    val result = StepOptionsMapper.mapStepOptions(options, ORC)
    
    result should be (options)
  }

  test("HTTP: should not modify options") {
    val options = Map("method" -> "POST", "endpoint" -> "/api/data")
    val result = StepOptionsMapper.mapStepOptions(options, HTTP)
    
    result should be (options)
  }

  test("Kafka: should not modify options") {
    val options = Map("topic" -> "my-topic", "url" -> "localhost:9092")
    val result = StepOptionsMapper.mapStepOptions(options, KAFKA)
    
    result should be (options)
  }

  test("Unknown format: should not modify options") {
    val options = Map("key" -> "value")
    val result = StepOptionsMapper.mapStepOptions(options, "unknown-format")
    
    result should be (options)
  }
}
