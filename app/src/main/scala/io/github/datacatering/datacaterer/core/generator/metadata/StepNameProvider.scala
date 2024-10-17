package io.github.datacatering.datacaterer.core.generator.metadata

import io.github.datacatering.datacaterer.api.model.Constants.{CASSANDRA_KEYSPACE, CASSANDRA_TABLE, JDBC_TABLE, JMS_DESTINATION_NAME, METADATA_IDENTIFIER, PATH}
import io.github.datacatering.datacaterer.core.model.Constants.{REAL_TIME_ENDPOINT, REAL_TIME_METHOD_COL}

object StepNameProvider {

  def fromOptions(options: Map[String, String]): Option[String] = {
    if (options.contains(CASSANDRA_KEYSPACE) && options.contains(CASSANDRA_TABLE)) {
      Some(fromCassandra(options(CASSANDRA_KEYSPACE), options(CASSANDRA_TABLE)))
    } else if (options.contains(JDBC_TABLE)) {
      Some(fromJdbc(options(JDBC_TABLE)))
    } else if (options.contains(REAL_TIME_METHOD_COL) && options.contains(REAL_TIME_ENDPOINT)) {
      Some(fromHttp(options(REAL_TIME_METHOD_COL), options(REAL_TIME_ENDPOINT)))
    } else if (options.contains(PATH)) {
      Some(options(PATH))
    } else if (options.contains(JMS_DESTINATION_NAME)) {
      Some(options(JMS_DESTINATION_NAME))
    } else if (options.contains(METADATA_IDENTIFIER)) {
      Some(options(METADATA_IDENTIFIER))
    } else {
      None
    }
  }

  def fromHttp(method: String, path: String): String = {
    s"$method$path"
  }

  def fromCassandra(keyspace: String, table: String): String = {
    s"${keyspace}_$table"
  }

  def fromJdbc(dbTable: String): String = {
    val dbTableSpt = dbTable.split("\\.")
    assert(dbTableSpt.length == 2, s"JDBC table definition should contain only a single '.' with format '<schema>.<table>', table=$dbTable")
    s"${dbTableSpt.head}_${dbTableSpt.last}"
  }

}
