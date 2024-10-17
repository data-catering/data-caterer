package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jms

import io.github.datacatering.datacaterer.api.model.Constants.{JMS_DESTINATION_NAME, SCHEMA_LOCATION}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import org.apache.spark.sql.SparkSession

case class JmsMetadata(name: String, format: String, connectionConfig: Map[String, String]) extends DataSourceMetadata {

  override val hasSourceData: Boolean = false

  override def toStepName(options: Map[String, String]): String = {
    options(JMS_DESTINATION_NAME)
  }

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    //TODO schema location can be either API endpoint or file, for now only allow file
    val schemaLocation = connectionConfig(SCHEMA_LOCATION)
    //could be JSON, Avro, Protobuf or XML schema definition
    //only avro and protobuf define schema with fields and types
    //find all avro/profobuf files under schema location directory

    //or example is defined and schema is learned from the example
    Array()
  }
}
