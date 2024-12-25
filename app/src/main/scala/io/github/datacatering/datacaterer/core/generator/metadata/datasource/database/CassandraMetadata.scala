package io.github.datacatering.datacaterer.core.generator.metadata.datasource.database

import io.github.datacatering.datacaterer.api.model.Constants.{CASSANDRA, CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CLUSTERING_POSITION, IS_NULLABLE, IS_PRIMARY_KEY, PRIMARY_KEY_POSITION, SOURCE_FIELD_DATA_TYPE}
import io.github.datacatering.datacaterer.core.model.ForeignKeyRelationship
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, desc, row_number}
import org.apache.spark.sql.{Dataset, SparkSession}

case class CassandraMetadata(name: String, connectionConfig: Map[String, String]) extends DatabaseMetadata {

  override val format: String = CASSANDRA

  override val metadataTable: Map[String, String] = Map(CASSANDRA_KEYSPACE -> "system_schema", CASSANDRA_TABLE -> "tables")

  override val selectExpr: List[String] = List("keyspace_name AS schema", "table_name AS table", "'BASE TABLE' AS table_type")

  override val baseFilterSchema: List[String] = List("system", "system_traces", "system_auth", "system_schema", "system_distributed", "system_backups",
    "dse_security", "dse_system_local", "dse_system", "dse_leases", "dse_insights", "dse_insights_local", "dse_perf", "solr_admin")

  override def getTableDataOptions(schema: String, table: String): Map[String, String] = {
    Map(CASSANDRA_KEYSPACE -> schema, CASSANDRA_TABLE -> table)
  }

  override def getAdditionalFieldMetadata(implicit sparkSession: SparkSession): Dataset[FieldMetadata] = {
    val cassandraFieldMetadata = sparkSession
      .read
      .format(CASSANDRA)
      .options(connectionConfig)
      .options(Map(CASSANDRA_KEYSPACE -> "system_schema", CASSANDRA_TABLE -> "fields"))
      .load()
      .selectExpr("keyspace_name AS schema", "table_name AS table", "'BASE TABLE' AS table_type", "field_name AS field",
        "type AS source_data_type", "clustering_order", "kind AS primary_key_type", "position AS primary_key_position")
    val filteredMetadata = createFilterQuery.map(cassandraFieldMetadata.filter).getOrElse(cassandraFieldMetadata)

    val metadataWithPrimaryKeyPosition = filteredMetadata
      .filter("primary_key_type != 'regular'")
      .select(col("schema"), col("table"), col("field"), col("primary_key_type"),
        col("source_data_type"), col("clustering_order"),
        row_number()
          .over(
            Window.partitionBy("schema", "table")
              .orderBy(desc("primary_key_type"), asc("primary_key_position"))
          ).as("primary_key_position")
      )

    metadataWithPrimaryKeyPosition
      .map(r => {
        val isPrimaryKey = if (r.getAs[String]("primary_key_type") != "regular") "true" else "false"
        val isNullable = if (isPrimaryKey.equalsIgnoreCase("true")) "false" else "true"
        val clusteringOrder = r.getAs[String]("clustering_order")

        val metadata = Map(
          SOURCE_FIELD_DATA_TYPE -> r.getAs[String]("source_data_type"),
          IS_PRIMARY_KEY -> isPrimaryKey,
          PRIMARY_KEY_POSITION -> r.getAs[String]("primary_key_position"),
          IS_NULLABLE -> isNullable,
        )
        val metadataWithClusterOrder = if (clusteringOrder != "none") metadata ++ Map(CLUSTERING_POSITION -> clusteringOrder) else metadata

        val dataSourceReadOptions = Map(CASSANDRA_KEYSPACE -> r.getAs[String]("schema"), CASSANDRA_TABLE -> r.getAs[String]("table"))
        FieldMetadata(r.getAs[String]("field"), dataSourceReadOptions, metadataWithClusterOrder)
      })
  }

  override def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = {
    sparkSession.emptyDataset[ForeignKeyRelationship]
  }
}
