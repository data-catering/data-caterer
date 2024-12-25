package io.github.datacatering.datacaterer.core.generator.metadata.datasource.database

import io.github.datacatering.datacaterer.api.model.Constants.{MAXIMUM, MINIMUM}
import org.apache.spark.sql.Row

case class MysqlMetadata(name: String, connectionConfig: Map[String, String]) extends JdbcMetadata {
  override val baseFilterSchema: List[String] = List("sys", "information_schema", "mysql", "performance_schema")

  //TODO: all is_unique might not be captured due to table only having singular primary key

  override def additionalFieldMetadataQuery: String =
    """SELECT c.table_schema                                                     AS "schema",
      |       c.table_name                                                       AS "table",
      |       c.field_name                                                      AS "field",
      |       c.data_type                                                        AS source_data_type,
      |       c.is_nullable                                                      AS is_nullable,
      |       c.character_maximum_length                                         AS character_maximum_length,
      |       c.numeric_precision                                                AS numeric_precision,
      |       c.numeric_scale                                                    AS numeric_scale,
      |       c.field_default                                                   AS field_default,
      |       CASE WHEN uc.constraint_type != 'PRIMARY' THEN 'YES' ELSE 'NO' END AS is_unique,
      |       CASE WHEN pc.constraint_type = 'PRIMARY' THEN 'YES' ELSE 'NO' END  AS is_primary_key,
      |       pc.seq_in_index                                                    AS primary_key_position,
      |       c.extra                                                            AS data_source_generation
      |FROM information_schema.fields AS c
      |         LEFT OUTER JOIN (SELECT stat.table_schema,
      |                                 stat.table_name,
      |                                 stat.field_name,
      |                                 stat.index_name AS constraint_type
      |                          FROM information_schema.statistics stat
      |                          WHERE index_name != 'PRIMARY') uc
      |                         ON uc.field_name = c.field_name AND uc.table_schema = c.table_schema AND
      |                            uc.table_name = c.table_name
      |         LEFT OUTER JOIN (SELECT stat1.table_schema,
      |                                 stat1.table_name,
      |                                 stat1.field_name,
      |                                 stat1.index_name AS constraint_type,
      |                                 stat1.seq_in_index
      |                          FROM information_schema.statistics stat1
      |                          WHERE index_name = 'PRIMARY') pc
      |                         ON pc.field_name = c.field_name AND pc.table_schema = c.table_schema AND
      |                            pc.table_name = c.table_name""".stripMargin

  override def foreignKeyQuery: String =
    """SELECT ke.referenced_table_schema                                        AS "schema",
      |       ke.referenced_table_name                                          AS "table",
      |       concat(ke.table_schema, '.', ke.table_name)                       AS dbtable,
      |       ke.field_name                                                    AS "field",
      |       concat(ke.referenced_table_schema, '.', ke.referenced_table_name) AS foreign_dbtable,
      |       ke.referenced_field_name                                         AS foreign_field
      |FROM information_schema.key_field_usage ke
      |WHERE ke.referenced_table_name IS NOT NULL
      |ORDER BY ke.referenced_table_name""".stripMargin

  override def dataSourceGenerationMetadata(row: Row): Map[String, String] = {
    //    val baseMetadata = Map(DATA_SOURCE_GENERATION -> dataSourceGeneration, OMIT -> "true")
    //    val sqlExpression = dataSourceGeneration.toLowerCase match {
    //      case "auto_increment" => "monotonically_increasing_id()"
    //      case "on update current_timestamp" => "now()"
    //      case _ => ""
    //    }
    //    baseMetadata ++ Map(EXPRESSION -> sqlExpression)
    val sourceDataType = row.getAs[String]("source_data_type")
    sourceDataType.toLowerCase match {
      case "tinyint" => Map(MINIMUM -> "-128", MAXIMUM -> "127")
      case "smallint" => Map(MINIMUM -> "-32768", MAXIMUM -> "32767")
      case "mediumint" => Map(MINIMUM -> "-8388608", MAXIMUM -> "8388607")
      case _ => Map.empty[String, String]
    }
  }
}
