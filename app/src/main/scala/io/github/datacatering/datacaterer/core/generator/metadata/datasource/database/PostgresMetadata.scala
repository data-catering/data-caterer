package io.github.datacatering.datacaterer.core.generator.metadata.datasource.database

import org.apache.spark.sql.Row

case class PostgresMetadata(name: String, connectionConfig: Map[String, String]) extends JdbcMetadata {
  override val baseFilterSchema: List[String] = List("pg_catalog", "information_schema")

  override def additionalFieldMetadataQuery: String =
    """SELECT c.table_schema                                                        AS "schema",
      |       c.table_name                                                          AS "table",
      |       c.column_name                                                         AS "column",
      |       c.data_type                                                           AS source_data_type,
      |       c.is_nullable                                                         AS is_nullable,
      |       c.character_maximum_length                                            AS character_maximum_length,
      |       c.numeric_precision                                                   AS numeric_precision,
      |       c.numeric_precision_radix                                             AS numeric_precision_radix,
      |       c.numeric_scale                                                       AS numeric_scale,
      |       c.column_default                                                      AS column_default,
      |       CASE WHEN uc.constraint_type = 'UNIQUE' THEN 'YES' ELSE 'NO' END      AS is_unique,
      |       CASE WHEN pc.constraint_type = 'PRIMARY KEY' THEN 'YES' ELSE 'NO' END AS is_primary_key,
      |       pc.ordinal_position                                                   AS primary_key_position
      |FROM information_schema.columns AS c
      |         LEFT OUTER JOIN (SELECT tc.constraint_name, tc.constraint_type, tc.table_schema, tc.table_name, ccu.column_name
      |                          FROM information_schema.table_constraints tc
      |                                   JOIN information_schema.constraint_column_usage ccu
      |                                        ON tc.constraint_name = ccu.constraint_name AND
      |                                           tc.table_schema = ccu.table_schema AND tc.table_name = ccu.table_name
      |                          WHERE constraint_type = 'UNIQUE') uc
      |                         ON uc.column_name = c.column_name AND uc.table_schema = c.table_schema AND uc.table_name = c.table_name
      |         LEFT OUTER JOIN (SELECT tc1.constraint_name, tc1.constraint_type, tc1.table_schema, tc1.table_name, kcu.column_name, kcu.ordinal_position
      |                          FROM information_schema.table_constraints tc1
      |                                   JOIN information_schema.key_column_usage kcu
      |                                        ON tc1.constraint_name = kcu.constraint_name AND
      |                                           tc1.table_schema = kcu.table_schema AND tc1.table_name = kcu.table_name
      |                          WHERE constraint_type = 'PRIMARY KEY') pc
      |                         ON pc.column_name = c.column_name AND pc.table_schema = c.table_schema AND pc.table_name = c.table_name""".stripMargin

  override def foreignKeyQuery: String =
    """SELECT tc.constraint_name,
      |       tc.table_schema                               AS "schema",
      |       tc.table_name                                 AS "table",
      |       concat(tc.table_schema, '.', tc.table_name)   AS dbtable,
      |       kcu.column_name                               AS "column",
      |       concat(ccu.table_schema, '.', ccu.table_name) AS foreign_dbtable,
      |       ccu.column_name                               AS foreign_column
      |FROM information_schema.table_constraints AS tc
      |         JOIN information_schema.key_column_usage AS kcu
      |              ON tc.constraint_name = kcu.constraint_name
      |                  AND tc.table_schema = kcu.table_schema
      |         JOIN information_schema.constraint_column_usage AS ccu
      |              ON ccu.constraint_name = tc.constraint_name
      |                  AND ccu.table_schema = tc.table_schema
      |WHERE tc.constraint_type = 'FOREIGN KEY'""".stripMargin

  override def dataSourceGenerationMetadata(row: Row): Map[String, String] = {
    Map()
  }
}
