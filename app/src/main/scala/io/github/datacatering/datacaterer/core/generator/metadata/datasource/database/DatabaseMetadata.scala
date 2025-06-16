package io.github.datacatering.datacaterer.core.generator.metadata.datasource.database

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_VALUE, FIELD_DATA_TYPE, IS_NULLABLE, IS_PRIMARY_KEY, IS_UNIQUE, JDBC, JDBC_QUERY, JDBC_TABLE, MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, PRIMARY_KEY_POSITION, SOURCE_FIELD_DATA_TYPE}
import io.github.datacatering.datacaterer.api.model.{ForeignKeyRelation, StringType, StructType}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import io.github.datacatering.datacaterer.core.model.Constants.{METADATA_FILTER_OUT_SCHEMA, METADATA_FILTER_OUT_TABLE}
import io.github.datacatering.datacaterer.core.model.ForeignKeyRelationship
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

trait DatabaseMetadata extends DataSourceMetadata {
  val metadataTable: Map[String, String]
  val selectExpr: List[String]
  val baseFilterSchema: List[String] = List()
  val baseFilterTable: List[String] = List()

  override val hasSourceData: Boolean = true

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    val allDatabaseSchemasWithTableName = sparkSession.read
      .format(format)
      .options(connectionConfig ++ metadataTable)
      .load()
      .selectExpr(selectExpr: _*)
    val baseTableFilter = "table_type = 'BASE TABLE'"
    val filteredSchemasAndTables = createFilterQuery
      .map(f => s"$f AND $baseTableFilter")
      .orElse(Some(baseTableFilter))
      .map(fq => allDatabaseSchemasWithTableName.filter(fq))
      .getOrElse(allDatabaseSchemasWithTableName)
    // have to collect here due to being unable to use encoder for DataType and Metadata from Spark. Should be okay given data size is small
    filteredSchemasAndTables.collect()
      .map(r => SubDataSourceMetadata(getTableDataOptions(r.getAs[String]("schema"), r.getAs[String]("table"))))
  }

  def getTableDataOptions(schema: String, table: String): Map[String, String]

  def createFilterQuery: Option[String] = {
    val filterSchema = if (connectionConfig.contains(METADATA_FILTER_OUT_SCHEMA)) {
      val configFilterSchema = connectionConfig(METADATA_FILTER_OUT_SCHEMA).split(",").map(_.trim).toList
      configFilterSchema ++ baseFilterSchema
    } else baseFilterSchema
    val filterTable = if (connectionConfig.contains(METADATA_FILTER_OUT_TABLE)) {
      val configFilterTable = connectionConfig(METADATA_FILTER_OUT_TABLE).split(",").map(_.trim).toList
      configFilterTable ++ baseFilterTable
    } else baseFilterTable

    val filterSchemaQuery = filterSchema.map(s => s"schema != '$s'").mkString(" AND ")
    val filterTableQuery = filterTable.map(t => s"table != '$t'").mkString(" AND ")
    (filterSchemaQuery.nonEmpty, filterTableQuery.nonEmpty) match {
      case (true, true) => Some(s"$filterSchemaQuery AND $filterTableQuery")
      case (true, false) => Some(filterSchemaQuery)
      case (false, true) => Some(filterTableQuery)
      case _ => None
    }
  }
}

trait JdbcMetadata extends DatabaseMetadata {
  override val format: String = JDBC

  override val metadataTable: Map[String, String] = Map(JDBC_TABLE -> "information_schema.tables")

  override val selectExpr: List[String] = List("table_schema AS schema", "table_name AS table", "table_type")

  override def getTableDataOptions(schema: String, table: String): Map[String, String] = {
    Map(JDBC_TABLE -> s"$schema.$table")
  }

  override def createFilterQuery: Option[String] = {
    super.createFilterQuery
  }

  override def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = {
    val filteredForeignKeyData: DataFrame = runQuery(sparkSession, foreignKeyQuery)

    filteredForeignKeyData.map(r =>
      ForeignKeyRelationship(
        new ForeignKeyRelation(name,
          toStepName(Map(JDBC_TABLE -> r.getAs[String]("foreign_dbtable"))),
          r.getAs[String]("foreign_column")
        ),
        new ForeignKeyRelation(name,
          toStepName(Map(JDBC_TABLE -> r.getAs[String]("dbtable"))),
          r.getAs[String]("column")
        )
      )
    )
  }

  override def getAdditionalFieldMetadata(implicit sparkSession: SparkSession): Dataset[FieldMetadata] = {
    val filteredTableConstraintData: DataFrame = runQuery(sparkSession, additionalFieldMetadataQuery)

    filteredTableConstraintData.map(r => {
      val isPrimaryKey = if (r.getAs[String]("is_primary_key").equalsIgnoreCase("yes")) "true" else "false"
      val isUnique = if (r.getAs[String]("is_unique").equalsIgnoreCase("yes")) "true" else "false"
      val isNullable = if (r.getAs[String]("is_nullable").equalsIgnoreCase("yes")) "true" else "false"

      val fieldMetadata = Map(
        SOURCE_FIELD_DATA_TYPE -> r.getAs[String]("source_data_type"),
        IS_PRIMARY_KEY -> isPrimaryKey,
        PRIMARY_KEY_POSITION -> r.getAs[String]("primary_key_position"),
        IS_UNIQUE -> isUnique,
        IS_NULLABLE -> isNullable,
        MAXIMUM_LENGTH -> r.getAs[String]("character_maximum_length"),
        NUMERIC_PRECISION -> r.getAs[String]("numeric_precision"),
        NUMERIC_SCALE -> r.getAs[String]("numeric_scale"),
        DEFAULT_VALUE -> r.getAs[String]("column_default")
      ).filter(m => m._2 != null) ++ dataSourceGenerationMetadata(r)

      val dataSourceReadOptions = Map(JDBC_TABLE -> s"${r.getAs[String]("schema")}.${r.getAs("table")}")
      FieldMetadata(r.getAs("column"), dataSourceReadOptions, fieldMetadata)
    })
  }

  /*
    Foreign key query requires the following return columns names to be returned:
    - schema
    - table
    - dbtable
    - column
    - foreign_dbtable
    - foreign_column
     */
  def foreignKeyQuery: String

  /*
  Additional column metadata query requires the following return columns names to be returned:
  - schema
  - table
  - column
  - source_data_type
  - is_nullable
  - character_maximum_length
  - numeric_precision
  - numeric_scale
  - column_default
  - is_unique
  - is_primary_key
  - primary_key_position
   */
  def additionalFieldMetadataQuery: String

  /*
  Given metadata from source database, further metadata could be extracted based on the generation of the fields data
  i.e. auto_increment means we can omit generating the values ourselves, so we add OMIT -> true into the metadata
   */
  def dataSourceGenerationMetadata(row: Row): Map[String, String]

  def runQuery(sparkSession: SparkSession, query: String): DataFrame = {
    val queryData = sparkSession.read
      .format(JDBC)
      .options(connectionConfig ++ Map(JDBC_QUERY -> query))
      .load()
    val optCreateFilterQuery = createFilterQuery
    if (optCreateFilterQuery.isDefined) {
      queryData.filter(optCreateFilterQuery.get)
    } else {
      queryData
    }
  }
}

case class FieldMetadata(
                          field: String,
                          dataSourceReadOptions: Map[String, String] = Map(),
                          metadata: Map[String, String] = Map(),
                          nestedFields: List[FieldMetadata] = List()
                         ) {
  def getNestedDataType: String = {
    val baseType = metadata.getOrElse(FIELD_DATA_TYPE, StringType.toString)
    if (nestedFields.isEmpty) {
      baseType
    } else {
      val innerType = nestedFields.map(c => {
        if (baseType.equalsIgnoreCase(StructType.toString)) {
          s"${c.field}: ${c.getNestedDataType}"
        } else {
          c.getNestedDataType
        }
      }).mkString(",")
      s"$baseType<$innerType>"
    }
  }
}
