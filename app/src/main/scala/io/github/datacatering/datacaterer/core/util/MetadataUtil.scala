package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{Field, MetadataConfig, Step}
import io.github.datacatering.datacaterer.api.util.ConfigUtil
import io.github.datacatering.datacaterer.core.exception.UnsupportedDataFormatForTrackingException
import io.github.datacatering.datacaterer.core.generator.metadata.ExpressionPredictor
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.DataSourceMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.CassandraMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.confluentschemaregistry.ConfluentSchemaRegistryMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.datacontractcli.DataContractCliDataSourceMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.file.FileMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.GreatExpectationsDataSourceMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.http.HttpMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jms.JmsMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.JsonSchemaDataSourceMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.MysqlMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.openlineage.OpenLineageMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata.OpenMetadataDataSourceMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard.OpenDataContractStandardDataSourceMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.PostgresMetadata
import io.github.datacatering.datacaterer.core.util.ValidationUtil.cleanValidationIdentifier
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, Metadata, MetadataBuilder, ShortType, StringType, StructField, StructType, TimestampType, ArrayType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

import scala.util.{Failure, Success, Try}
import io.github.datacatering.datacaterer.api.model.Step

object MetadataUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper
  private val mapStringToAnyClass = Map[String, Any]()
  private val TEMP_CACHED_TABLE_NAME = "__temp_table"
  implicit private val fieldMetadataEncoder: Encoder[FieldMetadata] = Encoders.kryo[FieldMetadata]

  def metadataToMap(metadata: Metadata): Map[String, Any] = {
    OBJECT_MAPPER.readValue(metadata.json, mapStringToAnyClass.getClass)
  }

  def mapToMetadata(mapMetadata: Map[String, Any]): Metadata = {
    Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(mapMetadata))
  }

  /**
   * Map to StructFields with ExpressionPredictor applied - used for sink/generation phase
   */
  def mapToStructFields(
                         sourceData: DataFrame,
                         dataSourceReadOptions: Map[String, String],
                         fieldDataProfilingMetadata: List[DataProfilingMetadata]
                       )(implicit sparkSession: SparkSession): Array[StructField] = {
    mapToStructFieldsWithPredictions(sourceData, dataSourceReadOptions, fieldDataProfilingMetadata, sparkSession.emptyDataset[FieldMetadata])
  }

  /**
   * Map to StructFields with ExpressionPredictor applied - used for sink/generation phase
   */
  def mapToStructFields(sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                        fieldDataProfilingMetadata: List[DataProfilingMetadata],
                        additionalFieldMetadata: Dataset[FieldMetadata])(implicit sparkSession: SparkSession): Array[StructField] = {
    mapToStructFieldsWithPredictions(sourceData, dataSourceReadOptions, fieldDataProfilingMetadata, additionalFieldMetadata)
  }

  /**
   * Map to StructFields for metadata extraction phase - NO ExpressionPredictor applied
   */
  def mapToStructFieldsForMetadataExtraction(additionalFieldMetadata: Dataset[FieldMetadata], dataSourceReadOptions: Map[String, String]): Array[StructField] = {
    val colsMetadata = additionalFieldMetadata.collect()
    val targetMetadataId = dataSourceReadOptions.getOrElse(METADATA_IDENTIFIER, "")
    
    val matchedColMetadata = colsMetadata.filter(fm => {
      val fieldMetadataId = fm.dataSourceReadOptions.getOrElse(METADATA_IDENTIFIER, "")
      fieldMetadataId.equals(targetMetadataId)
    })
    
    getStructFieldsForMetadataExtraction(matchedColMetadata)
  }

  /**
   * Map to StructFields for metadata extraction phase - NO ExpressionPredictor applied
   */
  def mapToStructFieldsForMetadataExtraction(sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                        fieldDataProfilingMetadata: List[DataProfilingMetadata],
                        additionalFieldMetadata: Dataset[FieldMetadata])(implicit sparkSession: SparkSession): Array[StructField] = {
    
    val dataSourceAdditionalFieldMetadata = additionalFieldMetadata.filter(_.dataSourceReadOptions.equals(dataSourceReadOptions))
    val primaryKeys = dataSourceAdditionalFieldMetadata.filter(_.metadata.get(IS_PRIMARY_KEY).exists(_.toBoolean))
    val primaryKeyCount = primaryKeys.count()

    val fieldsWithMetadata = sourceData.schema.fields.map(field => {
      val baseMetadata = new MetadataBuilder().withMetadata(field.metadata)
      fieldDataProfilingMetadata.find(_.fieldName == field.name).foreach(c => baseMetadata.withMetadata(mapToMetadata(c.metadata)))

      var nullable = field.nullable
      val optFieldAdditionalMetadata = dataSourceAdditionalFieldMetadata.filter(_.field == field.name)
      if (!optFieldAdditionalMetadata.isEmpty) {
        val fieldAdditionalMetadata = optFieldAdditionalMetadata.first()
        fieldAdditionalMetadata.metadata.foreach(m => {
          if (m._1.equals(IS_NULLABLE)) nullable = m._2.toBoolean
          baseMetadata.putString(m._1, String.valueOf(m._2))
        })
        val isPrimaryKey = fieldAdditionalMetadata.metadata.get(IS_PRIMARY_KEY).exists(_.toBoolean)
        if (isPrimaryKey && primaryKeyCount == 1) {
          baseMetadata.putString(IS_UNIQUE, "true")
        }
      }
      val updatedField = StructField(field.name, field.dataType, nullable, baseMetadata.build())
      // NO ExpressionPredictor applied here - this is metadata extraction phase
      updatedField
    })

    if (sparkSession.catalog.tableExists(TEMP_CACHED_TABLE_NAME)) {
      sparkSession.catalog.uncacheTable(TEMP_CACHED_TABLE_NAME)
    }
    additionalFieldMetadata.unpersist()
    fieldsWithMetadata
  }

  /**
   * PRIVATE: Map to StructFields with ExpressionPredictor applied - used for sink/generation phase
   */
  private def mapToStructFieldsWithPredictions(sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                        fieldDataProfilingMetadata: List[DataProfilingMetadata],
                        additionalFieldMetadata: Dataset[FieldMetadata])(implicit sparkSession: SparkSession): Array[StructField] = {
    
    val dataSourceAdditionalFieldMetadata = additionalFieldMetadata.filter(_.dataSourceReadOptions.equals(dataSourceReadOptions))
    val primaryKeys = dataSourceAdditionalFieldMetadata.filter(_.metadata.get(IS_PRIMARY_KEY).exists(_.toBoolean))
    val primaryKeyCount = primaryKeys.count()

    val fieldsWithMetadata = sourceData.schema.fields.map(field => {
      val baseMetadata = new MetadataBuilder().withMetadata(field.metadata)
      fieldDataProfilingMetadata.find(_.fieldName == field.name).foreach(c => baseMetadata.withMetadata(mapToMetadata(c.metadata)))

      var nullable = field.nullable
      val optFieldAdditionalMetadata = dataSourceAdditionalFieldMetadata.filter(_.field == field.name)
      if (!optFieldAdditionalMetadata.isEmpty) {
        val fieldAdditionalMetadata = optFieldAdditionalMetadata.first()
        fieldAdditionalMetadata.metadata.foreach(m => {
          if (m._1.equals(IS_NULLABLE)) nullable = m._2.toBoolean
          baseMetadata.putString(m._1, String.valueOf(m._2))
        })
        val isPrimaryKey = fieldAdditionalMetadata.metadata.get(IS_PRIMARY_KEY).exists(_.toBoolean)
        if (isPrimaryKey && primaryKeyCount == 1) {
          baseMetadata.putString(IS_UNIQUE, "true")
        }
      }
      val updatedField = StructField(field.name, field.dataType, nullable, baseMetadata.build())
      ExpressionPredictor.getFieldPredictions(updatedField)
    })

    if (sparkSession.catalog.tableExists(TEMP_CACHED_TABLE_NAME)) {
      sparkSession.catalog.uncacheTable(TEMP_CACHED_TABLE_NAME)
    }
    additionalFieldMetadata.unpersist()
    fieldsWithMetadata
  }

  /**
   * PRIVATE: Get StructFields for metadata extraction phase - NO ExpressionPredictor applied
   */
  private def getStructFieldsForMetadataExtraction(colsMetadata: Array[FieldMetadata]): Array[StructField] = {
    colsMetadata.map(colMetadata => {
      var dataType = DataType.fromDDL(colMetadata.metadata.getOrElse(FIELD_DATA_TYPE, DEFAULT_FIELD_TYPE))
      
      // Handle nested structures and arrays with proper metadata preservation
      if (colMetadata.nestedFields.nonEmpty) {
        if (dataType.typeName == "struct") {
          // For struct types, recursively process nested fields and preserve their metadata
          val nestedStructFields = getStructFieldsForMetadataExtraction(colMetadata.nestedFields.toArray)
          dataType = StructType(nestedStructFields)
        } else if (dataType.typeName == "array") {
          // For array types, check if the element type should be a struct with nested fields
          val arrayType = dataType.asInstanceOf[ArrayType]
          if (arrayType.elementType.typeName == "struct" || colMetadata.nestedFields.nonEmpty) {
            // Create a struct type from the nested fields for the array element
            val nestedStructFields = getStructFieldsForMetadataExtraction(colMetadata.nestedFields.toArray)
            val elementStructType = StructType(nestedStructFields)
            dataType = ArrayType(elementStructType, arrayType.containsNull)
          }
        }
      }
      
      val nullable = colMetadata.metadata.get(IS_NULLABLE).map(_.toBoolean).getOrElse(DEFAULT_FIELD_NULLABLE)
      val metadata = mapToMetadata(colMetadata.metadata)
      val baseField = StructField(colMetadata.field, dataType, nullable, metadata)
      // NO ExpressionPredictor applied here - this is metadata extraction phase
      baseField
    })
  }

  def getFieldDataProfilingMetadata(
                                     sourceData: DataFrame,
                                     dataSourceReadOptions: Map[String, String],
                                     dataSourceMetadata: DataSourceMetadata,
                                     metadataConfig: MetadataConfig
                                   )(implicit sparkSession: SparkSession): List[DataProfilingMetadata] = {
    val dataSourceFormat = dataSourceReadOptions.getOrElse(FORMAT, "csv")
    computeFieldStatistics(sourceData, dataSourceReadOptions, dataSourceMetadata.name, dataSourceMetadata.format)
    val fieldLevelStatistics = sparkSession.sharedState.cacheManager.lookupCachedData(sourceData).get.cachedRepresentation.stats
    val rowCount = fieldLevelStatistics.rowCount.getOrElse(BigInt(0))
    LOGGER.debug(s"Computed metadata statistics for data source, name=${dataSourceMetadata.name}, format=$dataSourceFormat, " +
      s"details=${ConfigUtil.cleanseOptions(dataSourceReadOptions)}, rows-analysed=$rowCount, size-in-bytes=${fieldLevelStatistics.sizeInBytes}, " +
      s"num-fields-analysed=${fieldLevelStatistics.attributeStats.size}")

    fieldLevelStatistics.attributeStats.map(x => {
      val fieldName = x._1.name
      val statisticsMap = columnStatToMap(x._2.toCatalogColumnStat(fieldName, x._1.dataType)) ++ Map(ROW_COUNT -> rowCount.toString)
      val optOneOfField = determineIfOneOfField(sourceData, fieldName, statisticsMap, metadataConfig)
      val optionalMetadataMap = optOneOfField.map(oneOf => Map(ONE_OF_GENERATOR -> oneOf)).getOrElse(Map())
      val statWithOptionalMetadata = statisticsMap ++ optionalMetadataMap

      LOGGER.debug(s"Field summary statistics, name=${dataSourceMetadata.name}, format=$dataSourceFormat, field-name=$fieldName, " +
        s"statistics=${statWithOptionalMetadata - s"$fieldName.$HISTOGRAM"}")
      DataProfilingMetadata(fieldName, statWithOptionalMetadata)
    }).toList
  }

  private def computeFieldStatistics(
                                      sourceData: DataFrame,
                                      dataSourceReadOptions: Map[String, String],
                                      dataSourceName: String,
                                      dataSourceFormat: String
                                    )(implicit sparkSession: SparkSession): Unit = {
    //have to create temp view then analyze the field stats which can be found in the cached data
    sourceData.createOrReplaceTempView(TEMP_CACHED_TABLE_NAME)
    if (!sparkSession.catalog.isCached(TEMP_CACHED_TABLE_NAME)) sparkSession.catalog.cacheTable(TEMP_CACHED_TABLE_NAME)
    val cleansedOptions = ConfigUtil.cleanseOptions(dataSourceReadOptions)
    val optFieldsToAnalyze = Some(sourceData.schema.fields.filter(f => analyzeSupportsType(f.dataType)).map(_.name).toSeq)
    val tryAnalyzeData = Try(AnalyzeColumnCommand(TableIdentifier(TEMP_CACHED_TABLE_NAME), optFieldsToAnalyze, false).run(sparkSession))
    tryAnalyzeData match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to analyze all fields in data source, name=$dataSourceName, format=$dataSourceFormat, " +
          s"options=$cleansedOptions, error-message=${exception.getMessage}")
      case Success(_) =>
        LOGGER.debug(s"Successfully analyzed all fields in data source, name=$dataSourceName, " +
          s"format=$dataSourceFormat, options=$cleansedOptions")
    }
  }

  def determineIfOneOfField(
                             sourceData: DataFrame,
                             fieldName: String,
                             statisticsMap: Map[String, String],
                             metadataConfig: MetadataConfig
                           ): Option[Array[String]] = {
    val fieldDataType = sourceData.schema.fields.find(_.name == fieldName).map(_.dataType)
    val count = statisticsMap(ROW_COUNT).toLong
    (fieldDataType, count) match {
      case (Some(DateType), _) => None
      case (_, 0) => None
      case (Some(_), c) if c >= metadataConfig.oneOfMinCount =>
        val distinctCount = statisticsMap(DISTINCT_COUNT).toDouble
        if (distinctCount / count <= metadataConfig.oneOfDistinctCountVsCountThreshold) {
          LOGGER.debug(s"Identified field as a 'oneOf' field as distinct count / total count is below threshold, threshold=${metadataConfig.oneOfDistinctCountVsCountThreshold}")
          Some(sourceData.select(fieldName).distinct().collect().map(_.mkString))
        } else {
          None
        }
      case _ => None
    }
  }

  def getSubDataSourcePath(
                            dataSourceName: String,
                            planName: String,
                            step: Step,
                            recordTrackingFolderPath: String
                          ): String = {
    val format = step.options(FORMAT)
    val lowerFormat = format.toLowerCase
    val subPath = lowerFormat match {
      case JDBC =>
        val spt = step.options(JDBC_TABLE).split("\\.")
        val (schema, table) = (spt.head, spt.last)
        s"$schema/$table"
      case CASSANDRA =>
        s"${step.options(CASSANDRA_KEYSPACE)}/${step.options(CASSANDRA_TABLE)}"
      case PARQUET | CSV | JSON | DELTA | ORC | DELTA | ICEBERG =>
        step.options(PATH).replaceAll("s3(a|n?)://|wasb(s?)://|gs://|file://|hdfs://[a-zA-Z0-9]+:[0-9]+", "")
      case JMS | RABBITMQ | SOLACE =>
        step.options(JMS_DESTINATION_NAME)
      case KAFKA =>
        step.options(KAFKA_TOPIC)
      case HTTP =>
        cleanValidationIdentifier(step.name)
      case _ =>
        LOGGER.warn(s"Unsupported data format for record tracking, format=$lowerFormat")
        throw UnsupportedDataFormatForTrackingException(lowerFormat)
    }
    val basePath = s"$recordTrackingFolderPath/$planName/$lowerFormat/$dataSourceName/$subPath"
    basePath.replaceAll("//", "/")
  }

  def getMetadataFromConnectionConfig(connectionConfig: (String, Map[String, String])): Option[DataSourceMetadata] = {
    //TODO allow for getting metadata from multiple data sources
    val optExternalMetadataSource = connectionConfig._2.get(METADATA_SOURCE_TYPE)
    val format = connectionConfig._2.getOrElse(FORMAT, "").toLowerCase
    (optExternalMetadataSource, format) match {
      case (Some(metadataSourceType), _) =>
        metadataSourceType match {
          case MARQUEZ => Some(OpenLineageMetadata(connectionConfig._1, format, connectionConfig._2))
          case OPEN_API => Some(HttpMetadata(connectionConfig._1, format, connectionConfig._2))
          case OPEN_METADATA => Some(OpenMetadataDataSourceMetadata(connectionConfig._1, format, connectionConfig._2))
          case GREAT_EXPECTATIONS => Some(GreatExpectationsDataSourceMetadata(connectionConfig._1, format, connectionConfig._2))
          case OPEN_DATA_CONTRACT_STANDARD => Some(OpenDataContractStandardDataSourceMetadata(connectionConfig._1, format, connectionConfig._2))
          case DATA_CONTRACT_CLI => Some(DataContractCliDataSourceMetadata(connectionConfig._1, format, connectionConfig._2))
          case CONFLUENT_SCHEMA_REGISTRY => Some(ConfluentSchemaRegistryMetadata(connectionConfig._1, format, connectionConfig._2))
          case JSON_SCHEMA => Some(JsonSchemaDataSourceMetadata(connectionConfig._1, format, connectionConfig._2))
          case metadataSourceType =>
            LOGGER.warn(s"Unsupported external metadata source, connection-name=${connectionConfig._1}, metadata-source-type=$metadataSourceType")
            None
        }
      case (_, CASSANDRA) => Some(CassandraMetadata(connectionConfig._1, connectionConfig._2))
      case (_, JDBC) =>
        connectionConfig._2(DRIVER) match {
          case POSTGRES_DRIVER => Some(PostgresMetadata(connectionConfig._1, connectionConfig._2))
          case MYSQL_DRIVER => Some(MysqlMetadata(connectionConfig._1, connectionConfig._2))
          case driver =>
            LOGGER.warn(s"Metadata extraction not supported for JDBC driver type '$driver', connection-name=${connectionConfig._1}")
            None
        }
      case (_, CSV | JSON | PARQUET | DELTA | ORC | ICEBERG) => Some(FileMetadata(connectionConfig._1, format, connectionConfig._2))
      case (_, HTTP) => Some(HttpMetadata(connectionConfig._1, format, connectionConfig._2))
      case (_, JMS) => Some(JmsMetadata(connectionConfig._1, format, connectionConfig._2))
      case _ =>
        if (format.nonEmpty) {
          LOGGER.warn(s"Metadata extraction not supported for connection type '$format', connection-name=${connectionConfig._1}")
        }
        None
    }
  }

  def getFieldMetadata(
                        dataSourceName: String,
                        df: DataFrame,
                        connectionConfig: Map[String, String],
                        metadataConfig: MetadataConfig
                      )(implicit sparkSession: SparkSession): Array[Field] = {
    val optDataSourceMetadata = getMetadataFromConnectionConfig((dataSourceName, connectionConfig))
    if (optDataSourceMetadata.isDefined) {
      val fieldMetadata = getFieldDataProfilingMetadata(df, connectionConfig, optDataSourceMetadata.get, metadataConfig)
      val structFields = mapToStructFields(df, connectionConfig, fieldMetadata)
      structFields.map(FieldHelper.fromStructField)
    } else {
      LOGGER.warn(s"No metadata found for data source name, data-source-name=$dataSourceName")
      Array()
    }
  }

  private def analyzeSupportsType(dataType: DataType): Boolean = dataType match {
    case IntegerType | ShortType | LongType | DecimalType() | DoubleType | FloatType => true
    case BooleanType => true
    case BinaryType | StringType => true
    case TimestampType | DateType => true
    case _ => false
  }

  /**
   * Rename Spark field statistics to be aligned with Data Caterer statistic names. Remove 'version'
   *
   * @param catalogColumnStat Spark field statistics
   * @return Map of statistics for field
   */
  private def columnStatToMap(catalogColumnStat: CatalogColumnStat): Map[String, String] = {
    catalogColumnStat.toMap("col")
      .map(kv => {
        val baseStatName = kv._1.replaceFirst("col\\.", "")
        if (baseStatName.equalsIgnoreCase("minvalue")) {
          ("min", kv._2)
        } else if (baseStatName.equalsIgnoreCase("maxvalue")) {
          ("max", kv._2)
        } else (baseStatName, kv._2)
      })
      .filter(_._1 != "version")
  }
}


case class DataProfilingMetadata(fieldName: String, metadata: Map[String, Any], nestedProfiling: List[DataProfilingMetadata] = List())
