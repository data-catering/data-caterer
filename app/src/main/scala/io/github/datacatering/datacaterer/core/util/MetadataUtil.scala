package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{Field, MetadataConfig}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, Metadata, MetadataBuilder, ShortType, StringType, StructField, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object MetadataUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper
  private val mapStringToAnyClass = Map[String, Any]()
  private val TEMP_CACHED_TABLE_NAME = "__temp_table"

  def metadataToMap(metadata: Metadata): Map[String, Any] = {
    OBJECT_MAPPER.readValue(metadata.json, mapStringToAnyClass.getClass)
  }

  def mapToMetadata(mapMetadata: Map[String, Any]): Metadata = {
    Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(mapMetadata))
  }

  def mapToStructFields(sourceData: DataFrame, columnDataProfilingMetadata: List[DataProfilingMetadata])
                       (implicit sparkSession: SparkSession): Array[StructField] = {
    val fieldsWithMetadata = sourceData.schema.fields.map(field => {
      val baseMetadata = new MetadataBuilder().withMetadata(field.metadata)
      columnDataProfilingMetadata.find(_.columnName == field.name).foreach(c => baseMetadata.withMetadata(mapToMetadata(c.metadata)))
      field.copy(metadata = baseMetadata.build())
    })

    if (sparkSession.catalog.tableExists(TEMP_CACHED_TABLE_NAME)) {
      sparkSession.catalog.uncacheTable(TEMP_CACHED_TABLE_NAME)
    }
    fieldsWithMetadata
  }

  def getFieldDataProfilingMetadata(
                                     sourceData: DataFrame,
                                     dataSourceReadOptions: Map[String, String],
                                     dataSourceName: String,
                                     metadataConfig: MetadataConfig
                                   )(implicit sparkSession: SparkSession): List[DataProfilingMetadata] = {
    val dataSourceFormat = dataSourceReadOptions(FORMAT)
    computeColumnStatistics(sourceData, dataSourceReadOptions, dataSourceName, dataSourceFormat)
    val columnLevelStatistics = sparkSession.sharedState.cacheManager.lookupCachedData(sourceData).get.cachedRepresentation.stats
    val rowCount = columnLevelStatistics.rowCount.getOrElse(BigInt(0))
    LOGGER.info(s"Computed metadata statistics for data source, name=$dataSourceName, format=$dataSourceFormat, " +
      s"details=${ConfigUtil.cleanseOptions(dataSourceReadOptions)}, rows-analysed=$rowCount, size-in-bytes=${columnLevelStatistics.sizeInBytes}, " +
      s"num-columns-analysed=${columnLevelStatistics.attributeStats.size}")

    columnLevelStatistics.attributeStats.map(x => {
      val columnName = x._1.name
      val statisticsMap = columnStatToMap(x._2.toCatalogColumnStat(columnName, x._1.dataType)) ++ Map(ROW_COUNT -> rowCount.toString)
      val optOneOfColumn = determineIfOneOfColumn(sourceData, columnName, statisticsMap, metadataConfig)
      val optionalMetadataMap = optOneOfColumn.map(oneOf => Map(ONE_OF_GENERATOR -> oneOf)).getOrElse(Map())
      val statWithOptionalMetadata = statisticsMap ++ optionalMetadataMap

      LOGGER.debug(s"Column summary statistics, name=$dataSourceName, format=$dataSourceFormat, column-name=$columnName, " +
        s"statistics=${statWithOptionalMetadata - s"$columnName.$HISTOGRAM"}")
      DataProfilingMetadata(columnName, statWithOptionalMetadata)
    }).toList
  }

  private def computeColumnStatistics(
                                       sourceData: DataFrame,
                                       dataSourceReadOptions: Map[String, String],
                                       dataSourceName: String,
                                       dataSourceFormat: String
                                     )(implicit sparkSession: SparkSession): Unit = {
    //have to create temp view then analyze the column stats which can be found in the cached data
    sourceData.createOrReplaceTempView(TEMP_CACHED_TABLE_NAME)
    if (!sparkSession.catalog.isCached(TEMP_CACHED_TABLE_NAME)) sparkSession.catalog.cacheTable(TEMP_CACHED_TABLE_NAME)
    val cleansedOptions = ConfigUtil.cleanseOptions(dataSourceReadOptions)
    val optColumnsToAnalyze = Some(sourceData.schema.fields.filter(f => analyzeSupportsType(f.dataType)).map(_.name).toSeq)
    val tryAnalyzeData = Try(AnalyzeColumnCommand(TableIdentifier(TEMP_CACHED_TABLE_NAME), optColumnsToAnalyze, false).run(sparkSession))
    tryAnalyzeData match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to analyze all columns in data source, name=$dataSourceName, format=$dataSourceFormat, " +
          s"options=$cleansedOptions, error-message=${exception.getMessage}")
      case Success(_) =>
        LOGGER.debug(s"Successfully analyzed all columns in data source, name=$dataSourceName, " +
          s"format=$dataSourceFormat, options=$cleansedOptions")
    }
  }

  def determineIfOneOfColumn(
                              sourceData: DataFrame,
                              columnName: String,
                              statisticsMap: Map[String, String],
                              metadataConfig: MetadataConfig
                            ): Option[Array[String]] = {
    val columnDataType = sourceData.schema.fields.find(_.name == columnName).map(_.dataType)
    val count = statisticsMap(ROW_COUNT).toLong
    (columnDataType, count) match {
      case (Some(DateType), _) => None
      case (_, 0) => None
      case (Some(_), c) if c >= metadataConfig.oneOfMinCount =>
        val distinctCount = statisticsMap(DISTINCT_COUNT).toDouble
        if (distinctCount / count <= metadataConfig.oneOfDistinctCountVsCountThreshold) {
          LOGGER.debug(s"Identified column as a 'oneOf' column as distinct count / total count is below threshold, threshold=${metadataConfig.oneOfDistinctCountVsCountThreshold}")
          Some(sourceData.select(columnName).distinct().collect().map(_.mkString))
        } else {
          None
        }
      case _ => None
    }
  }

  def getFieldMetadata(
                        dataSourceName: String,
                        df: DataFrame,
                        connectionConfig: Map[String, String],
                        metadataConfig: MetadataConfig
                      )(implicit sparkSession: SparkSession): Array[Field] = {
    val fieldMetadata = getFieldDataProfilingMetadata(df, connectionConfig, dataSourceName, metadataConfig)
    val structFields = mapToStructFields(df, fieldMetadata)
    structFields.map(FieldHelper.fromStructField)
  }

  private def analyzeSupportsType(dataType: DataType): Boolean = dataType match {
    case IntegerType | ShortType | LongType | DecimalType() | DoubleType | FloatType => true
    case BooleanType => true
    case BinaryType | StringType => true
    case TimestampType | DateType => true
    case _ => false
  }

  /**
   * Rename Spark column statistics to be aligned with Data Caterer statistic names. Remove 'version'
   *
   * @param catalogColumnStat Spark column statistics
   * @return Map of statistics for column
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


case class DataProfilingMetadata(columnName: String, metadata: Map[String, Any], nestedProfiling: List[DataProfilingMetadata] = List())
