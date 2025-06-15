package io.github.datacatering.datacaterer.core.generator.metadata.datasource

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.DEFAULT_STEP_NAME
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, MetadataConfig}
import io.github.datacatering.datacaterer.core.generator.metadata.StepNameProvider
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.LogHolder.LOGGER
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.validation.ValidationPredictor
import io.github.datacatering.datacaterer.core.model.ForeignKeyRelationship
import io.github.datacatering.datacaterer.core.util.MetadataUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.util.{Failure, Success, Try}

trait DataSourceMetadata {

  implicit val fieldMetadataEncoder: Encoder[FieldMetadata] = Encoders.kryo[FieldMetadata]
  implicit val foreignKeyRelationshipEncoder: Encoder[ForeignKeyRelationship] = Encoders.kryo[ForeignKeyRelationship]

  val name: String
  val format: String
  val connectionConfig: Map[String, String]
  val hasSourceData: Boolean

  def getAdditionalFieldMetadata(implicit sparkSession: SparkSession): Dataset[FieldMetadata] = {
    sparkSession.emptyDataset[FieldMetadata]
  }

  def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = {
    sparkSession.emptyDataset[ForeignKeyRelationship]
  }

  def close(): Unit = {}

  def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] =
    Array(SubDataSourceMetadata(connectionConfig))

  def toStepName(options: Map[String, String]): String = StepNameProvider.fromOptions(options).getOrElse(DEFAULT_STEP_NAME)

  def getDataSourceValidations(dataSourceReadOptions: Map[String, String]): List[ValidationBuilder] = List()

  /**
   * Extracts metadata for a given data source.
   *
   * This method is responsible for retrieving the metadata for a data source, including any sub-data sources and
   * additional field metadata. It handles cases where there are errors in finding the sub-data sources or existing
   * metadata, and provides a fallback to use the additional field metadata.
   *
   * @param flagsConfig    flags to control whether metadata is extracted or not
   * @param metadataConfig configuration to control how metadata is extracted
   * @return a list of `DataSourceDetail` objects containing the extracted metadata
   */
  def getMetadataForDataSource(
                                flagsConfig: FlagsConfig,
                                metadataConfig: MetadataConfig
                              )(implicit sparkSession: SparkSession): List[DataSourceDetail] = {
    LOGGER.info(s"Extracting out metadata from data source, name=$name, format=$format")
    val allDataSourceReadOptions = Try(getSubDataSourcesMetadata) match {
      case Failure(error) =>
        LOGGER.error(s"Unable to find any sub data sources or existing metadata, name=$name, " +
          s"format=$format, error=${error.getMessage}", error)
        Array[SubDataSourceMetadata]()
      case Success(value) =>
        LOGGER.info(s"Found sub data sources, name=$name, format=$format, " +
          s"num-sub-data-sources=${value.length}")
        value
    }

    val additionalFieldMetadata = getAdditionalFieldMetadata
    allDataSourceReadOptions.map(subDataSourceMeta => {
      val fieldMetadata = subDataSourceMeta.optFieldMetadata.getOrElse(additionalFieldMetadata)
      getFieldLevelMetadata(fieldMetadata, subDataSourceMeta.readOptions, flagsConfig, metadataConfig)
    }).toList
  }

  /**
   * Generates field-level metadata for a data source.
   *
   * This method reads in a sample of records from the data source, calculates data profiling metadata for the fields,
   * and maps the fields to Spark StructFields. It also generates any validations for the data source.
   *
   * @param additionalFieldMetadata additional field metadata for the data source
   * @param dataSourceReadOptions    the read options for the data source
   * @param flagsConfig              flags to control whether metadata is extracted or not
   * @param metadataConfig           configuration to control how metadata is extracted
   * @return a `DataSourceDetail` object containing the field-level metadata
   */
  private def getFieldLevelMetadata(
                                     additionalFieldMetadata: Dataset[FieldMetadata],
                                     dataSourceReadOptions: Map[String, String],
                                     flagsConfig: FlagsConfig,
                                     metadataConfig: MetadataConfig
                                   )(implicit sparkSession: SparkSession): DataSourceDetail = {
    if (flagsConfig.enableDeleteGeneratedRecords) {
      LOGGER.debug(s"Delete records is enabled, skipping field level metadata analysis of data source, name=$name")
      DataSourceDetail(this, dataSourceReadOptions, StructType(Seq()))
    } else if (!hasSourceData) {
      LOGGER.debug(s"Metadata source does not contain source data for data analysis. Field level metadata will not be calculated, name=$name")
      val structFields = MetadataUtil.mapToStructFieldsForMetadataExtraction(additionalFieldMetadata, dataSourceReadOptions)
      val validations = getGeneratedValidations(dataSourceReadOptions, structFields, flagsConfig)
      DataSourceDetail(this, dataSourceReadOptions, StructType(structFields), validations)
    } else {
      LOGGER.debug(s"Reading in records from data source for metadata analysis, name=$name, options=$dataSourceReadOptions, " +
        s"num-records-from-data-source=${metadataConfig.numRecordsFromDataSource}, num-records-for-analysis=${metadataConfig.numRecordsForAnalysis}")
      val data = sparkSession.read
        .format(format)
        .options(connectionConfig ++ dataSourceReadOptions)
        .load()
        .limit(metadataConfig.numRecordsFromDataSource)
        .sample(metadataConfig.numRecordsForAnalysis.toDouble / metadataConfig.numRecordsFromDataSource)

      val fieldsWithDataProfilingMetadata = MetadataUtil.getFieldDataProfilingMetadata(data, dataSourceReadOptions, this, metadataConfig)
      val structFields = MetadataUtil.mapToStructFieldsForMetadataExtraction(data, dataSourceReadOptions, fieldsWithDataProfilingMetadata, additionalFieldMetadata)
      val validations = getGeneratedValidations(dataSourceReadOptions, structFields, flagsConfig)
      DataSourceDetail(this, dataSourceReadOptions, StructType(structFields), validations)
    }
  }

  /**
   * Generate data validations based on external data source and schema
   *
   * @param dataSourceReadOptions connection options needed to connect to external data source
   * @param structFields          schema of the dataset
   * @param flagsConfig           flags to enable/disable generating validations
   * @return
   */
  private def getGeneratedValidations(
                                       dataSourceReadOptions: Map[String, String],
                                       structFields: Array[StructField],
                                       flagsConfig: FlagsConfig
                                     ): List[ValidationBuilder] = {
    if (flagsConfig.enableGenerateValidations) {
      LOGGER.debug("Generate validations is enabled")
      val suggestedValidations = ValidationPredictor.suggestValidations(this, dataSourceReadOptions, structFields)
      val validationsFromDataSource = getDataSourceValidations(dataSourceReadOptions)
      suggestedValidations ++ validationsFromDataSource
    } else {
      LOGGER.debug("Generate validations is disabled")
      List()
    }
  }
}

case class SubDataSourceMetadata(readOptions: Map[String, String] = Map(), optFieldMetadata: Option[Dataset[FieldMetadata]] = None)

object LogHolder extends Serializable {
  @transient lazy val LOGGER: Logger = Logger.getLogger(getClass.getName)
}
