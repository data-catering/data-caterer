package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.exception.{InvalidDataContractFileFormatException, MissingDataContractFilePathException}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard.model.{OpenDataContractStandard, OpenDataContractStandardV3}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.{Failure, Success, Try}

case class OpenDataContractStandardDataSourceMetadata(
                                                       name: String,
                                                       format: String,
                                                       connectionConfig: Map[String, String],
                                                     ) extends DataSourceMetadata {

  override val hasSourceData: Boolean = false

  override def toStepName(options: Map[String, String]): String = {
    options(METADATA_IDENTIFIER)
  }

  override def getDataSourceValidations(dataSourceReadOptions: Map[String, String]): List[ValidationBuilder] = {
    //TODO wait until v3 before using data quality since ODCS uses rules from closed source system
    List()
  }

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    val optDataContractFile = connectionConfig.get(DATA_CONTRACT_FILE)
    optDataContractFile match {
      case Some(dataContractPath) =>
        val dataContractFile = new File(dataContractPath)
        val tryParseYamlV2 = Try(ObjectMapperUtil.yamlObjectMapper.readValue(dataContractFile, classOf[OpenDataContractStandard]))
        tryParseYamlV2 match {
          case Failure(exception) =>
            //try parse as v3 model
            val tryParseYamlV3 = Try(ObjectMapperUtil.yamlObjectMapper.readValue(dataContractFile, classOf[OpenDataContractStandardV3]))
            tryParseYamlV3 match {
              case Failure(exception) => throw InvalidDataContractFileFormatException(dataContractPath, exception)
              case Success(value) =>
                value.schema.getOrElse(Array()).map(schema => OpenDataContractStandardV3Mapper.toSubDataSourceMetadata(value, schema, connectionConfig))
            }
          case Success(value) =>
            //            val optSchemaName = connectionConfig.get(DATA_CONTRACT_SCHEMA)
            //TODO filter for schema if schema name is defined, otherwise return back all schemas
            value.dataset.map(dataset => OpenDataContractStandardV2Mapper.toSubDataSourceMetadata(value, dataset, connectionConfig))
        }
      case None => throw MissingDataContractFilePathException(name, format)
    }
  }
}
