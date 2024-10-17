package io.github.datacatering.datacaterer.core.generator.metadata.datasource.http

import io.github.datacatering.datacaterer.api.model.Constants.{METADATA_IDENTIFIER, SCHEMA_LOCATION}
import io.github.datacatering.datacaterer.core.exception.MissingOpenApiServersException
import io.github.datacatering.datacaterer.core.generator.metadata.StepNameProvider
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import io.swagger.v3.parser.OpenAPIV3Parser
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class HttpMetadata(name: String, format: String, connectionConfig: Map[String, String]) extends DataSourceMetadata {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override val hasSourceData: Boolean = false

  override def toStepName(options: Map[String, String]): String = {
    options(METADATA_IDENTIFIER)
  }

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    connectionConfig.get(SCHEMA_LOCATION) match {
      case Some(location) =>
        val openApiSpec = new OpenAPIV3Parser().read(location)
        val openAPIConverter = new OpenAPIConverter(openApiSpec)

        if (openApiSpec.getServers.size() < 0) {
          throw MissingOpenApiServersException(location)
        } else if (openApiSpec.getServers.size() > 1) {
          LOGGER.warn(s"More than one server definition found under servers, will use the first URL found in servers definition, " +
            s"schema-location=$location")
        }

        val pathSubDataSourceMetadata = openApiSpec.getPaths.asScala.flatMap(path => {
          path._2.readOperationsMap()
            .asScala
            .map(pathOperation => {
              val stepName = StepNameProvider.fromHttp(pathOperation._1.name(), path._1)
              val readOptions = Map(METADATA_IDENTIFIER -> stepName)
              val columnMetadata = openAPIConverter.toColumnMetadata(path._1, pathOperation._1, pathOperation._2, readOptions)
              val dsMetadata = sparkSession.createDataset(columnMetadata)
              SubDataSourceMetadata(readOptions, Some(dsMetadata))
            })
        }).toArray
        pathSubDataSourceMetadata
      case None =>
        LOGGER.warn(s"No $SCHEMA_LOCATION defined, unable to extract out metadata for http data source. Please define $SCHEMA_LOCATION " +
          s"as either an endpoint or file location to the OpenAPI specification for your http endpoints, name=$name")
        Array()
    }
  }
}
