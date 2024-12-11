package io.github.datacatering.datacaterer.core.generator.metadata.datasource.confluentschemaregistry

import io.github.datacatering.datacaterer.api.model.Constants.{CONFLUENT_SCHEMA_REGISTRY_ID, CONFLUENT_SCHEMA_REGISTRY_MESSAGE_NAME, CONFLUENT_SCHEMA_REGISTRY_SUBJECT, CONFLUENT_SCHEMA_REGISTRY_VERSION, DATA_SOURCE_NAME, DEFAULT_CONFLUENT_SCHEMA_REGISTRY_VERSION, DEFAULT_FIELD_TYPE, FACET_DATA_SOURCE, FIELD_DATA_TYPE, FIELD_DESCRIPTION, JDBC, JDBC_TABLE, METADATA_IDENTIFIER, METADATA_SOURCE_URL, OPEN_LINEAGE_NAMESPACE, URI}
import io.github.datacatering.datacaterer.core.exception.{FailedConfluentSchemaRegistryHttpCallException, InvalidConfluentSchemaRegistryResponseException, InvalidConfluentSchemaRegistrySchemaRequestException}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import io.github.datacatering.datacaterer.core.model.confluentschemaregistry.{ConfluentSchemaRegistrySchemaResponse, ConfluentSchemaRegistrySubjectVersionResponse}
import io.github.datacatering.datacaterer.core.model.openlineage.{ListDatasetResponse, OpenLineageDataset}
import io.github.datacatering.datacaterer.core.parser.ProtobufParser
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.asynchttpclient.Dsl.asyncHttpClient
import org.asynchttpclient.{AsyncHttpClient, Response}

import scala.util.{Failure, Success, Try}

case class ConfluentSchemaRegistryMetadata(
                                            name: String,
                                            format: String,
                                            connectionConfig: Map[String, String],
                                            asyncHttpClient: AsyncHttpClient = asyncHttpClient
                                          ) extends DataSourceMetadata {
  require(
    connectionConfig.contains(METADATA_SOURCE_URL),
    s"Configuration missing for Confluent Schema Registry metadata source, metadata-source=$name, missing-configuration=$METADATA_SOURCE_URL"
  )

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OPT_SUBJECT = connectionConfig.get(CONFLUENT_SCHEMA_REGISTRY_SUBJECT)
  private val OPT_VERSION = connectionConfig.get(CONFLUENT_SCHEMA_REGISTRY_VERSION)
  private val OPT_SCHEMA_ID = connectionConfig.get(CONFLUENT_SCHEMA_REGISTRY_ID)
  private val BASE_URL = connectionConfig(METADATA_SOURCE_URL)

  override val hasSourceData: Boolean = false

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    val (schemaType, schema) = (OPT_SCHEMA_ID, OPT_SUBJECT, OPT_VERSION) match {
      case (Some(id), _, _) =>
        val baseSchema = getSchema(id)
        (baseSchema.schemaType, baseSchema.schema)
      case (None, Some(subject), Some(version)) =>
        val baseSchema = getSchema(subject, version)
        (baseSchema.schemaType, baseSchema.schema)
      case (None, Some(subject), None) =>
        val baseSchema = getSchema(subject, DEFAULT_CONFLUENT_SCHEMA_REGISTRY_VERSION)
        (baseSchema.schemaType, baseSchema.schema)
      case (None, None, Some(version)) =>
        throw InvalidConfluentSchemaRegistrySchemaRequestException("subject")
      case _ =>
        throw InvalidConfluentSchemaRegistrySchemaRequestException("id, subject, version")
    }

    val parsedSchema = if (schemaType.equalsIgnoreCase("protobuf")) {
      val optMessageName = connectionConfig.get(CONFLUENT_SCHEMA_REGISTRY_MESSAGE_NAME)
      optMessageName match {
        case Some(messageName) => ProtobufParser.getMessageFromProtoString(schema, messageName)
        case None => throw InvalidConfluentSchemaRegistrySchemaRequestException(CONFLUENT_SCHEMA_REGISTRY_MESSAGE_NAME)
      }
    } else if (schemaType.equalsIgnoreCase("avro")) {

    } else if (schemaType.equalsIgnoreCase("json")) {

    }
    Array()
  }

  override def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[FieldMetadata] = {
    sparkSession.emptyDataset
  }

  override def close(): Unit = {
    asyncHttpClient.close()
  }

  def toMetadataIdentifier(dataset: OpenLineageDataset) = s"${dataset.id.namespace}_${dataset.id.name}"

  def getSchema(id: String): ConfluentSchemaRegistrySchemaResponse = {
    val response = getResponse(s"$BASE_URL/schemas/id/$id")
    val tryParseResponse = Try(
      ObjectMapperUtil.jsonObjectMapper.readValue(response.getResponseBody, classOf[ConfluentSchemaRegistrySchemaResponse])
    )
    getResponse(tryParseResponse)
  }

  def getSchema(subject: String, version: String): ConfluentSchemaRegistrySubjectVersionResponse = {
    val response = getResponse(s"$BASE_URL/subjects/$subject/versions/$version")
    val tryParseResponse = Try(
      ObjectMapperUtil.jsonObjectMapper.readValue(response.getResponseBody, classOf[ConfluentSchemaRegistrySubjectVersionResponse])
    )
    getResponse(tryParseResponse)
  }

  private def getResponse(url: String): Response = {
    val tryRequest = Try(asyncHttpClient.prepareGet(url).execute().get())
    tryRequest match {
      case Failure(exception) => throw FailedConfluentSchemaRegistryHttpCallException(url, exception)
      case Success(value) => value
    }
  }

  private def getResponse[T](tryParse: Try[T]): T = {
    tryParse match {
      case Failure(exception) =>
        LOGGER.error("Failed to parse response from Confluent Schema Registry")
        throw InvalidConfluentSchemaRegistryResponseException(exception)
      case Success(value) =>
        LOGGER.debug("Successfully parse response from Marquez to OpenLineage definition")
        value
    }
  }
}
