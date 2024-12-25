package io.github.datacatering.datacaterer.core.generator.metadata.datasource.confluentschemaregistry

import io.github.datacatering.datacaterer.api.model.Constants.{CONFLUENT_SCHEMA_REGISTRY, CONFLUENT_SCHEMA_REGISTRY_ID, CONFLUENT_SCHEMA_REGISTRY_SUBJECT, CONFLUENT_SCHEMA_REGISTRY_VERSION, DEFAULT_CONFLUENT_SCHEMA_REGISTRY_VERSION, FIELD_DATA_TYPE, METADATA_IDENTIFIER, METADATA_SOURCE_URL}
import io.github.datacatering.datacaterer.api.model.Field
import io.github.datacatering.datacaterer.core.exception.{FailedConfluentSchemaRegistryHttpCallException, FailedConfluentSchemaRegistryResponseException, InvalidConfluentSchemaRegistryResponseException, InvalidConfluentSchemaRegistrySchemaRequestException}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import io.github.datacatering.datacaterer.core.model.confluentschemaregistry.ConfluentSchemaRegistrySchemaResponse
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
    val schemaDetails = getSchemaDetails

    //TODO add in support for avro and json
    val parsedFields = if (schemaDetails.`type`.equalsIgnoreCase("protobuf")) {
      LOGGER.info(s"Found schema of type protobuf from Confluent Schema Registry, schema-name=${schemaDetails.name}")
      ProtobufParser.getSchemaFromProtoString(schemaDetails.schema, schemaDetails.name)
    } else {
      LOGGER.warn(s"Unsupported schema type from Confluent Schema Registry, schema-type=${schemaDetails.`type`}, schema-name=${schemaDetails.name}")
      List()
    }
    val ds = sparkSession.createDataset(parsedFields.map(f => fieldToFieldMetadata(f, schemaDetails.name)))
    Array(SubDataSourceMetadata(
      Map(METADATA_IDENTIFIER -> toMetadataIdentifier(schemaDetails.name)) ++ connectionConfig,
      Some(ds)
    ))
  }

  private def getSchemaDetails: ConfluentSchemaDetails = {
    (OPT_SCHEMA_ID, OPT_SUBJECT, OPT_VERSION) match {
      case (Some(id), _, _) =>
        LOGGER.info(s"Attempting to get schema from Confluent Schema Registry for schema id, id=$id")
        val baseSchema = getSchema(id)
        ConfluentSchemaDetails(id, baseSchema.schemaType, baseSchema.schema)
      case (None, Some(subject), Some(version)) =>
        LOGGER.debug(s"Attempting to get schema from Confluent Schema Registry for schema subject and version, subject=$subject, version=$version")
        val baseSchema = getSchema(subject, version)
        ConfluentSchemaDetails(subject, baseSchema.schemaType, baseSchema.schema)
      case (None, Some(subject), None) =>
        LOGGER.debug(s"Attempting to get schema from Confluent Schema Registry for schema subject, subject=$subject")
        val baseSchema = getSchema(subject, DEFAULT_CONFLUENT_SCHEMA_REGISTRY_VERSION)
        ConfluentSchemaDetails(subject, baseSchema.schemaType, baseSchema.schema)
      case (None, None, Some(_)) =>
        throw InvalidConfluentSchemaRegistrySchemaRequestException("subject")
      case _ =>
        throw InvalidConfluentSchemaRegistrySchemaRequestException("id, subject, version")
    }
  }

  override def getAdditionalFieldMetadata(implicit sparkSession: SparkSession): Dataset[FieldMetadata] = {
    sparkSession.emptyDataset
  }

  override def close(): Unit = {
    asyncHttpClient.close()
  }

  private def toMetadataIdentifier(schemaName: String): String = s"${CONFLUENT_SCHEMA_REGISTRY}_$schemaName"

  private def getSchema(id: String): ConfluentSchemaRegistrySchemaResponse = {
    val response = getResponse(s"$BASE_URL/schemas/ids/$id")
    parseResponse(response)
  }

  private def getSchema(subject: String, version: String): ConfluentSchemaRegistrySchemaResponse = {
    val response = getResponse(s"$BASE_URL/subjects/$subject/versions/$version")
    parseResponse(response)
  }

  private def fieldToFieldMetadata(field: Field, schemaName: String): FieldMetadata = {
    val generatorOpts = field.options.map(o => o._1 -> o._2.toString)
    val optNested = field.fields.map(f => fieldToFieldMetadata(f, schemaName))
    FieldMetadata(
      field.name,
      Map(METADATA_IDENTIFIER -> toMetadataIdentifier(schemaName)),
      Map(FIELD_DATA_TYPE -> field.`type`.getOrElse("string")) ++ generatorOpts,
      optNested
    )
  }

  private def getResponse(url: String): Response = {
    val tryRequest = Try(asyncHttpClient.prepareGet(url).execute().get())
    tryRequest match {
      case Failure(exception) => throw FailedConfluentSchemaRegistryHttpCallException(url, exception)
      case Success(value) => value
    }
  }

  private def parseResponse(response: Response): ConfluentSchemaRegistrySchemaResponse = {
    if (response.getStatusCode == 200) {
      val tryParseResponse = Try(
        ObjectMapperUtil.jsonObjectMapper.readValue(response.getResponseBody, classOf[ConfluentSchemaRegistrySchemaResponse])
      )
      tryParseResponse match {
        case Failure(exception) =>
          LOGGER.error("Failed to parse response from Confluent Schema Registry")
          throw InvalidConfluentSchemaRegistryResponseException(exception)
        case Success(value) =>
          LOGGER.debug("Successfully parse response from Confluent Schema Registry")
          value
      }
    } else {
      throw FailedConfluentSchemaRegistryResponseException(response.getUri.toFullUrl, response.getStatusCode, response.getStatusText)
    }
  }

  private case class ConfluentSchemaDetails(name: String, `type`: String, schema: String)
}
