package io.github.datacatering.datacaterer.api

import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api.converter.Converters.toScalaMap
import io.github.datacatering.datacaterer.api.model.Constants.{CONFLUENT_SCHEMA_REGISTRY_ID, CONFLUENT_SCHEMA_REGISTRY_SUBJECT, CONFLUENT_SCHEMA_REGISTRY_VERSION, DATA_CONTRACT_FILE, DATA_CONTRACT_SCHEMA, GREAT_EXPECTATIONS_FILE, JSON_SCHEMA_FILE, METADATA_SOURCE_URL, OPEN_LINEAGE_DATASET, OPEN_LINEAGE_NAMESPACE, OPEN_METADATA_API_VERSION, OPEN_METADATA_AUTH_TYPE, OPEN_METADATA_AUTH_TYPE_OPEN_METADATA, OPEN_METADATA_DEFAULT_API_VERSION, OPEN_METADATA_HOST, OPEN_METADATA_JWT_TOKEN, SCHEMA_LOCATION}
import io.github.datacatering.datacaterer.api.model.{ConfluentSchemaRegistrySource, DataContractCliSource, GreatExpectationsSource, JsonSchemaSource, MarquezMetadataSource, MetadataSource, OpenAPISource, OpenDataContractStandardSource, OpenMetadataSource}

case class MetadataSourceBuilder(metadataSource: MetadataSource = MarquezMetadataSource()) {
  def this() = this(MarquezMetadataSource())

  def marquez(url: String, namespace: String, optDataset: Option[String] = None, options: Map[String, String] = Map()): MetadataSourceBuilder = {
    val baseOptions = Map(
      METADATA_SOURCE_URL -> url,
      OPEN_LINEAGE_NAMESPACE -> namespace,
    ) ++ options
    val optionsWithDataset = optDataset.map(ds => baseOptions ++ Map(OPEN_LINEAGE_DATASET -> ds)).getOrElse(baseOptions)
    val marquezMetadataSource = MarquezMetadataSource(optionsWithDataset)
    this.modify(_.metadataSource).setTo(marquezMetadataSource)
  }

  def marquezJava(url: String, namespace: String, dataset: String, options: java.util.Map[String, String]): MetadataSourceBuilder =
    marquez(url, namespace, Some(dataset), toScalaMap(options))

  def marquez(url: String, namespace: String, dataset: String): MetadataSourceBuilder =
    marquez(url, namespace, Some(dataset), Map())

  def marquez(url: String, namespace: String): MetadataSourceBuilder =
    marquez(url, namespace, None, Map())

  def openMetadata(url: String, apiVersion: String, authProvider: String, options: Map[String, String]): MetadataSourceBuilder = {
    val baseOptions = Map(
      OPEN_METADATA_HOST -> url,
      OPEN_METADATA_API_VERSION -> apiVersion,
      OPEN_METADATA_AUTH_TYPE -> authProvider
    ) ++ options
    val openMetadataSource = OpenMetadataSource(baseOptions)
    this.modify(_.metadataSource).setTo(openMetadataSource)
  }

  /**
   * authProvider is one of:
   * - no-auth
   * - basic
   * - azure
   * - google
   * - okta
   * - auth0
   * - aws-cognito
   * - custom-oidc
   * - ldap
   * - saml
   * - openmetadata
   *
   * options can contain additional authentication related configuration values.
   * Check under {{{Constants}}} openmetadata section for more details.
   *
   * @param url          URL to OpenMetadata server
   * @param authProvider See above for list of auth providers
   * @param options      Additional auth configuration
   * @return
   */
  def openMetadata(url: String, authProvider: String, options: Map[String, String]): MetadataSourceBuilder =
    openMetadata(url, OPEN_METADATA_DEFAULT_API_VERSION, authProvider, options)

  def openMetadataWithToken(url: String, openMetadataToken: String, options: Map[String, String] = Map()): MetadataSourceBuilder =
    openMetadata(url, OPEN_METADATA_DEFAULT_API_VERSION, OPEN_METADATA_AUTH_TYPE_OPEN_METADATA, options ++ Map(OPEN_METADATA_JWT_TOKEN -> openMetadataToken))

  def openMetadataJava(url: String, authProvider: String, options: java.util.Map[String, String]): MetadataSourceBuilder =
    openMetadata(url, OPEN_METADATA_DEFAULT_API_VERSION, authProvider, toScalaMap(options))

  def openApi(schemaLocation: String): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(OpenAPISource(Map(SCHEMA_LOCATION -> schemaLocation)))
  }

  def greatExpectations(expectationsFile: String): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(GreatExpectationsSource(Map(GREAT_EXPECTATIONS_FILE -> expectationsFile)))
  }

  def openDataContractStandard(dataContractFile: String): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(OpenDataContractStandardSource(Map(DATA_CONTRACT_FILE -> dataContractFile)))
  }

  def openDataContractStandard(dataContractFile: String, schemaName: String): MetadataSourceBuilder = {
    openDataContractStandard(dataContractFile, List(schemaName))
  }

  def openDataContractStandard(dataContractFile: String, schemaNames: List[String]): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(OpenDataContractStandardSource(Map(
      DATA_CONTRACT_FILE -> dataContractFile,
      DATA_CONTRACT_SCHEMA -> schemaNames.mkString(",")
    )))
  }

  def dataContractCli(dataContractFile: String): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(DataContractCliSource(Map(DATA_CONTRACT_FILE -> dataContractFile)))
  }

  def dataContractCli(dataContractFile: String, modelName: String): MetadataSourceBuilder = {
    dataContractCli(dataContractFile, List(modelName))
  }

  def dataContractCli(dataContractFile: String, modelNames: List[String]): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(DataContractCliSource(Map(
      DATA_CONTRACT_FILE -> dataContractFile,
      DATA_CONTRACT_SCHEMA -> modelNames.mkString(",")
    )))
  }

  def confluentSchemaRegistry(url: String, schemaId: Int): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(ConfluentSchemaRegistrySource(Map(
      METADATA_SOURCE_URL -> url,
      CONFLUENT_SCHEMA_REGISTRY_ID -> schemaId.toString
    )))
  }

  def confluentSchemaRegistry(url: String, schemaSubject: String): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(ConfluentSchemaRegistrySource(Map(
      METADATA_SOURCE_URL -> url,
      CONFLUENT_SCHEMA_REGISTRY_SUBJECT -> schemaSubject
    )))
  }

  def confluentSchemaRegistry(url: String, schemaSubject: String, version: Int): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(ConfluentSchemaRegistrySource(Map(
      METADATA_SOURCE_URL -> url,
      CONFLUENT_SCHEMA_REGISTRY_SUBJECT -> schemaSubject,
      CONFLUENT_SCHEMA_REGISTRY_VERSION -> version.toString,
    )))
  }

  def jsonSchema(schemaFile: String): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(JsonSchemaSource(Map(JSON_SCHEMA_FILE -> schemaFile)))
  }

  def jsonSchema(schemaFile: String, options: Map[String, String]): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(JsonSchemaSource(Map(JSON_SCHEMA_FILE -> schemaFile) ++ options))
  }

  def jsonSchemaJava(schemaFile: String, options: java.util.Map[String, String]): MetadataSourceBuilder =
    jsonSchema(schemaFile, toScalaMap(options))
}
