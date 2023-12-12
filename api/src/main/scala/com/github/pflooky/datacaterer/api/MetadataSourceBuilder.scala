package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap
import com.github.pflooky.datacaterer.api.model.Constants.{DATA_CONTRACT_FILE, EXPECTATIONS_FILE, METADATA_SOURCE_URL, OPEN_LINEAGE_DATASET, OPEN_LINEAGE_NAMESPACE, OPEN_METADATA_API_VERSION, OPEN_METADATA_AUTH_TYPE, OPEN_METADATA_AUTH_TYPE_OPEN_METADATA, OPEN_METADATA_DEFAULT_API_VERSION, OPEN_METADATA_HOST, OPEN_METADATA_JWT_TOKEN, SCHEMA_LOCATION}
import com.github.pflooky.datacaterer.api.model.{GreatExpectationsSource, MarquezMetadataSource, MetadataSource, OpenAPISource, OpenDataContractStandardSource, OpenMetadataSource}
import com.softwaremill.quicklens.ModifyPimp

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
    this.modify(_.metadataSource).setTo(GreatExpectationsSource(Map(EXPECTATIONS_FILE -> expectationsFile)))
  }

  def openDataContractStandard(dataContractFile: String): MetadataSourceBuilder = {
    this.modify(_.metadataSource).setTo(OpenDataContractStandardSource(Map(DATA_CONTRACT_FILE -> dataContractFile)))
  }
}
