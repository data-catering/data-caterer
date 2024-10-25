package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties


object KindEnum extends Enumeration {
  val DataContract = Value
}


object ApiVersionEnum extends Enumeration {
  val `v3.0.0`, `v2.2.2`, `v2.2.1`, `v2.2.0` = Value
}

object ServerTypeEnum extends Enumeration {
  val api, athena, azure, bigquery, clickhouse, databricks, denodo, dremio, duckdb, glue, cloudsql, db2, informix, kafka, kinesis, local, mysql, oracle, postgresql, postgres, presto, pubsub, redshift, s3, sftp, snowflake, sqlserver, synapse, trino, vertica, custom = Value
}

object LogicalTypeEnum extends Enumeration {
  val string, date, number, integer, `object`, array, boolean = Value
}

object DataQualityTypeEnum extends Enumeration {
  val text, library, sql, custom = Value
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardV3(
                                       apiVersion: ApiVersionEnum.Value,
                                       id: String,
                                       kind: KindEnum.Value,
                                       status: String,
                                       version: String,
                                       contractCreatedTs: Option[String] = None,
                                       customProperties: Option[List[OpenDataContractStandardCustomProperty]] = None,
                                       dataProduct: Option[String] = None,
                                       description: Option[OpenDataContractStandardDescription] = None,
                                       domain: Option[String] = None,
                                       name: Option[String] = None,
                                       price: Option[OpenDataContractStandardPrice] = None,
                                       roles: Option[Array[OpenDataContractStandardRole]] = None,
                                       schema: Option[Array[OpenDataContractStandardSchemaV3]] = None,
                                       server: Option[List[OpenDataContractStandardServerV3]] = None,
                                       slaDefaultElement: Option[String] = None,
                                       slaProperties: Option[Array[OpenDataContractStandardServiceLevelAgreementProperty]] = None,
                                       support: Option[List[OpenDataContractStandardSupport]] = None,
                                       tags: Option[Array[String]] = None,
                                       team: Option[Array[OpenDataContractStandardTeam]] = None,
                                       tenant: Option[String] = None,
                                       `type`: Option[String] = None,
                                     )

/**
 * Data source details of where data is physically stored.
 *
 * @param server           Identifier of the server.
 * @param type             Type of the server.
 * @param description      Description of the server.
 * @param environment      Environment of the server.
 * @param roles            List of roles that have access to the server.
 * @param customProperties A list of key/value pairs for custom properties.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardServerV3(
                                             server: String,
                                             `type`: ServerTypeEnum.Value,
                                             description: Option[String],
                                             environment: Option[String],
                                             roles: Option[Array[OpenDataContractStandardRole]],
                                             customProperties: Option[Array[OpenDataContractStandardCustomProperty]]
                                           )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardSchemaV3(
                                             name: String,
                                             authoritativeDefinitions: Option[Array[OpenDataContractStandardAuthoritativeDefinition]] = None,
                                             businessName: Option[String] = None,
                                             customProperties: Option[Array[OpenDataContractStandardCustomProperty]] = None,
                                             dataGranularityDescription: Option[String] = None,
                                             description: Option[String] = None,
                                             logicalType: Option[String] = None,
                                             physicalName: Option[String] = None,
                                             physicalType: Option[String] = None,
                                             properties: Option[Array[OpenDataContractStandardElementV3]] = None,
                                             priorTableName: Option[String] = None,
                                             quality: Option[Array[OpenDataContractStandardDataQualityV3]] = None,
                                             tags: Option[Array[String]] = None,
                                           )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardElementV3(
                                              name: String,
                                              logicalType: LogicalTypeEnum.Value,
                                              physicalType: String,
                                              authoritativeDefinitions: Option[Array[OpenDataContractStandardAuthoritativeDefinition]] = None,
                                              businessName: Option[String] = None,
                                              criticalDataElement: Option[Boolean] = None,
                                              classification: Option[String] = None,
                                              customProperties: Option[Array[OpenDataContractStandardCustomProperty]] = None,
                                              description: Option[String] = None,
                                              encryptedName: Option[String] = None,
                                              examples: Option[Array[Any]] = None,
                                              logicalTypeOptions: Option[OpenDataContractStandardLogicalTypeOptionsV3] = None,
                                              partitioned: Option[Boolean] = None,
                                              partitionKeyPosition: Option[Int] = None,
                                              properties: Option[Array[OpenDataContractStandardElementV3]] = None,
                                              primaryKey: Option[Boolean] = None,
                                              primaryKeyPosition: Option[Int] = None,
                                              quality: Option[Array[OpenDataContractStandardDataQualityV3]] = None,
                                              required: Option[Boolean] = None,
                                              tags: Option[Array[String]] = None,
                                              transformDescription: Option[String] = None,
                                              transformLogic: Option[String] = None,
                                              transformSourceObjects: Option[Array[String]] = None,
                                              unique: Option[Boolean] = None,
                                            )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardLogicalTypeOptionsV3(
                                                         exclusiveMaximum: Option[Boolean] = None,
                                                         exclusiveMinimum: Option[Boolean] = None,
                                                         format: Option[String] = None,
                                                         maximum: Option[Any] = None,
                                                         maxItems: Option[Int] = None,
                                                         maxLength: Option[Int] = None,
                                                         maxProperties: Option[Int] = None,
                                                         minimum: Option[Any] = None,
                                                         minItems: Option[Int] = None,
                                                         minLength: Option[Int] = None,
                                                         minProperties: Option[Int] = None,
                                                         multipleOf: Option[Int] = None,
                                                         pattern: Option[String] = None,
                                                         required: Option[Array[String]] = None,
                                                         uniqueItems: Option[Boolean] = None,
                                                       )

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardDataQualityV3(
                                                  `type`: DataQualityTypeEnum.Value,
                                                  authoritativeDefinitions: Option[Array[OpenDataContractStandardAuthoritativeDefinition]] = None,
                                                  businessImpact: Option[String] = None,
                                                  code: Option[String] = None,
                                                  column: Option[String] = None,
                                                  columns: Option[String] = None,
                                                  customProperties: Option[Array[OpenDataContractStandardCustomProperty]] = None,
                                                  description: Option[String] = None,
                                                  dimension: Option[String] = None,
                                                  engine: Option[String] = None,
                                                  implementation: Option[String] = None,
                                                  method: Option[String] = None,
                                                  mustBe: Option[Any] = None,
                                                  mustBeBetween: Option[Array[Double]] = None,
                                                  mustBeGreaterThan: Option[Double] = None,
                                                  mustBeGreaterOrEqualTo: Option[Double] = None,
                                                  mustBeLessThan: Option[Double] = None,
                                                  mustBeLessOrEqualTo: Option[Double] = None,
                                                  mustNotBe: Option[Any] = None,
                                                  mustNotBeBetween: Option[Array[Double]] = None,
                                                  name: Option[String] = None,
                                                  query: Option[String] = None,
                                                  rule: Option[String] = None,
                                                  scheduler: Option[String] = None,
                                                  schedule: Option[String] = None,
                                                  severity: Option[String] = None,
                                                  tags: Option[Array[String]] = None,
                                                  toolRuleName: Option[String] = None,
                                                  unit: Option[String] = None,
                                                )

/**
 * @param channel       Channel name or identifier.
 * @param url           Access URL using normal [URL scheme](https://en.wikipedia.org/wiki/URL#Syntax) (https, mailto, etc.).
 * @param description   Description of the channel, free text.
 * @param tool          Name of the tool, value can be `email`, `slack`, `teams`, `discord`, `ticket`, or `other`.
 * @param scope         Scope can be: `interactive`, `announcements`, `issues`.
 * @param invitationUrl Some tools uses invitation URL for requesting or subscribing. Follows the [URL scheme](https://en.wikipedia.org/wiki/URL#Syntax).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardSupport(
                                            channel: String,
                                            url: String,
                                            description: Option[String] = None,
                                            tool: Option[String] = None,
                                            scope: Option[String] = None,
                                            invitationUrl: Option[String] = None
                                          )

/**
 * @param username           The user's username or email.
 * @param role               The user's job role; Examples might be owner, data steward. There is no limit on the role.
 * @param dateIn             The date when the user joined the team.
 * @param dateOut            The date when the user ceased to be part of the team.
 * @param replacedByUsername The username of the user who replaced the previous user.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenDataContractStandardTeam(
                                         dateIn: Option[String] = None,
                                         dateOut: Option[String] = None,
                                         username: Option[String] = None,
                                         replacedByUsername: Option[String] = None,
                                         role: Option[String] = None,
                                       )
