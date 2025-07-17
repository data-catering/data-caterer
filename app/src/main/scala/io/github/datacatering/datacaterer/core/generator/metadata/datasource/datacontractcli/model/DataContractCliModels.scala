package io.github.datacatering.datacaterer.core.generator.metadata.datasource.datacontractcli.model

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

/**
 * @param dataContractSpecification Specifies the Data Contract Specification being used.
 * @param id                        Specifies the identifier of the data contract.
 * @param info                      Metadata and life cycle information about the data contract.
 * @param servers                   Information about the servers.
 * @param terms                     The terms and conditions of the data contract.
 * @param models                    Specifies the logical data model. Use the models name (e.g., the table name) as the key.
 * @param definitions               Clear and concise explanations of syntax, semantic, and classification of business objects in a given domain.
 * @param schema                    The schema of the data contract describes the syntax and semantics of provided data sets. It supports different schema types.
 * @param examples                  The Examples Object is an array of Example Objects.
 * @param serviceLevels             Specifies the service level agreements for the provided data, including availability, data retention policies, latency requirements, data freshness, update frequency, support availability, and backup policies.
 * @param quality                   The quality object contains quality attributes and checks.
 * @param links                     Links to external resources.
 * @param tags                      Tags to facilitate searching and filtering.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class DataContractSpecification(
                                      dataContractSpecification: String,
                                      id: String,
                                      info: Info,
                                      servers: Option[Map[String, Server]],
                                      terms: Option[Terms],
                                      models: Option[Map[String, Model]],
                                      definitions: Option[Definitions],
                                      schema: Option[Schema],
                                      examples: Option[List[Examples]],
                                      serviceLevels: Option[ServiceLevels],
                                      quality: Option[Quality],
                                      links: Option[Links],
                                      tags: Option[List[String]]
                                    )


/**
 * Metadata and life cycle information about the data contract.
 *
 * @param title       The title of the data contract.
 * @param version     The version of the data contract document (which is distinct from the Data Contract Specification version or the Data Product implementation version).
 * @param status      The status of the data contract. Can be proposed, in development, active, retired.
 * @param description A description of the data contract.
 * @param owner       The owner or team responsible for managing the data contract and providing the data.
 * @param contact     Contact information for the data contract.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Info(
                 title: String,
                 version: String,
                 status: Option[String],
                 description: Option[String],
                 owner: Option[String],
                 contact: Option[Contact]
               )


/**
 * Contact information for the data contract.
 *
 * @param name  The identifying name of the contact person/organization.
 * @param url   The URL pointing to the contact information. This MUST be in the form of a URL.
 * @param email The email address of the contact person/organization. This MUST be in the form of an email address.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Contact(
                    name: Option[String],
                    url: Option[String],
                    email: Option[String]
                  )


/**
 * Information about the servers.
 *
 * @param description An optional string describing the servers.
 * @param environment The environment in which the servers are running. Examples: prod, sit, stg.
 */
@JsonTypeInfo(use = Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type")
@JsonSubTypes(Array(
  new Type(value = classOf[BigQueryServer], name = "bigquery"),
  new Type(value = classOf[S3Server], name = "s3"),
  new Type(value = classOf[SftpServer], name = "sftp"),
  new Type(value = classOf[RedshiftServer], name = "redshift"),
  new Type(value = classOf[AzureServer], name = "azure"),
  new Type(value = classOf[SqlServerServer], name = "sqlserver"),
  new Type(value = classOf[SnowflakeServer], name = "snowflake"),
  new Type(value = classOf[DatabricksServer], name = "databricks"),
  new Type(value = classOf[DataframeServer], name = "dataframe"),
  new Type(value = classOf[GlueServer], name = "glue"),
  new Type(value = classOf[PostgresServer], name = "postgres"),
  new Type(value = classOf[OracleServer], name = "oracle"),
  new Type(value = classOf[KafkaServer], name = "kafka"),
  new Type(value = classOf[PubSubServer], name = "pubsub"),
  new Type(value = classOf[KinesisDataStreamsServer], name = "kinesis"),
  new Type(value = classOf[TrinoServer], name = "trino"),
  new Type(value = classOf[LocalServer], name = "local"),
))
@JsonIgnoreProperties(ignoreUnknown = true)
trait Server {
  val `type`: String = "s3"
  val description: Option[String] = None
  val environment: Option[String] = None
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class BigQueryServer(
                           project: String,
                           dataset: String,
                         ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class S3Server(
                     location: String,
                     endpointUrl: Option[String],
                     format: Option[String],
                     delimiter: Option[String],
                   ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class SftpServer(
                       location: String,
                       format: Option[String],
                       delimiter: Option[String],
                     ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class RedshiftServer(
                           account: String,
                           database: String,
                           schema: String,
                         ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class AzureServer(
                        location: String,
                        format: String,
                        delimiter: Option[String],
                      ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class SqlServerServer(
                            host: String,
                            database: String,
                            schema: String,
                            port: Option[Int] = Some(1433),
                          ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class SnowflakeServer(
                            account: String,
                            database: String,
                            schema: String,
                          ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class DatabricksServer(
                             host: String,
                             catalog: String,
                             schema: String,
                           ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class DataframeServer() extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class GlueServer(
                       account: String,
                       database: String,
                       location: String,
                       format: String,
                     ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class PostgresServer(
                           host: String,
                           port: Int,
                           database: String,
                           schema: String,
                         ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class OracleServer(
                         host: String,
                         port: Int,
                         serviceName: String,
                       ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class KafkaServer(
                        host: String,
                        topic: String,
                        format: Option[String] = Some("json"),
                      ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class PubSubServer(
                         host: String,
                         project: String,
                         topic: String,
                       ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class KinesisDataStreamsServer(
                                     stream: String,
                                     region: String,
                                     format: String,
                                   ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class TrinoServer(
                        host: String,
                        port: Int,
                        catalog: String,
                        schema: String,
                      ) extends Server

@JsonIgnoreProperties(ignoreUnknown = true)
case class LocalServer(
                        path: String,
                        format: String,
                      ) extends Server


/**
 * The terms and conditions of the data contract.
 *
 * @param usage        The usage describes the way the data is expected to be used. Can contain business and technical information.
 * @param limitations  The limitations describe the restrictions on how the data can be used, can be technical or restrictions on what the data may not be used for.
 * @param billing      The billing describes the pricing model for using the data, such as whether it's free, having a monthly fee, or metered pay-per-use.
 * @param noticePeriod The period of time that must be given by either party to terminate or modify a data usage agreement. Uses ISO-8601 period format, e.g., 'P3M' for a period of three months.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Terms(
                  usage: Option[String],
                  limitations: Option[String],
                  billing: Option[String],
                  noticePeriod: Option[String]
                )

@JsonIgnoreProperties(ignoreUnknown = true)
case class Model(
                  description: Option[String],
                  fields: Map[String, Field]
                )

@JsonIgnoreProperties(ignoreUnknown = true)
case class FieldConfig(
                        avroType: Option[String],
                        avroLogicalType: Option[String],
                        bigqueryType: Option[String],
                        snowflakeType: Option[String],
                        redshiftType: Option[String],
                        sqlserverType: Option[String],
                        databricksType: Option[String],
                        glueType: Option[String]
                      )


object FieldTypeEnum extends Enumeration {
  val number, decimal, numeric, int, integer, long, bigint, float, double, string, text, varchar, boolean, timestamp,
  timestamp_tz, timestamp_ntz, date, array, map, `object`, record, struct, bytes, `null` = Value
}
class FieldTypeEnumRef extends TypeReference[FieldTypeEnum.type]

@JsonIgnoreProperties(ignoreUnknown = true)
case class Field(
                  @JsonScalaEnumeration(classOf[FieldTypeEnumRef]) `type`: FieldTypeEnum.Value,
                  description: Option[String],
                  title: Option[String],
                  required: Option[Boolean],
                  fields: Option[Map[String, Field]],
                  items: Option[Map[String, Field]],
                  keys: Option[Map[String, Field]],
                  values: Option[Map[String, Field]],
                  primary: Option[Boolean],
                  references: Option[String],
                  unique: Option[Boolean],
                  `enum`: Option[List[String]],
                  minLength: Option[Int],
                  maxLength: Option[Int],
                  format: Option[String],
                  precision: Option[Int],
                  scale: Option[Int],
                  pattern: Option[String],
                  minimum: Option[Double],
                  exclusiveMinimum: Option[Double],
                  maximum: Option[Double],
                  exclusiveMaximum: Option[Double],
                  example: Option[String],
                  pii: Option[Boolean],
                  classification: Option[String],
                  tags: Option[List[String]],
                  links: Option[Map[String, String]],
                  config: Option[FieldConfig],
                )

/**
 * Clear and concise explanations of syntax, semantic, and classification of business objects in a given domain.
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Definitions(
                      )

object SchemaTypeEnum extends Enumeration {
  val dbt, bigquery, `json-schema`, `sql-ddl`, avro, protobuf, custom = Value
}
class SchemaTypeEnumRef extends TypeReference[SchemaTypeEnum.type]

/**
 * The schema of the data contract describes the syntax and semantics of provided data sets. It supports different schema types.
 *
 * @param type The type of the schema. Typical values are dbt, bigquery, json-schema, sql-ddl, avro, protobuf, custom.
 * @param specification
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Schema(
                   @JsonScalaEnumeration(classOf[SchemaTypeEnumRef]) `type`: SchemaTypeEnum.Value,
                   specification: Any
                 )

object ExamplesTypeEnum extends Enumeration {
  val csv, json, yaml, custom = Value
}
class ExamplesTypeEnumRef extends TypeReference[ExamplesTypeEnum.type]

/**
 * @param type        The type of the example data. Well-known types are csv, json, yaml, custom.
 * @param description An optional string describing the example.
 * @param model       The reference to the model in the schema, e.g., a table name.
 * @param data
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Examples(
                     @JsonScalaEnumeration(classOf[ExamplesTypeEnumRef]) `type`: ExamplesTypeEnum.Value,
                     description: Option[String],
                     model: Option[String],
                     data: Any
                   )


/**
 * Specifies the service level agreements for the provided data, including availability, data retention policies, latency requirements, data freshness, update frequency, support availability, and backup policies.
 *
 * @param availability Availability refers to the promise or guarantee by the service provider about the uptime of the system that provides the data.
 * @param retention    Retention covers the period how long data will be available.
 * @param latency      Latency refers to the maximum amount of time from the source to its destination.
 * @param freshness    The maximum age of the youngest row in a table.
 * @param frequency    Frequency describes how often data is updated.
 * @param support      Support describes the times when support will be available for contact.
 * @param backup       Backup specifies details about data backup procedures.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class ServiceLevels(
                          availability: Option[Availability],
                          retention: Option[Retention],
                          latency: Option[Latency],
                          freshness: Option[Freshness],
                          frequency: Option[Frequency],
                          support: Option[Support],
                          backup: Option[Backup]
                        )


/**
 * Availability refers to the promise or guarantee by the service provider about the uptime of the system that provides the data.
 *
 * @param description An optional string describing the availability service level.
 * @param percentage  An optional string describing the guaranteed uptime in percent (e.g., `99.9%`)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Availability(
                         description: Option[String],
                         percentage: Option[String]
                       ) {
  assert(percentage.forall(_.matches("^\\d+(\\.\\d+)?%$")), "`percentage` does not match the pattern")
}


/**
 * Retention covers the period how long data will be available.
 *
 * @param description    An optional string describing the retention service level.
 * @param period         An optional period of time, how long data is available. Supported formats: Simple duration (e.g., `1 year`, `30d`) and ISO 8601 duration (e.g, `P1Y`).
 * @param unlimited      An optional indicator that data is kept forever.
 * @param timestampField An optional reference to the field that contains the timestamp that the period refers to.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Retention(
                      description: Option[String],
                      period: Option[String],
                      unlimited: Option[Boolean],
                      timestampField: Option[String]
                    )


/**
 * Latency refers to the maximum amount of time from the source to its destination.
 *
 * @param description             An optional string describing the latency service level.
 * @param threshold               An optional maximum duration between the source timestamp and the processed timestamp. Supported formats: Simple duration (e.g., `24 hours`, `5s`) and ISO 8601 duration (e.g, `PT24H`).
 * @param sourceTimestampField    An optional reference to the field that contains the timestamp when the data was provided at the source.
 * @param processedTimestampField An optional reference to the field that contains the processing timestamp, which denotes when the data is made available to consumers of this data contract.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Latency(
                    description: Option[String],
                    threshold: Option[String],
                    sourceTimestampField: Option[String],
                    processedTimestampField: Option[String]
                  )


/**
 * The maximum age of the youngest row in a table.
 *
 * @param description    An optional string describing the freshness service level.
 * @param threshold      An optional maximum age of the youngest entry. Supported formats: Simple duration (e.g., `24 hours`, `5s`) and ISO 8601 duration (e.g., `PT24H`).
 * @param timestampField An optional reference to the field that contains the timestamp that the threshold refers to.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Freshness(
                      description: Option[String],
                      threshold: Option[String],
                      timestampField: Option[String]
                    )

object FrequencyTypeEnum extends Enumeration {
  val batch, `micro-batching`, streaming, manual = Value
}
class FrequencyTypeEnumRef extends TypeReference[FrequencyTypeEnum.type]

/**
 * Frequency describes how often data is updated.
 *
 * @param description An optional string describing the frequency service level.
 * @param type        The method of data processing.
 * @param interval    Optional. Only for batch: How often the pipeline is triggered, e.g., `daily`.
 * @param cron        Optional. Only for batch: A cron expression when the pipelines is triggered. E.g., `0 0 * * *`.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Frequency(
                      description: Option[String],
                      @JsonScalaEnumeration(classOf[FrequencyTypeEnumRef]) `type`: Option[FrequencyTypeEnum.Value],
                      interval: Option[String],
                      cron: Option[String]
                    )


/**
 * Support describes the times when support will be available for contact.
 *
 * @param description  An optional string describing the support service level.
 * @param time         An optional string describing the times when support will be available for contact such as `24/7` or `business hours only`.
 * @param responseTime An optional string describing the time it takes for the support team to acknowledge a request. This does not mean the issue will be resolved immediately, but it assures users that their request has been received and will be dealt with.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Support(
                    description: Option[String],
                    time: Option[String],
                    responseTime: Option[String]
                  )


/**
 * Backup specifies details about data backup procedures.
 *
 * @param description   An optional string describing the backup service level.
 * @param interval      An optional interval that defines how often data will be backed up, e.g., `daily`.
 * @param cron          An optional cron expression when data will be backed up, e.g., `0 0 * * *`.
 * @param recoveryTime  An optional Recovery Time Objective (RTO) specifies the maximum amount of time allowed to restore data from a backup after a failure or loss event (e.g., 4 hours, 24 hours).
 * @param recoveryPoint An optional Recovery Point Objective (RPO) defines the maximum acceptable age of files that must be recovered from backup storage for normal operations to resume after a disaster or data loss event. This essentially measures how much data you can afford to lose, measured in time (e.g., 4 hours, 24 hours).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Backup(
                   description: Option[String],
                   interval: Option[String],
                   cron: Option[String],
                   recoveryTime: Option[String],
                   recoveryPoint: Option[String]
                 )

object ExpectationTypeEnum extends Enumeration {
  val SodaCL, montecarlo, `great-expectations`, custom = Value
}
class ExpectationTypeEnumRef extends TypeReference[ExpectationTypeEnum.type]

/**
 * The quality object contains quality attributes and checks.
 *
 * @param type The type of the quality check. Typical values are SodaCL, montecarlo, great-expectations, custom.
 * @param specification
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Quality(
                    @JsonScalaEnumeration(classOf[ExpectationTypeEnumRef]) `type`: ExpectationTypeEnum.Value,
                    specification: Any
                  )


/**
 * Links to external resources.
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Links(
                  links: Map[String, String]
                )

