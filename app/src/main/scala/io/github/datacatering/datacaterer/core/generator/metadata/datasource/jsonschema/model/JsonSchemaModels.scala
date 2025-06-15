package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties, JsonProperty}

import scala.annotation.meta.field

/**
 * Main JSON Schema definition containing all schema properties
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class JsonSchemaDefinition(
                                 @(JsonProperty @field)("$schema") schemaVersion: Option[String] = None,
                                 @(JsonProperty @field)("$id") id: Option[String] = None,
                                 title: Option[String] = None,
                                 description: Option[String] = None,
                                 `type`: Option[String] = None,
                                 properties: Option[Map[String, JsonSchemaProperty]] = None,
                                 required: Option[List[String]] = None,
                                 additionalProperties: Option[Boolean] = None,
                                 items: Option[JsonSchemaProperty] = None,
                                 @(JsonProperty @field)("$ref") ref: Option[String] = None,
                                 allOf: Option[List[JsonSchemaProperty]] = None,
                                 oneOf: Option[List[JsonSchemaProperty]] = None,
                                 anyOf: Option[List[JsonSchemaProperty]] = None,
                                 not: Option[JsonSchemaProperty] = None,
                                 // Validation constraints
                                 pattern: Option[String] = None,
                                 format: Option[String] = None,
                                 minimum: Option[String] = None,
                                 maximum: Option[String] = None,
                                 exclusiveMinimum: Option[String] = None,
                                 exclusiveMaximum: Option[String] = None,
                                 multipleOf: Option[String] = None,
                                 minLength: Option[Int] = None,
                                 maxLength: Option[Int] = None,
                                 minItems: Option[Int] = None,
                                 maxItems: Option[Int] = None,
                                 uniqueItems: Option[Boolean] = None,
                                 minProperties: Option[Int] = None,
                                 maxProperties: Option[Int] = None,
                                 `enum`: Option[List[Any]] = None,
                                 const: Option[Any] = None,
                                 `default`: Option[Any] = None,
                                 examples: Option[List[Any]] = None,
                                 // New JSON Schema Draft 2019-09 / 2020-12 features
                                 minContains: Option[Int] = None,
                                 maxContains: Option[Int] = None,
                                 contains: Option[JsonSchemaProperty] = None,
                                 prefixItems: Option[List[JsonSchemaProperty]] = None,
                                 unevaluatedItems: Option[JsonSchemaProperty] = None,
                                 unevaluatedProperties: Option[JsonSchemaProperty] = None,
                                 dependentRequired: Option[Map[String, List[String]]] = None,
                                 dependentSchemas: Option[Map[String, JsonSchemaProperty]] = None,
                                 if_ : Option[JsonSchemaProperty] = None,
                                 then_ : Option[JsonSchemaProperty] = None,
                                 else_ : Option[JsonSchemaProperty] = None,
                                 // JSON Schema definitions for reusable components
                                 definitions: Option[Map[String, JsonSchemaProperty]] = None
                               ) {

  @JsonIgnore
  def isArrayType: Boolean = `type`.contains("array")

  @JsonIgnore
  def isObjectType: Boolean = `type`.contains("object")

  @JsonIgnore
  def isStringType: Boolean = `type`.contains("string")

  @JsonIgnore
  def isNumberType: Boolean = `type`.exists(t => t == "number" || t == "integer")

  @JsonIgnore
  def isBooleanType: Boolean = `type`.contains("boolean")

  @JsonIgnore
  def isNullType: Boolean = `type`.contains("null")

  @JsonIgnore
  def hasReference: Boolean = ref.isDefined

  @JsonIgnore
  def hasComposition: Boolean = allOf.isDefined || oneOf.isDefined || anyOf.isDefined

  @JsonIgnore
  def getSchemaVersion: JsonSchemaVersion = {
    schemaVersion match {
      case Some(version) => JsonSchemaVersion.fromString(version)
      case None => JsonSchemaVersion.Draft202012 // Default to latest
    }
  }
}

/**
 * JSON Schema property definition supporting all schema features
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class JsonSchemaProperty(
                               title: Option[String] = None,
                               description: Option[String] = None,
                               `type`: Option[String] = None,
                               properties: Option[Map[String, JsonSchemaProperty]] = None,
                               required: Option[List[String]] = None,
                               additionalProperties: Option[Boolean] = None,
                               items: Option[JsonSchemaProperty] = None,
                               @(JsonProperty @field)("$ref") ref: Option[String] = None,
                               allOf: Option[List[JsonSchemaProperty]] = None,
                               oneOf: Option[List[JsonSchemaProperty]] = None,
                               anyOf: Option[List[JsonSchemaProperty]] = None,
                               not: Option[JsonSchemaProperty] = None,
                               // Validation constraints
                               pattern: Option[String] = None,
                               format: Option[String] = None,
                               minimum: Option[String] = None,
                               maximum: Option[String] = None,
                               exclusiveMinimum: Option[String] = None,
                               exclusiveMaximum: Option[String] = None,
                               multipleOf: Option[String] = None,
                               minLength: Option[Int] = None,
                               maxLength: Option[Int] = None,
                               minItems: Option[Int] = None,
                               maxItems: Option[Int] = None,
                               uniqueItems: Option[Boolean] = None,
                               minProperties: Option[Int] = None,
                               maxProperties: Option[Int] = None,
                               `enum`: Option[List[Any]] = None,
                               const: Option[Any] = None,
                               `default`: Option[Any] = None,
                               examples: Option[List[Any]] = None,
                               // New JSON Schema Draft 2019-09 / 2020-12 features
                               minContains: Option[Int] = None,
                               maxContains: Option[Int] = None,
                               contains: Option[JsonSchemaProperty] = None,
                               prefixItems: Option[List[JsonSchemaProperty]] = None,
                               unevaluatedItems: Option[JsonSchemaProperty] = None,
                               unevaluatedProperties: Option[JsonSchemaProperty] = None,
                               dependentRequired: Option[Map[String, List[String]]] = None,
                               dependentSchemas: Option[Map[String, JsonSchemaProperty]] = None,
                               if_ : Option[JsonSchemaProperty] = None,
                               then_ : Option[JsonSchemaProperty] = None,
                               else_ : Option[JsonSchemaProperty] = None
                             ) {

  @JsonIgnore
  def isArrayType: Boolean = `type`.contains("array")

  @JsonIgnore
  def isObjectType: Boolean = `type`.contains("object")

  @JsonIgnore
  def isStringType: Boolean = `type`.contains("string")

  @JsonIgnore
  def isNumberType: Boolean = `type`.exists(t => t == "number" || t == "integer")

  @JsonIgnore
  def isBooleanType: Boolean = `type`.contains("boolean")

  @JsonIgnore
  def isNullType: Boolean = `type`.contains("null")

  @JsonIgnore
  def hasReference: Boolean = ref.isDefined

  @JsonIgnore
  def hasComposition: Boolean = allOf.isDefined || oneOf.isDefined || anyOf.isDefined

  @JsonIgnore
  def getConstraints: JsonSchemaConstraints = {
    JsonSchemaConstraints(
      pattern = pattern,
      format = format,
      minimum = minimum,
      maximum = maximum,
      exclusiveMinimum = exclusiveMinimum,
      exclusiveMaximum = exclusiveMaximum,
      multipleOf = multipleOf,
      minLength = minLength,
      maxLength = maxLength,
      minItems = minItems,
      maxItems = maxItems,
      uniqueItems = uniqueItems,
      minProperties = minProperties,
      maxProperties = maxProperties,
      `enum` = `enum`,
      const = const,
      `default` = `default`,
      examples = examples,
      minContains = minContains,
      maxContains = maxContains
    )
  }
}

/**
 * JSON Schema validation constraints extracted from property
 */
case class JsonSchemaConstraints(
                                  pattern: Option[String] = None,
                                  format: Option[String] = None,
                                                                                                     minimum: Option[String] = None,
                                maximum: Option[String] = None,
                                exclusiveMinimum: Option[String] = None,
                                exclusiveMaximum: Option[String] = None,
                                multipleOf: Option[String] = None,
                                  minLength: Option[Int] = None,
                                  maxLength: Option[Int] = None,
                                  minItems: Option[Int] = None,
                                  maxItems: Option[Int] = None,
                                  uniqueItems: Option[Boolean] = None,
                                  minProperties: Option[Int] = None,
                                  maxProperties: Option[Int] = None,
                                  `enum`: Option[List[Any]] = None,
                                  const: Option[Any] = None,
                                  `default`: Option[Any] = None,
                                  examples: Option[List[Any]] = None,
                                  minContains: Option[Int] = None,
                                  maxContains: Option[Int] = None
                                ) {

  @JsonIgnore
  def hasStringConstraints: Boolean = pattern.isDefined || format.isDefined || minLength.isDefined || maxLength.isDefined

  @JsonIgnore
  def hasNumericConstraints: Boolean = minimum.isDefined || maximum.isDefined || exclusiveMinimum.isDefined || 
    exclusiveMaximum.isDefined || multipleOf.isDefined

  @JsonIgnore
  def hasArrayConstraints: Boolean = minItems.isDefined || maxItems.isDefined || uniqueItems.isDefined || 
    minContains.isDefined || maxContains.isDefined

  @JsonIgnore
  def hasObjectConstraints: Boolean = minProperties.isDefined || maxProperties.isDefined

  @JsonIgnore
  def hasValueConstraints: Boolean = `enum`.isDefined || const.isDefined

  @JsonIgnore
  def isEmpty: Boolean = !hasStringConstraints && !hasNumericConstraints && !hasArrayConstraints && 
    !hasObjectConstraints && !hasValueConstraints
}

/**
 * JSON Schema version enumeration
 */
sealed trait JsonSchemaVersion {
  def version: String
  def url: String
}

object JsonSchemaVersion {
  case object Draft4 extends JsonSchemaVersion {
    override val version: String = "draft-04"
    override val url: String = "http://json-schema.org/draft-04/schema#"
  }

  case object Draft6 extends JsonSchemaVersion {
    override val version: String = "draft-06"
    override val url: String = "http://json-schema.org/draft-06/schema#"
  }

  case object Draft7 extends JsonSchemaVersion {
    override val version: String = "draft-07"
    override val url: String = "http://json-schema.org/draft-07/schema#"
  }

  case object Draft201909 extends JsonSchemaVersion {
    override val version: String = "2019-09"
    override val url: String = "https://json-schema.org/draft/2019-09/schema"
  }

  case object Draft202012 extends JsonSchemaVersion {
    override val version: String = "2020-12"
    override val url: String = "https://json-schema.org/draft/2020-12/schema"
  }

  def fromString(versionString: String): JsonSchemaVersion = {
    versionString.toLowerCase match {
      case url if url.contains("draft-04") || url.contains("draft/4") => Draft4
      case url if url.contains("draft-06") || url.contains("draft/6") => Draft6
      case url if url.contains("draft-07") || url.contains("draft/7") => Draft7
      case url if url.contains("2019-09") => Draft201909
      case url if url.contains("2020-12") => Draft202012
      case _ => Draft202012 // Default to latest
    }
  }

  def allVersions: List[JsonSchemaVersion] = List(Draft4, Draft6, Draft7, Draft201909, Draft202012)
}

/**
 * JSON Schema type enumeration
 */
object JsonSchemaType {
  val STRING = "string"
  val NUMBER = "number"
  val INTEGER = "integer"
  val BOOLEAN = "boolean"
  val ARRAY = "array"
  val OBJECT = "object"
  val NULL = "null"

  val ALL_TYPES: List[String] = List(STRING, NUMBER, INTEGER, BOOLEAN, ARRAY, OBJECT, NULL)

  def isValidType(typeString: String): Boolean = ALL_TYPES.contains(typeString)
}

/**
 * JSON Schema format enumeration (common formats)
 */
object JsonSchemaFormat {
  val DATE_TIME = "date-time"
  val DATE = "date"
  val TIME = "time"
  val EMAIL = "email"
  val HOSTNAME = "hostname"
  val IPV4 = "ipv4"
  val IPV6 = "ipv6"
  val URI = "uri"
  val URI_REFERENCE = "uri-reference"
  val URI_TEMPLATE = "uri-template"
  val UUID = "uuid"
  val REGEX = "regex"
  val JSON_POINTER = "json-pointer"
  val RELATIVE_JSON_POINTER = "relative-json-pointer"

  val ALL_FORMATS: List[String] = List(
    DATE_TIME, DATE, TIME, EMAIL, HOSTNAME, IPV4, IPV6, URI, URI_REFERENCE, 
    URI_TEMPLATE, UUID, REGEX, JSON_POINTER, RELATIVE_JSON_POINTER
  )

  def isValidFormat(formatString: String): Boolean = ALL_FORMATS.contains(formatString)
}

/**
 * Exception classes for JSON Schema processing
 */
case class JsonSchemaParseException(message: String, cause: Throwable = null) extends Exception(message, cause)

case class JsonSchemaValidationException(message: String, cause: Throwable = null) extends Exception(message, cause)

case class JsonSchemaUnsupportedFeatureException(feature: String, version: JsonSchemaVersion) 
  extends Exception(s"Feature '$feature' is not supported in JSON Schema version '${version.version}'")

case class JsonSchemaReferenceResolutionException(reference: String, cause: Throwable = null) 
  extends Exception(s"Failed to resolve JSON Schema reference: $reference", cause) 