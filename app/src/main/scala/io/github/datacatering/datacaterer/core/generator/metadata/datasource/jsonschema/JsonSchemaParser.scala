package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model._
import org.apache.log4j.Logger

/**
 * Parser for JSON Schema files/URLs that converts JSON to JsonSchemaDefinition objects
 */
object JsonSchemaParser {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)

  /**
   * Parse a JSON schema from a file path or URL
   *
   * @param source the file path or URL to the JSON schema
   * @return the parsed JsonSchemaDefinition
   * @throws JsonSchemaParseException if parsing fails
   */
  def parseSchema(source: String): JsonSchemaDefinition = {
    LOGGER.info(s"Parsing JSON schema from source: $source")
    
    try {
      // Load the schema using JsonSchemaLoader
      val jsonSchema = JsonSchemaLoader.loadSchema(source)
      val schemaNode = jsonSchema.getSchemaNode
      
      // Convert Jackson JsonNode to JsonSchemaDefinition
      val jsonSchemaDefinition = objectMapper.treeToValue(schemaNode, classOf[JsonSchemaDefinition])
      
      // Validate the parsed schema
      validateParsedSchema(jsonSchemaDefinition, source)
      
      LOGGER.info(s"Successfully parsed JSON schema from $source. Version: ${jsonSchemaDefinition.getSchemaVersion.version}")
      jsonSchemaDefinition
      
    } catch {
      case ex: JsonSchemaLoadException =>
        LOGGER.error(s"Failed to load JSON schema from $source", ex)
        throw JsonSchemaParseException(s"Failed to load JSON schema from $source: ${ex.getMessage}", ex)
      case ex: Exception =>
        LOGGER.error(s"Failed to parse JSON schema from $source", ex)
        throw JsonSchemaParseException(s"Failed to parse JSON schema from $source: ${ex.getMessage}", ex)
    }
  }

  /**
   * Parse a JSON schema from raw JSON string
   *
   * @param jsonString the raw JSON schema string
   * @return the parsed JsonSchemaDefinition
   * @throws JsonSchemaParseException if parsing fails
   */
  def parseSchemaFromString(jsonString: String): JsonSchemaDefinition = {
    LOGGER.info("Parsing JSON schema from string")
    
    try {
      val jsonSchemaDefinition = objectMapper.readValue(jsonString, classOf[JsonSchemaDefinition])
      validateParsedSchema(jsonSchemaDefinition, "string input")
      
      LOGGER.info(s"Successfully parsed JSON schema from string. Version: ${jsonSchemaDefinition.getSchemaVersion.version}")
      jsonSchemaDefinition
      
    } catch {
      case ex: Exception =>
        LOGGER.error("Failed to parse JSON schema from string", ex)
        throw JsonSchemaParseException(s"Failed to parse JSON schema from string: ${ex.getMessage}", ex)
    }
  }

  /**
   * Detect the JSON Schema version from a source
   *
   * @param source the file path or URL to the JSON schema
   * @return the detected JsonSchemaVersion
   */
  def detectSchemaVersion(source: String): JsonSchemaVersion = {
    LOGGER.info(s"Detecting JSON schema version from source: $source")
    
    try {
      val jsonSchema = JsonSchemaLoader.loadSchema(source)
      val schemaNode = jsonSchema.getSchemaNode
      
      // Check for $schema property
      val schemaVersionOpt = Option(schemaNode.get("$schema")).map(_.asText())
      
      val detectedVersion = schemaVersionOpt match {
        case Some(versionString) => 
          val version = JsonSchemaVersion.fromString(versionString)
          LOGGER.info(s"Detected JSON schema version from $$schema property: ${version.version}")
          version
        case None =>
          // Try to detect based on features present
          val version = detectVersionFromFeatures(schemaNode)
          LOGGER.info(s"Detected JSON schema version from features: ${version.version}")
          version
      }
      
      detectedVersion
      
    } catch {
      case ex: Exception =>
        LOGGER.warn(s"Failed to detect JSON schema version from $source, defaulting to latest", ex)
        JsonSchemaVersion.Draft202012
    }
  }

  /**
   * Validate that a parsed schema is structurally correct
   *
   * @param schema the parsed JsonSchemaDefinition
   * @param source the source identifier for error messages
   */
  private def validateParsedSchema(schema: JsonSchemaDefinition, source: String): Unit = {
    // Basic validation - check for required structure
    if (schema.`type`.isEmpty && schema.properties.isEmpty && schema.ref.isEmpty && !schema.hasComposition) {
      throw JsonSchemaParseException(s"Invalid JSON schema from $source: missing type, properties, reference, or composition")
    }
    
    // Validate type if present
    schema.`type`.foreach { typeString =>
      if (!JsonSchemaType.isValidType(typeString)) {
        throw JsonSchemaParseException(s"Invalid JSON schema type '$typeString' in schema from $source")
      }
    }
    
    // Validate format if present
    schema.format.foreach { formatString =>
      if (!JsonSchemaFormat.isValidFormat(formatString)) {
        LOGGER.warn(s"Unknown JSON schema format '$formatString' in schema from $source")
      }
    }
    
    // Validate numeric constraints
    validateNumericConstraints(schema, source)
    
    // Validate array constraints  
    validateArrayConstraints(schema, source)
    
    // Recursively validate nested properties
    schema.properties.foreach { props =>
      props.foreach { case (propName, propSchema) =>
        validatePropertySchema(propSchema, s"$source.properties.$propName")
      }
    }
    
    // Validate composition schemas
    validateCompositionSchemas(schema, source)
  }

  /**
   * Validate numeric constraints in a schema
   */
  private def validateNumericConstraints(schema: JsonSchemaDefinition, source: String): Unit = {
    (schema.minimum, schema.maximum) match {
      case (Some(min), Some(max)) if min > max =>
        throw JsonSchemaParseException(s"Invalid numeric constraints in $source: minimum ($min) > maximum ($max)")
      case _ => // Valid
    }
    
    (schema.exclusiveMinimum, schema.exclusiveMaximum) match {
      case (Some(min), Some(max)) if min >= max =>
        throw JsonSchemaParseException(s"Invalid exclusive numeric constraints in $source: exclusiveMinimum ($min) >= exclusiveMaximum ($max)")
      case _ => // Valid
    }
  }

  /**
   * Validate array constraints in a schema
   */
  private def validateArrayConstraints(schema: JsonSchemaDefinition, source: String): Unit = {
    (schema.minItems, schema.maxItems) match {
      case (Some(min), Some(max)) if min > max =>
        throw JsonSchemaParseException(s"Invalid array constraints in $source: minItems ($min) > maxItems ($max)")
      case _ => // Valid
    }
    
    (schema.minLength, schema.maxLength) match {
      case (Some(min), Some(max)) if min > max =>
        throw JsonSchemaParseException(s"Invalid string length constraints in $source: minLength ($min) > maxLength ($max)")
      case _ => // Valid
    }
  }

  /**
   * Recursively validate a property schema
   */
  private def validatePropertySchema(propSchema: JsonSchemaProperty, source: String): Unit = {
    // Validate type if present
    propSchema.`type`.foreach { typeString =>
      if (!JsonSchemaType.isValidType(typeString)) {
        throw JsonSchemaParseException(s"Invalid JSON schema type '$typeString' in $source")
      }
    }
    
    // Validate format if present
    propSchema.format.foreach { formatString =>
      if (!JsonSchemaFormat.isValidFormat(formatString)) {
        LOGGER.warn(s"Unknown JSON schema format '$formatString' in $source")
      }
    }
    
    // Recursively validate nested properties
    propSchema.properties.foreach { props =>
      props.foreach { case (propName, nestedSchema) =>
        validatePropertySchema(nestedSchema, s"$source.properties.$propName")
      }
    }
    
    // Validate items schema for arrays
    propSchema.items.foreach { itemsSchema =>
      validatePropertySchema(itemsSchema, s"$source.items")
    }
  }

  /**
   * Validate composition schemas (allOf, oneOf, anyOf)
   */
  private def validateCompositionSchemas(schema: JsonSchemaDefinition, source: String): Unit = {
    schema.allOf.foreach { schemas =>
      schemas.zipWithIndex.foreach { case (subSchema, index) =>
        validatePropertySchema(subSchema, s"$source.allOf[$index]")
      }
    }
    
    schema.oneOf.foreach { schemas =>
      schemas.zipWithIndex.foreach { case (subSchema, index) =>
        validatePropertySchema(subSchema, s"$source.oneOf[$index]")
      }
    }
    
    schema.anyOf.foreach { schemas =>
      schemas.zipWithIndex.foreach { case (subSchema, index) =>
        validatePropertySchema(subSchema, s"$source.anyOf[$index]")
      }
    }
  }

  /**
   * Detect JSON Schema version based on features present in the schema
   */
  private def detectVersionFromFeatures(schemaNode: com.fasterxml.jackson.databind.JsonNode): JsonSchemaVersion = {
    // Check for Draft 2020-12 features
    if (schemaNode.has("prefixItems") || schemaNode.has("unevaluatedItems") || 
        schemaNode.has("unevaluatedProperties") || schemaNode.has("dependentSchemas")) {
      return JsonSchemaVersion.Draft202012
    }
    
    // Check for Draft 2019-09 features
    if (schemaNode.has("minContains") || schemaNode.has("maxContains") || 
        schemaNode.has("dependentRequired") || schemaNode.has("if")) {
      return JsonSchemaVersion.Draft201909
    }
    
    // Check for Draft 7 features
    if (schemaNode.has("if") || schemaNode.has("then") || schemaNode.has("else") ||
        schemaNode.has("const") || schemaNode.has("contains")) {
      return JsonSchemaVersion.Draft7
    }
    
    // Check for Draft 6 features
    if (schemaNode.has("const") || schemaNode.has("contains") || 
        schemaNode.has("propertyNames") || schemaNode.has("examples")) {
      return JsonSchemaVersion.Draft6
    }
    
    // Default to Draft 4 if no newer features detected
    JsonSchemaVersion.Draft4
  }

  /**
   * Check if a schema version supports a specific feature
   *
   * @param version the JSON Schema version
   * @param feature the feature name
   * @return true if the feature is supported
   */
  def isFeatureSupported(version: JsonSchemaVersion, feature: String): Boolean = {
    (version, feature) match {
      case (JsonSchemaVersion.Draft4, f) => 
        !List("const", "contains", "propertyNames", "examples", "if", "then", "else", 
              "minContains", "maxContains", "dependentRequired", "dependentSchemas",
              "prefixItems", "unevaluatedItems", "unevaluatedProperties").contains(f)
      case (JsonSchemaVersion.Draft6, f) => 
        !List("if", "then", "else", "minContains", "maxContains", "dependentRequired", 
              "dependentSchemas", "prefixItems", "unevaluatedItems", "unevaluatedProperties").contains(f)
      case (JsonSchemaVersion.Draft7, f) => 
        !List("minContains", "maxContains", "dependentRequired", "dependentSchemas",
              "prefixItems", "unevaluatedItems", "unevaluatedProperties").contains(f)
      case (JsonSchemaVersion.Draft201909, f) => 
        !List("prefixItems", "unevaluatedItems", "unevaluatedProperties").contains(f)
      case (JsonSchemaVersion.Draft202012, _) => true // Latest version supports all features
    }
  }
} 