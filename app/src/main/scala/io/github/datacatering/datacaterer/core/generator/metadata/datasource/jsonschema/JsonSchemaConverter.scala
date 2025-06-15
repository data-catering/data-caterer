package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.JsonSchemaConstraintsConverter._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.JsonSchemaReferenceResolver._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.JsonSchemaTypeMapper._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model._
import org.apache.log4j.Logger

/**
 * Converts JSON Schema definitions to Data Caterer Field models
 * This is a refactored version that uses specialized utility classes for better maintainability
 */
object JsonSchemaConverter {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Convert JSON schema definition to Data Caterer fields with reference resolution
   */
  def convertSchemaWithRefs(schema: JsonSchemaDefinition, schemaSource: String): List[Field] = {
    LOGGER.info(s"Converting JSON schema with $$ref support from $schemaSource, type: ${schema.`type`.getOrElse("undefined")}")
    
    val referenceContext = ReferenceResolutionContext(schema.definitions.getOrElse(Map()), schemaSource)
    convertSchemaInternal(schema, Some(referenceContext), schemaSource)
  }

  /**
   * Convert JSON schema definition to Data Caterer fields without reference resolution (legacy support)
   */
  def convertSchema(schema: JsonSchemaDefinition, schemaSource: String): List[Field] = {
    LOGGER.info(s"Converting JSON schema without $$ref support from $schemaSource, type: ${schema.`type`.getOrElse("undefined")}")
    
    convertSchemaInternal(schema, None, schemaSource)
  }

  /**
   * Internal method for converting schema with optional reference resolution
   */
  private def convertSchemaInternal(
                                     schema: JsonSchemaDefinition, 
                                     refContext: Option[ReferenceResolutionContext],
                                     schemaSource: String
                                   ): List[Field] = {
    try {
      schema.`type` match {
        case Some("object") =>
          schema.properties match {
            case Some(props) => convertObjectProperties(props, schema.required.getOrElse(List()), refContext, schemaSource)
            case None => 
              LOGGER.warn(s"Object schema has no properties defined in $schemaSource")
              List()
          }
        case Some("array") =>
          schema.items match {
            case Some(itemsSchema) =>
              val arrayField = convertArrayFromDefinition("root_array", itemsSchema, schema, refContext, schemaSource)
              List(arrayField)
            case None =>
              LOGGER.warn(s"Array schema has no items defined in $schemaSource")
              List(createSimpleField("root_array", ArrayType))
          }
        case Some(primitiveType) =>
          val field = convertPrimitiveFromDefinition("root_value", primitiveType, schema, schemaSource)
          List(field)
        case None =>
          handleUntyped(schema, refContext, schemaSource)
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to convert JSON schema from $schemaSource", ex)
        throw JsonSchemaValidationException(s"Failed to convert JSON schema from $schemaSource: ${ex.getMessage}", ex)
    }
  }

  /**
   * Handle schemas without explicit type (composition, references, etc.)
   */
  private def handleUntyped(
                             schema: JsonSchemaDefinition,
                             refContext: Option[ReferenceResolutionContext],
                             schemaSource: String
                           ): List[Field] = {
    if (schema.hasComposition) {
      JsonSchemaCompositionConverter.convertCompositionFromDefinition("root", schema, refContext, schemaSource)
    } else if (schema.hasReference && refContext.isDefined) {
      schema.ref match {
        case Some(refString) =>
          LOGGER.info(s"Resolving root-level reference: $refString in $schemaSource")
          val resolvedSchema = resolveReference(refString, refContext.get)
          convertProperty("root", resolvedSchema, isRequired = true, refContext, schemaSource)
            .map(List(_))
            .getOrElse(List(createSimpleField("root_ref", StringType)))
        case None =>
          LOGGER.warn(s"Schema marked as having reference but ref field is empty in $schemaSource")
          List(createSimpleField("root_ref", StringType))
      }
    } else {
      LOGGER.warn(s"Schema has no type defined and no composition in $schemaSource")
      List(createSimpleField("root", StringType))
    }
  }

  /**
   * Convert object properties
   */
  private def convertObjectProperties(
                                       properties: Map[String, JsonSchemaProperty], 
                                       required: List[String], 
                                       refContext: Option[ReferenceResolutionContext],
                                       schemaSource: String
                                     ): List[Field] = {
    properties.flatMap { case (propertyName, propertySchema) =>
      // Filter out JSON schema metadata fields that start with '$'
      if (propertyName.startsWith("$")) {
        LOGGER.debug(s"Filtering out JSON schema metadata field '$propertyName' from $schemaSource")
        None
      } else {
        convertProperty(propertyName, propertySchema, required.contains(propertyName), refContext, schemaSource)
      }
    }.toList
  }

  /**
   * Convert a single property with optional reference resolution
   */
  private def convertProperty(
                               fieldName: String, 
                               property: JsonSchemaProperty, 
                               isRequired: Boolean,
                               refContext: Option[ReferenceResolutionContext],
                               schemaSource: String
                             ): Option[Field] = {
    LOGGER.debug(s"Converting property '$fieldName' with type '${property.`type`.getOrElse("undefined")}' from $schemaSource")

    // Check for reference first if we have a reference context
    if (property.hasReference && refContext.isDefined) {
      property.ref match {
        case Some(refString) =>
          LOGGER.debug(s"Resolving reference for field '$fieldName': $refString")
          try {
            val resolvedSchema = resolveReference(refString, refContext.get)
            return convertProperty(fieldName, resolvedSchema, isRequired, refContext, s"$schemaSource->$refString")
          } catch {
            case ex: JsonSchemaReferenceResolutionException =>
              LOGGER.error(s"Failed to resolve reference '$refString' for field '$fieldName' in $schemaSource", ex)
              return Some(createSimpleField(fieldName, StringType, isRequired))
          }
        case None =>
          LOGGER.warn(s"Property '$fieldName' marked as having reference but ref field is empty in $schemaSource")
          return Some(createSimpleField(fieldName, StringType, isRequired))
      }
    }

    // Handle typed properties
    property.`type` match {
      case Some("object") =>
        Some(convertObjectField(fieldName, property, isRequired, refContext, schemaSource))
      case Some("array") =>
        Some(convertArrayField(fieldName, property, isRequired, refContext, schemaSource))
      case Some(primitiveType) =>
        Some(convertPrimitiveField(fieldName, primitiveType, property, isRequired))
      case None =>
        if (property.hasComposition) {
          Some(JsonSchemaCompositionConverter.convertCompositionField(fieldName, property, isRequired, refContext, schemaSource))
        } else {
          LOGGER.warn(s"Property '$fieldName' has no type defined in $schemaSource, defaulting to string")
          Some(createSimpleField(fieldName, StringType, isRequired))
        }
    }
  }

  /**
   * Convert object field with nested properties
   */
  private def convertObjectField(
                                  fieldName: String, 
                                  property: JsonSchemaProperty, 
                                  isRequired: Boolean,
                                  refContext: Option[ReferenceResolutionContext],
                                  schemaSource: String
                                ): Field = {
    property.properties match {
      case Some(nestedProps) =>
        val nestedFields = convertObjectProperties(nestedProps, property.required.getOrElse(List()), refContext, s"$schemaSource.$fieldName")
        if (nestedFields.nonEmpty) {
          val structType = new StructType(nestedFields.map(f => f.name -> getDataTypeFromField(f)))
          Field(
            name = fieldName,
            `type` = Some(structType.toString),
            options = convertConstraintsToOptions(property.getConstraints),
            nullable = !isRequired,
            fields = nestedFields
          )
        } else {
          LOGGER.warn(s"Object field '$fieldName' has properties but all were filtered out or failed to convert in $schemaSource")
          createSimpleField(fieldName, StructType, isRequired)
        }
      case None =>
        if (property.hasComposition) {
          LOGGER.debug(s"Object field '$fieldName' has no direct properties but has composition in $schemaSource")
          JsonSchemaCompositionConverter.convertCompositionField(fieldName, property, isRequired, refContext, schemaSource)
        } else {
          LOGGER.warn(s"Object field '$fieldName' has no properties defined and no composition in $schemaSource")
          createSimpleField(fieldName, StructType, isRequired)
        }
    }
  }

  /**
   * Convert array field
   */
  private def convertArrayField(
                                 fieldName: String, 
                                 property: JsonSchemaProperty, 
                                 isRequired: Boolean,
                                 refContext: Option[ReferenceResolutionContext],
                                 schemaSource: String
                               ): Field = {
    property.items match {
      case Some(itemsSchema) =>
        JsonSchemaArrayConverter.convertArrayFromProperty(fieldName, itemsSchema, property, refContext, schemaSource, isRequired)
      case None =>
        LOGGER.warn(s"Array field '$fieldName' has no items schema defined in $schemaSource")
        createSimpleField(fieldName, ArrayType, isRequired)
    }
  }

  /**
   * Convert array from definition (for root-level arrays)
   */
  private def convertArrayFromDefinition(
                                          fieldName: String,
                                          itemsSchema: JsonSchemaProperty,
                                          parentSchema: JsonSchemaDefinition,
                                          refContext: Option[ReferenceResolutionContext],
                                          schemaSource: String,
                                          isRequired: Boolean = true
                                        ): Field = {
    JsonSchemaArrayConverter.convertArrayFromDefinition(fieldName, itemsSchema, parentSchema, refContext, schemaSource, isRequired)
  }

  /**
   * Convert primitive field
   */
  private def convertPrimitiveField(
                                     fieldName: String,
                                     jsonSchemaType: String,
                                     property: JsonSchemaProperty,
                                     isRequired: Boolean
                                   ): Field = {
    val dataType = mapJsonSchemaTypeToDataCaterer(jsonSchemaType)
    val constraints = property.getConstraints
    val options = convertConstraintsToOptions(constraints)
    
    Field(
      name = fieldName,
      `type` = Some(dataType.toString),
      options = options,
      nullable = !isRequired
    )
  }

  /**
   * Convert primitive schema (for root-level primitives)
   */
  private def convertPrimitiveFromDefinition(
                                              fieldName: String,
                                              jsonSchemaType: String,
                                              schema: JsonSchemaDefinition,
                                              schemaSource: String
                                            ): Field = {
    val dataType = mapJsonSchemaTypeToDataCaterer(jsonSchemaType)
    val constraints = JsonSchemaConstraints(
      pattern = schema.pattern,
      format = schema.format,
      minimum = schema.minimum,
      maximum = schema.maximum,
      exclusiveMinimum = schema.exclusiveMinimum,
      exclusiveMaximum = schema.exclusiveMaximum,
      multipleOf = schema.multipleOf,
      minLength = schema.minLength,
      maxLength = schema.maxLength,
      `enum` = schema.`enum`,
      const = schema.const,
      `default` = schema.`default`,
      examples = schema.examples
    )
    val options = convertConstraintsToOptions(constraints)
    
    Field(
      name = fieldName,
      `type` = Some(dataType.toString),
      options = options,
      nullable = false
    )
  }
} 