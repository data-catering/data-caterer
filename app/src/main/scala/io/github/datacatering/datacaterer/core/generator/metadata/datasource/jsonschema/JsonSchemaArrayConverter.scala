package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.JsonSchemaConstraintsConverter._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.JsonSchemaReferenceResolver._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.JsonSchemaTypeMapper._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model._
import org.apache.log4j.Logger

/**
 * Handles conversion of JSON Schema array types to Data Caterer Fields
 */
object JsonSchemaArrayConverter {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Convert array from JsonSchemaDefinition (for root-level arrays)
   */
  def convertArrayFromDefinition(
                                  fieldName: String,
                                  itemsSchema: JsonSchemaProperty,
                                  parentSchema: JsonSchemaDefinition,
                                  refContext: Option[ReferenceResolutionContext],
                                  schemaSource: String,
                                  isRequired: Boolean = true
                                ): Field = {
    // Check if items schema has a reference and resolve it
    val resolvedItemsSchema = if (itemsSchema.hasReference && refContext.isDefined) {
      itemsSchema.ref match {
        case Some(refString) =>
          LOGGER.debug(s"Resolving reference for array items '$fieldName': $refString")
          resolveReference(refString, refContext.get)
        case None =>
          LOGGER.warn(s"Array items schema marked as having reference but ref field is empty for field '$fieldName' in $schemaSource")
          itemsSchema
      }
    } else {
      itemsSchema
    }

    resolvedItemsSchema.`type` match {
      case Some("object") =>
        convertArrayOfObjects(fieldName, resolvedItemsSchema, convertArrayConstraintsFromDefinition(parentSchema), isRequired, refContext, schemaSource)
      case Some(primitiveType) =>
        convertArrayOfPrimitives(fieldName, primitiveType, convertArrayConstraintsFromDefinition(parentSchema), isRequired)
      case None =>
        if (resolvedItemsSchema.hasComposition) {
          LOGGER.warn(s"Array items with composition schemas not fully supported for field '$fieldName' in $schemaSource")
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        } else {
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        }
    }
  }

  /**
   * Convert array from JsonSchemaProperty (for nested arrays)
   */
  def convertArrayFromProperty(
                                fieldName: String,
                                itemsSchema: JsonSchemaProperty,
                                parentProperty: JsonSchemaProperty,
                                refContext: Option[ReferenceResolutionContext],
                                schemaSource: String,
                                isRequired: Boolean = true
                              ): Field = {
    // Check if items schema has a reference and resolve it
    val resolvedItemsSchema = if (itemsSchema.hasReference && refContext.isDefined) {
      itemsSchema.ref match {
        case Some(refString) =>
          LOGGER.debug(s"Resolving reference for array items '$fieldName': $refString")
          resolveReference(refString, refContext.get)
        case None =>
          LOGGER.warn(s"Array items schema marked as having reference but ref field is empty for field '$fieldName' in $schemaSource")
          itemsSchema
      }
    } else {
      itemsSchema
    }

    val baseConstraints = convertConstraintsToOptions(parentProperty.getConstraints)
    val arrayConstraints = convertArrayConstraints(parentProperty)

    resolvedItemsSchema.`type` match {
      case Some("object") =>
        convertArrayOfObjects(fieldName, resolvedItemsSchema, baseConstraints ++ arrayConstraints, isRequired, refContext, schemaSource)
      case Some(primitiveType) =>
        convertArrayOfPrimitives(fieldName, primitiveType, baseConstraints ++ arrayConstraints, isRequired)
      case None =>
        if (resolvedItemsSchema.hasComposition) {
          LOGGER.warn(s"Array items with composition schemas not fully supported for field '$fieldName' in $schemaSource")
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        } else {
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        }
    }
  }

  /**
   * Convert array of objects
   */
  private def convertArrayOfObjects(
                                     fieldName: String,
                                     itemsSchema: JsonSchemaProperty,
                                     constraints: Map[String, Any],
                                     isRequired: Boolean,
                                     refContext: Option[ReferenceResolutionContext],
                                     schemaSource: String
                                   ): Field = {
    itemsSchema.properties match {
      case Some(itemProps) =>
        val nestedFields = convertObjectPropertiesForArray(itemProps, itemsSchema.required.getOrElse(List()), refContext, s"$schemaSource.$fieldName.items")
        // Create StructType directly from nested fields to avoid empty struct issue
        val elementType = if (nestedFields.nonEmpty) {
          new StructType(nestedFields.map(f => f.name -> getDataTypeFromField(f)))
        } else {
          StructType
        }
        Field(
          name = fieldName,
          `type` = Some(new ArrayType(elementType).toString),
          options = constraints,
          nullable = !isRequired,
          fields = nestedFields
        )
      case None =>
        createSimpleField(fieldName, new ArrayType(StructType), isRequired)
    }
  }

  /**
   * Convert array of primitives
   */
  private def convertArrayOfPrimitives(
                                        fieldName: String,
                                        primitiveType: String,
                                        constraints: Map[String, Any],
                                        isRequired: Boolean
                                      ): Field = {
    val elementDataType = mapJsonSchemaTypeToDataCaterer(primitiveType)
    Field(
      name = fieldName,
      `type` = Some(new ArrayType(elementDataType).toString),
      options = constraints,
      nullable = !isRequired
    )
  }

  /**
   * Convert object properties for array items (similar to main object conversion but specific for arrays)
   */
  private def convertObjectPropertiesForArray(
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
        convertPropertyForArray(propertyName, propertySchema, required.contains(propertyName), refContext, schemaSource)
      }
    }.toList
  }

  /**
   * Convert a property for array items (simplified version without full recursion)
   */
  private def convertPropertyForArray(
                                       fieldName: String, 
                                       property: JsonSchemaProperty, 
                                       isRequired: Boolean,
                                       refContext: Option[ReferenceResolutionContext],
                                       schemaSource: String
                                     ): Option[Field] = {
    LOGGER.debug(s"Converting array item property '$fieldName' with type '${property.`type`.getOrElse("undefined")}' from $schemaSource")

    // Check for reference first if we have a reference context
    if (property.hasReference && refContext.isDefined) {
      property.ref match {
        case Some(refString) =>
          LOGGER.debug(s"Resolving reference for array item field '$fieldName': $refString")
          try {
            val resolvedSchema = resolveReference(refString, refContext.get)
            return convertPropertyForArray(fieldName, resolvedSchema, isRequired, refContext, s"$schemaSource->$refString")
          } catch {
            case ex: JsonSchemaReferenceResolutionException =>
              LOGGER.error(s"Failed to resolve reference '$refString' for array item field '$fieldName' in $schemaSource", ex)
              return Some(createSimpleField(fieldName, StringType, isRequired))
          }
        case None =>
          LOGGER.warn(s"Array item property '$fieldName' marked as having reference but ref field is empty in $schemaSource")
          return Some(createSimpleField(fieldName, StringType, isRequired))
      }
    }

    // Handle typed properties
    property.`type` match {
      case Some("object") =>
        Some(convertNestedObjectForArray(fieldName, property, isRequired, refContext, schemaSource))
      case Some("array") =>
        Some(convertNestedArrayForArray(fieldName, property, isRequired, refContext, schemaSource))
      case Some(primitiveType) =>
        Some(convertPrimitiveForArray(fieldName, primitiveType, property, isRequired))
      case None =>
        if (property.hasComposition) {
          Some(JsonSchemaCompositionConverter.convertCompositionField(fieldName, property, isRequired, refContext, schemaSource))
        } else {
          LOGGER.warn(s"Array item property '$fieldName' has no type defined in $schemaSource, defaulting to string")
          Some(createSimpleField(fieldName, StringType, isRequired))
        }
    }
  }

  /**
   * Convert nested object within array
   */
  private def convertNestedObjectForArray(
                                           fieldName: String, 
                                           property: JsonSchemaProperty, 
                                           isRequired: Boolean,
                                           refContext: Option[ReferenceResolutionContext],
                                           schemaSource: String
                                         ): Field = {
    property.properties match {
      case Some(nestedProps) =>
        val nestedFields = convertObjectPropertiesForArray(nestedProps, property.required.getOrElse(List()), refContext, s"$schemaSource.$fieldName")
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
          createSimpleField(fieldName, StructType, isRequired)
        }
      case None =>
        createSimpleField(fieldName, StructType, isRequired)
    }
  }

  /**
   * Convert nested array within array (arrays of arrays)
   */
  private def convertNestedArrayForArray(
                                          fieldName: String, 
                                          property: JsonSchemaProperty, 
                                          isRequired: Boolean,
                                          refContext: Option[ReferenceResolutionContext],
                                          schemaSource: String
                                        ): Field = {
    property.items match {
      case Some(itemsSchema) =>
        convertArrayFromProperty(fieldName, itemsSchema, property, refContext, schemaSource, isRequired)
      case None =>
        LOGGER.warn(s"Nested array field '$fieldName' has no items schema defined in $schemaSource")
        createSimpleField(fieldName, ArrayType, isRequired)
    }
  }

  /**
   * Convert primitive field within array
   */
  private def convertPrimitiveForArray(
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
} 