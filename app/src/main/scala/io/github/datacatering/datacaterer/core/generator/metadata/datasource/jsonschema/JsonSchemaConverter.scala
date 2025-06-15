package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model._
import org.apache.log4j.Logger

import scala.collection.mutable

/**
 * Converts JSON Schema definitions to Data Caterer Field models
 */
object JsonSchemaConverter {

  private val LOGGER = Logger.getLogger(getClass.getName)

  // Deprecated convertSchema method removed - use convertSchemaWithRefs instead

  /**
   * Enhanced conversion method that supports $ref resolution
   * Separate from the original convertSchema to avoid breaking existing logic
   */
  def convertSchemaWithRefs(schema: JsonSchemaDefinition, schemaSource: String): List[Field] = {
    LOGGER.info(s"Converting JSON schema with $$ref support from $schemaSource, type: ${schema.`type`.getOrElse("undefined")}")
    
    val referenceContext = ReferenceResolutionContext(schema.definitions.getOrElse(Map()), schemaSource)
    convertSchemaWithRefsInternal(schema, referenceContext, schemaSource)
  }

  /**
   * Internal method for converting schema with reference resolution context
   */
  private def convertSchemaWithRefsInternal(
                                             schema: JsonSchemaDefinition, 
                                             refContext: ReferenceResolutionContext,
                                             schemaSource: String
                                           ): List[Field] = {
    try {
      schema.`type` match {
        case Some("object") =>
          schema.properties match {
            case Some(props) => convertObjectPropertiesWithRefs(props, schema.required.getOrElse(List()), refContext, schemaSource)
            case None => 
              LOGGER.warn(s"Object schema has no properties defined in $schemaSource")
              List()
          }
        case Some("array") =>
          schema.items match {
            case Some(itemsSchema) =>
              val arrayField = convertArraySchemaFromDefinitionWithRefs("root_array", itemsSchema, schema, refContext, schemaSource)
              List(arrayField)
            case None =>
              LOGGER.warn(s"Array schema has no items defined in $schemaSource")
              List(createSimpleField("root_array", ArrayType))
          }
        case Some(primitiveType) =>
          val field = convertPrimitiveSchema("root_value", primitiveType, schema, schemaSource)
          List(field)
        case None =>
          if (schema.hasComposition) {
            convertCompositionSchemaWithRefs("root", schema, refContext, schemaSource)
          } else if (schema.hasReference) {
            schema.ref match {
              case Some(refString) =>
                LOGGER.info(s"Resolving root-level reference: $refString in $schemaSource")
                val resolvedSchema = resolveReference(refString, refContext)
                convertPropertyToFieldWithRefs("root", resolvedSchema, isRequired = true, refContext, schemaSource)
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
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to convert JSON schema with refs from $schemaSource", ex)
        throw JsonSchemaValidationException(s"Failed to convert JSON schema with refs from $schemaSource: ${ex.getMessage}", ex)
    }
  }

  /**
   * Convert object properties with reference resolution
   */
  private def convertObjectPropertiesWithRefs(
                                               properties: Map[String, JsonSchemaProperty], 
                                               required: List[String], 
                                               refContext: ReferenceResolutionContext,
                                               schemaSource: String
                                             ): List[Field] = {
    properties.flatMap { case (propertyName, propertySchema) =>
      // Filter out JSON schema metadata fields that start with '$'
      if (propertyName.startsWith("$")) {
        LOGGER.debug(s"Filtering out JSON schema metadata field '$propertyName' from $schemaSource")
        None
      } else {
        convertPropertyToFieldWithRefs(propertyName, propertySchema, required.contains(propertyName), refContext, schemaSource)
      }
    }.toList
  }

  /**
   * Convert a JSON schema property to a Data Caterer Field with reference resolution
   */
  private def convertPropertyToFieldWithRefs(
                                              fieldName: String, 
                                              property: JsonSchemaProperty, 
                                              isRequired: Boolean,
                                              refContext: ReferenceResolutionContext,
                                              schemaSource: String
                                            ): Option[Field] = {
    LOGGER.debug(s"Converting property '$fieldName' with refs of type '${property.`type`.getOrElse("undefined")}' from $schemaSource")

    // Check for reference first
    if (property.hasReference) {
      property.ref match {
        case Some(refString) =>
          LOGGER.debug(s"Resolving reference for field '$fieldName': $refString")
          try {
            val resolvedSchema = resolveReference(refString, refContext)
            return convertPropertyToFieldWithRefs(fieldName, resolvedSchema, isRequired, refContext, s"$schemaSource->$refString")
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
        Some(convertObjectFieldWithRefs(fieldName, property, isRequired, refContext, schemaSource))
      case Some("array") =>
        Some(convertArrayFieldWithRefs(fieldName, property, isRequired, refContext, schemaSource))
      case Some(primitiveType) =>
        Some(convertPrimitiveField(fieldName, primitiveType, property, isRequired, schemaSource))
      case None =>
        if (property.hasComposition) {
          Some(convertCompositionFieldWithRefs(fieldName, property, isRequired, refContext, schemaSource))
        } else {
          LOGGER.warn(s"Property '$fieldName' has no type defined in $schemaSource, defaulting to string")
          Some(createSimpleField(fieldName, StringType, isRequired))
        }
    }
  }

  /**
   * Convert an object field with nested properties and reference resolution
   */
  private def convertObjectFieldWithRefs(
                                          fieldName: String, 
                                          property: JsonSchemaProperty, 
                                          isRequired: Boolean,
                                          refContext: ReferenceResolutionContext,
                                          schemaSource: String
                                        ): Field = {
    property.properties match {
      case Some(nestedProps) =>
        val nestedFields = convertObjectPropertiesWithRefs(nestedProps, property.required.getOrElse(List()), refContext, s"$schemaSource.$fieldName")
        if (nestedFields.nonEmpty) {
          // Create StructType directly from nested fields to avoid empty struct issue
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
        // Check if this object has composition (oneOf/anyOf/allOf) instead of direct properties
        if (property.hasComposition) {
          LOGGER.debug(s"Object field '$fieldName' has no direct properties but has composition, converting composition in $schemaSource")
          convertCompositionFieldWithRefs(fieldName, property, isRequired, refContext, schemaSource)
        } else {
          LOGGER.warn(s"Object field '$fieldName' has no properties defined and no composition in $schemaSource")
          createSimpleField(fieldName, StructType, isRequired)
        }
    }
  }

  /**
   * Convert an array field with reference resolution
   */
  private def convertArrayFieldWithRefs(
                                         fieldName: String, 
                                         property: JsonSchemaProperty, 
                                         isRequired: Boolean,
                                         refContext: ReferenceResolutionContext,
                                         schemaSource: String
                                       ): Field = {
    property.items match {
      case Some(itemsSchema) =>
        convertArraySchemaWithRefs(fieldName, itemsSchema, property, refContext, schemaSource, isRequired)
      case None =>
        LOGGER.warn(s"Array field '$fieldName' has no items schema defined in $schemaSource")
        createSimpleField(fieldName, ArrayType, isRequired)
    }
  }

  /**
   * Convert array schema from JsonSchemaDefinition with reference resolution
   */
  private def convertArraySchemaFromDefinitionWithRefs(
                                                        fieldName: String,
                                                        itemsSchema: JsonSchemaProperty,
                                                        parentSchema: JsonSchemaDefinition,
                                                        refContext: ReferenceResolutionContext,
                                                        schemaSource: String,
                                                        isRequired: Boolean = true
                                                      ): Field = {
    // Check if items schema has a reference
    if (itemsSchema.hasReference) {
      itemsSchema.ref match {
        case Some(refString) =>
          LOGGER.debug(s"Resolving reference for array items '$fieldName': $refString")
          val resolvedItemsSchema = resolveReference(refString, refContext)
          return convertArraySchemaFromDefinitionWithRefs(fieldName, resolvedItemsSchema, parentSchema, refContext, s"$schemaSource->$refString", isRequired)
        case None =>
          LOGGER.warn(s"Array items schema marked as having reference but ref field is empty for field '$fieldName' in $schemaSource")
      }
    }

    itemsSchema.`type` match {
      case Some("object") =>
        itemsSchema.properties match {
          case Some(itemProps) =>
            val nestedFields = convertObjectPropertiesWithRefs(itemProps, itemsSchema.required.getOrElse(List()), refContext, s"$schemaSource.$fieldName.items")
            // Create StructType directly from nested fields to avoid empty struct issue
            val elementType = if (nestedFields.nonEmpty) {
              new StructType(nestedFields.map(f => f.name -> getDataTypeFromField(f)))
            } else {
              StructType
            }
            Field(
              name = fieldName,
              `type` = Some(new ArrayType(elementType).toString),
              options = convertArrayConstraintsFromDefinition(parentSchema),
              nullable = !isRequired,
              fields = nestedFields
            )
          case None =>
            createSimpleField(fieldName, new ArrayType(StructType), isRequired)
        }
      case Some(primitiveType) =>
        val elementDataType = mapJsonSchemaTypeToDataCaterer(primitiveType)
        Field(
          name = fieldName,
          `type` = Some(new ArrayType(elementDataType).toString),
          options = convertArrayConstraintsFromDefinition(parentSchema),
          nullable = !isRequired
        )
      case None =>
        if (itemsSchema.hasComposition) {
          LOGGER.warn(s"Array items with composition schemas not fully supported for field '$fieldName' in $schemaSource")
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        } else {
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        }
    }
  }

  /**
   * Convert array schema with items definition and reference resolution
   */
  private def convertArraySchemaWithRefs(
                                          fieldName: String,
                                          itemsSchema: JsonSchemaProperty,
                                          parentSchema: JsonSchemaProperty,
                                          refContext: ReferenceResolutionContext,
                                          schemaSource: String,
                                          isRequired: Boolean = true
                                        ): Field = {
    // Check if items schema has a reference
    if (itemsSchema.hasReference) {
      itemsSchema.ref match {
        case Some(refString) =>
          LOGGER.debug(s"Resolving reference for array items '$fieldName': $refString")
          val resolvedItemsSchema = resolveReference(refString, refContext)
          return convertArraySchemaWithRefs(fieldName, resolvedItemsSchema, parentSchema, refContext, s"$schemaSource->$refString", isRequired)
        case None =>
          LOGGER.warn(s"Array items schema marked as having reference but ref field is empty for field '$fieldName' in $schemaSource")
      }
    }

    itemsSchema.`type` match {
      case Some("object") =>
        itemsSchema.properties match {
          case Some(itemProps) =>
            val nestedFields = convertObjectPropertiesWithRefs(itemProps, itemsSchema.required.getOrElse(List()), refContext, s"$schemaSource.$fieldName.items")
            // Create StructType directly from nested fields to avoid empty struct issue
            val elementType = if (nestedFields.nonEmpty) {
              new StructType(nestedFields.map(f => f.name -> getDataTypeFromField(f)))
            } else {
              StructType
            }
            Field(
              name = fieldName,
              `type` = Some(new ArrayType(elementType).toString),
              options = convertConstraintsToOptions(parentSchema.getConstraints) ++ convertArrayConstraints(parentSchema),
              nullable = !isRequired,
              fields = nestedFields
            )
          case None =>
            createSimpleField(fieldName, new ArrayType(StructType), isRequired)
        }
      case Some(primitiveType) =>
        val elementDataType = mapJsonSchemaTypeToDataCaterer(primitiveType)
        Field(
          name = fieldName,
          `type` = Some(new ArrayType(elementDataType).toString),
          options = convertConstraintsToOptions(parentSchema.getConstraints) ++ convertArrayConstraints(parentSchema),
          nullable = !isRequired
        )
      case None =>
        if (itemsSchema.hasComposition) {
          LOGGER.warn(s"Array items with composition schemas not fully supported for field '$fieldName' in $schemaSource")
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        } else {
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        }
    }
  }

  /**
   * Convert composition schemas (allOf, oneOf, anyOf) with reference resolution
   */
  private def convertCompositionFieldWithRefs(
                                               fieldName: String,
                                               property: JsonSchemaProperty,
                                               isRequired: Boolean,
                                               refContext: ReferenceResolutionContext,
                                               schemaSource: String
                                             ): Field = {
    property.allOf match {
      case Some(allOfSchemas) =>
        LOGGER.info(s"Converting allOf composition field '$fieldName' with ${allOfSchemas.size} schemas and ref support")
        mergeAllOfSchemasWithRefs(fieldName, allOfSchemas, refContext, schemaSource, isRequired)
      case None =>
        property.oneOf.orElse(property.anyOf) match {
          case Some(schemas) if schemas.nonEmpty =>
            LOGGER.debug(s"Converting oneOf/anyOf composition field '$fieldName', using first schema of ${schemas.size} with ref support")
            // Resolve reference if present in first schema
            val firstSchema = schemas.head
            if (firstSchema.hasReference) {
              firstSchema.ref match {
                case Some(refString) =>
                  val resolvedSchema = resolveReference(refString, refContext)
                  convertPropertyToFieldWithRefs(fieldName, resolvedSchema, isRequired, refContext, s"$schemaSource->$refString")
                    .getOrElse(createSimpleField(fieldName, StringType, isRequired))
                case None =>
                  createSimpleField(fieldName, StringType, isRequired)
              }
            } else {
              convertPropertyToFieldWithRefs(fieldName, firstSchema, isRequired, refContext, schemaSource)
                .getOrElse(createSimpleField(fieldName, StringType, isRequired))
            }
          case _ =>
            createSimpleField(fieldName, StringType, isRequired)
        }
    }
  }

  /**
   * Convert composition schemas at definition level with reference resolution
   */
  private def convertCompositionSchemaWithRefs(
                                                fieldName: String,
                                                schema: JsonSchemaDefinition,
                                                refContext: ReferenceResolutionContext,
                                                schemaSource: String
                                              ): List[Field] = {
    schema.allOf match {
      case Some(allOfSchemas) =>
        LOGGER.info(s"Converting allOf composition schema '$fieldName' with ${allOfSchemas.size} schemas and ref support")
        val mergedField = mergeAllOfSchemasWithRefs(fieldName, allOfSchemas, refContext, schemaSource, isRequired = true)
        List(mergedField)
      case None =>
        schema.oneOf.orElse(schema.anyOf) match {
          case Some(schemas) if schemas.nonEmpty =>
            LOGGER.debug(s"Converting oneOf/anyOf composition schema '$fieldName', using first schema of ${schemas.size} with ref support")
            val firstSchema = schemas.head
            if (firstSchema.hasReference) {
              firstSchema.ref match {
                case Some(refString) =>
                  val resolvedSchema = resolveReference(refString, refContext)
                  convertPropertyToFieldWithRefs(fieldName, resolvedSchema, isRequired = true, refContext, s"$schemaSource->$refString")
                    .map(List(_))
                    .getOrElse(List(createSimpleField(fieldName, StringType)))
                case None =>
                  List(createSimpleField(fieldName, StringType))
              }
            } else {
              convertPropertyToFieldWithRefs(fieldName, firstSchema, isRequired = true, refContext, schemaSource)
                .map(List(_))
                .getOrElse(List(createSimpleField(fieldName, StringType)))
            }
          case _ =>
            List(createSimpleField(fieldName, StringType))
        }
    }
  }

  /**
   * Merge allOf schemas into a single field with reference resolution
   */
  private def mergeAllOfSchemasWithRefs(
                                         fieldName: String,
                                         allOfSchemas: List[JsonSchemaProperty],
                                         refContext: ReferenceResolutionContext,
                                         schemaSource: String,
                                         isRequired: Boolean
                                       ): Field = {
    val mergedProperties = scala.collection.mutable.Map[String, JsonSchemaProperty]()
    val mergedRequired = scala.collection.mutable.Set[String]()
    
    allOfSchemas.foreach { subSchema =>
      val resolvedSchema = if (subSchema.hasReference) {
        subSchema.ref match {
          case Some(refString) =>
            LOGGER.debug(s"Resolving reference in allOf for field '$fieldName': $refString")
            resolveReference(refString, refContext)
          case None =>
            subSchema
        }
      } else {
        subSchema
      }
      
      resolvedSchema.properties.foreach { props =>
        mergedProperties ++= props
      }
      resolvedSchema.required.foreach { req =>
        mergedRequired ++= req
      }
    }
    
    if (mergedProperties.nonEmpty) {
      val nestedFields = convertObjectPropertiesWithRefs(mergedProperties.toMap, mergedRequired.toList, refContext, schemaSource)
      Field(
        name = fieldName,
        `type` = Some(new StructType(nestedFields.map(f => f.name -> mapToDataCatererDataType(f.`type`))).toString),
        options = Map(), // Could merge constraints from all schemas if needed
        nullable = !isRequired,
        fields = nestedFields
      )
    } else {
      createSimpleField(fieldName, StructType, isRequired)
    }
  }

  /**
   * Resolve a JSON Schema reference
   */
  private def resolveReference(refString: String, refContext: ReferenceResolutionContext): JsonSchemaProperty = {
    LOGGER.debug(s"Resolving reference: $refString")
    
    if (refString.startsWith("#/definitions/")) {
      val definitionName = refString.substring("#/definitions/".length)
      refContext.definitions.get(definitionName) match {
        case Some(definition) =>
          LOGGER.debug(s"Successfully resolved reference to definition: $definitionName")
          definition
        case None =>
          val availableDefinitions = refContext.definitions.keys.mkString(", ")
          val errorMsg = s"Definition '$definitionName' not found in schema. Available definitions: $availableDefinitions"
          LOGGER.error(errorMsg)
          throw JsonSchemaReferenceResolutionException(refString, new IllegalArgumentException(errorMsg))
      }
    } else if (refString.startsWith("#/")) {
      // Handle other JSON Pointer references (not implemented yet)
      val errorMsg = s"JSON Pointer references other than #/definitions/ are not yet supported: $refString"
      LOGGER.error(errorMsg)
      throw JsonSchemaReferenceResolutionException(refString, new UnsupportedOperationException(errorMsg))
    } else {
      // External references (not implemented yet)
      val errorMsg = s"External references are not yet supported: $refString"
      LOGGER.error(errorMsg)
      throw JsonSchemaReferenceResolutionException(refString, new UnsupportedOperationException(errorMsg))
    }
  }

  /**
   * Context for resolving JSON Schema references
   */
  private case class ReferenceResolutionContext(
                                                 definitions: Map[String, JsonSchemaProperty],
                                                 schemaSource: String
                                               )

  /**
   * Convert object properties to Field list
   */
  private def convertObjectProperties(
                                       properties: Map[String, JsonSchemaProperty], 
                                       required: List[String], 
                                       schemaSource: String
                                     ): List[Field] = {
    properties.flatMap { case (propertyName, propertySchema) =>
      // Filter out JSON schema metadata fields that start with '$'
      if (propertyName.startsWith("$")) {
        LOGGER.debug(s"Filtering out JSON schema metadata field '$propertyName' from $schemaSource")
        None
      } else {
        Some(convertPropertyToField(propertyName, propertySchema, required.contains(propertyName), schemaSource))
      }
    }.toList
  }

  /**
   * Convert a JSON schema property to a Data Caterer Field
   */
  private def convertPropertyToField(
                                      fieldName: String, 
                                      property: JsonSchemaProperty, 
                                      isRequired: Boolean,
                                      schemaSource: String
                                    ): Field = {
    LOGGER.debug(s"Converting property '$fieldName' of type '${property.`type`.getOrElse("undefined")}' from $schemaSource")

    property.`type` match {
      case Some("object") =>
        convertObjectField(fieldName, property, isRequired, schemaSource)
      case Some("array") =>
        convertArrayField(fieldName, property, isRequired, schemaSource)
      case Some(primitiveType) =>
        convertPrimitiveField(fieldName, primitiveType, property, isRequired, schemaSource)
      case None =>
        if (property.hasComposition) {
          convertCompositionField(fieldName, property, isRequired, schemaSource)
        } else if (property.hasReference) {
          LOGGER.warn(s"Property references are not yet fully supported for field '$fieldName' in $schemaSource")
          createSimpleField(fieldName, StringType, isRequired)
        } else {
          LOGGER.warn(s"Property '$fieldName' has no type defined in $schemaSource, defaulting to string")
          createSimpleField(fieldName, StringType, isRequired)
        }
    }
  }

  /**
   * Convert an object field with nested properties
   */
  private def convertObjectField(
                                  fieldName: String, 
                                  property: JsonSchemaProperty, 
                                  isRequired: Boolean,
                                  schemaSource: String
                                ): Field = {
    property.properties match {
      case Some(nestedProps) =>
        val nestedFields = convertObjectProperties(nestedProps, property.required.getOrElse(List()), s"$schemaSource.$fieldName")
        Field(
          name = fieldName,
          `type` = Some(new StructType(nestedFields.map(f => f.name -> mapToDataCatererDataType(f.`type`))).toString),
          options = convertConstraintsToOptions(property.getConstraints),
          nullable = !isRequired,
          fields = nestedFields
        )
      case None =>
        LOGGER.warn(s"Object field '$fieldName' has no properties defined in $schemaSource")
        createSimpleField(fieldName, StructType, isRequired)
    }
  }

  /**
   * Convert an array field
   */
  private def convertArrayField(
                                 fieldName: String, 
                                 property: JsonSchemaProperty, 
                                 isRequired: Boolean,
                                 schemaSource: String
                               ): Field = {
    property.items match {
      case Some(itemsSchema) =>
        convertArraySchema(fieldName, itemsSchema, property, schemaSource, isRequired)
      case None =>
        LOGGER.warn(s"Array field '$fieldName' has no items schema defined in $schemaSource")
        createSimpleField(fieldName, ArrayType, isRequired)
    }
  }

  /**
   * Convert array schema from JsonSchemaDefinition (for root-level arrays)
   */
  private def convertArraySchemaFromDefinition(
                                                fieldName: String,
                                                itemsSchema: JsonSchemaProperty,
                                                parentSchema: JsonSchemaDefinition,
                                                schemaSource: String,
                                                isRequired: Boolean = true
                                              ): Field = {
    itemsSchema.`type` match {
      case Some("object") =>
        itemsSchema.properties match {
          case Some(itemProps) =>
            val nestedFields = convertObjectProperties(itemProps, itemsSchema.required.getOrElse(List()), s"$schemaSource.$fieldName.items")
            val elementType = new StructType(nestedFields.map(f => f.name -> mapToDataCatererDataType(f.`type`)))
            Field(
              name = fieldName,
              `type` = Some(new ArrayType(elementType).toString),
              options = convertArrayConstraintsFromDefinition(parentSchema),
              nullable = !isRequired,
              fields = nestedFields
            )
          case None =>
            createSimpleField(fieldName, new ArrayType(StructType), isRequired)
        }
      case Some(primitiveType) =>
        val elementDataType = mapJsonSchemaTypeToDataCaterer(primitiveType)
        Field(
          name = fieldName,
          `type` = Some(new ArrayType(elementDataType).toString),
          options = convertArrayConstraintsFromDefinition(parentSchema),
          nullable = !isRequired
        )
      case None =>
        if (itemsSchema.hasComposition) {
          LOGGER.warn(s"Array items with composition schemas not fully supported for field '$fieldName' in $schemaSource")
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        } else {
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        }
    }
  }

  /**
   * Convert array schema with items definition
   */
  private def convertArraySchema(
                                  fieldName: String,
                                  itemsSchema: JsonSchemaProperty,
                                  parentSchema: JsonSchemaProperty,
                                  schemaSource: String,
                                  isRequired: Boolean = true
                                ): Field = {
    itemsSchema.`type` match {
      case Some("object") =>
        itemsSchema.properties match {
          case Some(itemProps) =>
            val nestedFields = convertObjectProperties(itemProps, itemsSchema.required.getOrElse(List()), s"$schemaSource.$fieldName.items")
            val elementType = new StructType(nestedFields.map(f => f.name -> mapToDataCatererDataType(f.`type`)))
            Field(
              name = fieldName,
              `type` = Some(new ArrayType(elementType).toString),
              options = convertConstraintsToOptions(parentSchema.getConstraints) ++ convertArrayConstraints(parentSchema),
              nullable = !isRequired,
              fields = nestedFields
            )
          case None =>
            createSimpleField(fieldName, new ArrayType(StructType), isRequired)
        }
      case Some(primitiveType) =>
        val elementDataType = mapJsonSchemaTypeToDataCaterer(primitiveType)
        Field(
          name = fieldName,
          `type` = Some(new ArrayType(elementDataType).toString),
          options = convertConstraintsToOptions(parentSchema.getConstraints) ++ convertArrayConstraints(parentSchema),
          nullable = !isRequired
        )
      case None =>
        if (itemsSchema.hasComposition) {
          LOGGER.warn(s"Array items with composition schemas not fully supported for field '$fieldName' in $schemaSource")
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        } else {
          createSimpleField(fieldName, new ArrayType(StringType), isRequired)
        }
    }
  }

  /**
   * Convert a primitive field (string, number, integer, boolean, null)
   */
  private def convertPrimitiveField(
                                     fieldName: String,
                                     jsonSchemaType: String,
                                     property: JsonSchemaProperty,
                                     isRequired: Boolean,
                                     schemaSource: String
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
   * Convert a primitive schema (for root-level primitives)
   */
  private def convertPrimitiveSchema(
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

  /**
   * Convert composition schemas (allOf, oneOf, anyOf)
   */
  private def convertCompositionSchema(
                                        fieldName: String,
                                        schema: JsonSchemaDefinition,
                                        schemaSource: String
                                      ): List[Field] = {
    // For now, handle allOf by merging properties, oneOf/anyOf by taking first option
    schema.allOf match {
      case Some(allOfSchemas) =>
        LOGGER.info(s"Converting allOf composition for '$fieldName' with ${allOfSchemas.size} schemas")
        mergeAllOfSchemas(fieldName, allOfSchemas, schemaSource)
      case None =>
        schema.oneOf.orElse(schema.anyOf) match {
          case Some(schemas) if schemas.nonEmpty =>
            LOGGER.debug(s"Converting oneOf/anyOf composition for '$fieldName', using first schema of ${schemas.size}")
            // For simplicity, use the first schema in oneOf/anyOf
            List(convertPropertyToField(fieldName, schemas.head, isRequired = true, schemaSource))
          case _ =>
            LOGGER.warn(s"Composition schema has no valid schemas for '$fieldName' in $schemaSource")
            List(createSimpleField(fieldName, StringType))
        }
    }
  }

  /**
   * Convert composition field
   */
  private def convertCompositionField(
                                       fieldName: String,
                                       property: JsonSchemaProperty,
                                       isRequired: Boolean,
                                       schemaSource: String
                                     ): Field = {
    property.allOf match {
      case Some(allOfSchemas) =>
        LOGGER.info(s"Converting allOf composition field '$fieldName' with ${allOfSchemas.size} schemas")
        // Merge all schemas - for simplicity, take properties from all and combine constraints
        val mergedProperties = mutable.Map[String, JsonSchemaProperty]()
        val mergedRequired = mutable.Set[String]()
        
        allOfSchemas.foreach { subSchema =>
          subSchema.properties.foreach { props =>
            mergedProperties ++= props
          }
          subSchema.required.foreach { req =>
            mergedRequired ++= req
          }
        }
        
        if (mergedProperties.nonEmpty) {
          val nestedFields = convertObjectProperties(mergedProperties.toMap, mergedRequired.toList, s"$schemaSource.$fieldName")
          Field(
            name = fieldName,
            `type` = Some(new StructType(nestedFields.map(f => f.name -> mapToDataCatererDataType(f.`type`))).toString),
            options = convertConstraintsToOptions(property.getConstraints),
            nullable = !isRequired,
            fields = nestedFields
          )
        } else {
          createSimpleField(fieldName, StructType, isRequired)
        }
      case None =>
        property.oneOf.orElse(property.anyOf) match {
          case Some(schemas) if schemas.nonEmpty =>
            LOGGER.debug(s"Converting oneOf/anyOf composition field '$fieldName', using first schema of ${schemas.size}")
            convertPropertyToField(fieldName, schemas.head, isRequired, schemaSource)
          case _ =>
            createSimpleField(fieldName, StringType, isRequired)
        }
    }
  }

  /**
   * Merge allOf schemas into a single field list
   */
  private def mergeAllOfSchemas(
                                 fieldName: String,
                                 allOfSchemas: List[JsonSchemaProperty],
                                 schemaSource: String
                               ): List[Field] = {
    val mergedProperties = mutable.Map[String, JsonSchemaProperty]()
    val mergedRequired = mutable.Set[String]()
    
    allOfSchemas.foreach { subSchema =>
      subSchema.properties.foreach { props =>
        mergedProperties ++= props
      }
      subSchema.required.foreach { req =>
        mergedRequired ++= req
      }
    }
    
    if (mergedProperties.nonEmpty) {
      convertObjectProperties(mergedProperties.toMap, mergedRequired.toList, schemaSource)
    } else {
      List(createSimpleField(fieldName, StringType))
    }
  }

  /**
   * Map JSON Schema type to Data Caterer DataType
   */
  private def mapJsonSchemaTypeToDataCaterer(jsonSchemaType: String): DataType = {
    jsonSchemaType.toLowerCase match {
      case "string" => StringType
      case "integer" => IntegerType
      case "number" => DoubleType
      case "boolean" => BooleanType
      case "array" => ArrayType
      case "object" => StructType
      case "null" => StringType // Treat null as nullable string
      case _ =>
        LOGGER.warn(s"Unknown JSON schema type '$jsonSchemaType', defaulting to StringType")
        StringType
    }
  }

  /**
   * Map Field type to DataType (for nested structures)
   */
  private def mapToDataCatererDataType(fieldType: Option[String]): DataType = {
    fieldType.map(DataType.fromString).getOrElse(StringType)
  }

  /**
   * Get DataType from Field, handling nested structures properly
   */
  private def getDataTypeFromField(field: Field): DataType = {
    field.`type` match {
      case Some(typeStr) if typeStr.startsWith("struct<") && field.fields.nonEmpty =>
        // For struct types with nested fields, create StructType from the actual nested fields
        new StructType(field.fields.map(f => f.name -> getDataTypeFromField(f)))
      case Some(typeStr) if typeStr.startsWith("array<") =>
        // For array types, handle both with and without nested fields
        if (field.fields.nonEmpty && typeStr.contains("struct<")) {
          // Array of structs with nested fields
          new ArrayType(new StructType(field.fields.map(f => f.name -> getDataTypeFromField(f))))
        } else {
          // Array of primitives or simple types - parse the type string properly
          parseArrayType(typeStr)
        }
      case Some(typeStr) =>
        DataType.fromString(typeStr)
      case None =>
        StringType
    }
  }

  /**
   * Parse array type string like "array<string>" or "array<struct<...>>"
   */
  private def parseArrayType(typeStr: String): DataType = {
    if (typeStr.startsWith("array<") && typeStr.endsWith(">")) {
      val elementTypeStr = typeStr.substring(6, typeStr.length - 1) // Remove "array<" and ">"
      val elementType = elementTypeStr match {
        case "string" => StringType
        case "integer" => IntegerType
        case "double" => DoubleType
        case "boolean" => BooleanType
        case s if s.startsWith("struct<") => StructType // For complex struct types, use basic StructType
        case _ => StringType
      }
      new ArrayType(elementType)
    } else {
      // Fallback to DataType.fromString for non-array types
      DataType.fromString(typeStr)
    }
  }

  /**
   * Convert JSON Schema constraints to Data Caterer field options
   */
  private def convertConstraintsToOptions(constraints: JsonSchemaConstraints): Map[String, Any] = {
    val options = mutable.Map[String, Any]()

    // String constraints
    constraints.pattern.foreach(pattern => options += REGEX_GENERATOR -> pattern)
    constraints.minLength.foreach(min => options += MINIMUM_LENGTH -> min)
    constraints.maxLength.foreach(max => options += MAXIMUM_LENGTH -> max)

    // Numeric constraints - parse string values to double
    constraints.minimum.foreach { minStr =>
      try {
        val doubleValue = minStr.toDouble
        options += MINIMUM -> doubleValue
      } catch {
        case _: NumberFormatException =>
          LOGGER.warn(s"Invalid minimum value '$minStr', skipping constraint")
      }
    }
    constraints.maximum.foreach { maxStr =>
      try {
        val doubleValue = maxStr.toDouble
        options += MAXIMUM -> doubleValue
      } catch {
        case _: NumberFormatException =>
          LOGGER.warn(s"Invalid maximum value '$maxStr', skipping constraint")
      }
    }
    // Note: JSON Schema exclusive min/max would need special handling

    // Value constraints
    constraints.`enum`.foreach { enumValues =>
      options += ONE_OF_GENERATOR -> enumValues.map(_.toString).mkString(",")
    }
    constraints.const.foreach { constValue =>
      options += ONE_OF_GENERATOR -> constValue.toString
    }

    // Default value
    constraints.`default`.foreach { defaultValue =>
      options += "default" -> defaultValue
    }

    // Format-based generators for common patterns
    constraints.format.foreach { format =>
      options ++= mapFormatToOptions(format)
    }

    options.toMap
  }

  /**
   * Convert array-specific constraints
   */
  private def convertArrayConstraints(property: JsonSchemaProperty): Map[String, Any] = {
    val options = mutable.Map[String, Any]()

    property.minItems.foreach(min => options += ARRAY_MINIMUM_LENGTH -> min)
    property.maxItems.foreach(max => options += ARRAY_MAXIMUM_LENGTH -> max)
    
    // Unique items constraint
    property.uniqueItems.foreach { unique =>
      if (unique) options += IS_UNIQUE -> true
    }

    options.toMap
  }

  /**
   * Convert array-specific constraints from JsonSchemaDefinition
   */
  private def convertArrayConstraintsFromDefinition(schema: JsonSchemaDefinition): Map[String, Any] = {
    val options = mutable.Map[String, Any]()

    schema.minItems.foreach(min => options += ARRAY_MINIMUM_LENGTH -> min)
    schema.maxItems.foreach(max => options += ARRAY_MAXIMUM_LENGTH -> max)
    
    // Unique items constraint
    schema.uniqueItems.foreach { unique =>
      if (unique) options += IS_UNIQUE -> true
    }

    options.toMap
  }

  /**
   * Map JSON Schema format to Data Caterer options
   */
  private def mapFormatToOptions(format: String): Map[String, Any] = {
    format.toLowerCase match {
      case "email" => Map(REGEX_GENERATOR -> "^[a-zA-Z0-9._%+-]{2,20}@[a-zA-Z0-9.-]{3,10}}\\.[a-zA-Z]{2,3}$")
      case "uri" | "url" => Map(REGEX_GENERATOR -> "^https?://[a-zA-Z0-9.-]{3,20}\\.[a-zA-Z]{2,3}(/.*)?$")
      case "uuid" => Map("uuid" -> "")
      case "date" => Map("type" -> "date")
      case "date-time" => Map("type" -> "timestamp")
      case "time" => Map(REGEX_GENERATOR -> "^([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$")
      case "ipv4" => Map(REGEX_GENERATOR -> "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
      case "ipv6" => Map(REGEX_GENERATOR -> "^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$")
      case "hostname" => Map(REGEX_GENERATOR -> "^[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
      case _ =>
        LOGGER.debug(s"No specific mapping for format '$format', using default constraints")
        Map()
    }
  }

  /**
   * Create a simple field with basic properties
   */
  private def createSimpleField(name: String, dataType: DataType, isRequired: Boolean = true): Field = {
    Field(
      name = name,
      `type` = Some(dataType.toString),
      options = Map(),
      nullable = !isRequired
    )
  }
} 