package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.JsonSchemaConstraintsConverter._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.JsonSchemaReferenceResolver._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.JsonSchemaTypeMapper._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model._
import org.apache.log4j.Logger

import scala.collection.mutable

/**
 * Handles conversion of JSON Schema composition types (allOf, oneOf, anyOf) to Data Caterer Fields
 */
object JsonSchemaCompositionConverter {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Convert composition schemas at definition level (for root-level composition)
   */
  def convertCompositionFromDefinition(
                                        fieldName: String,
                                        schema: JsonSchemaDefinition,
                                        refContext: Option[ReferenceResolutionContext],
                                        schemaSource: String
                                      ): List[Field] = {
    schema.allOf match {
      case Some(allOfSchemas) =>
        LOGGER.info(s"Converting allOf composition schema '$fieldName' with ${allOfSchemas.size} schemas")
        val mergedField = mergeAllOfSchemas(fieldName, allOfSchemas, refContext, schemaSource, isRequired = true)
        List(mergedField)
      case None =>
        schema.oneOf.orElse(schema.anyOf) match {
          case Some(schemas) if schemas.nonEmpty =>
            LOGGER.debug(s"Converting oneOf/anyOf composition schema '$fieldName', using first schema of ${schemas.size}")
            val firstSchema = schemas.head
            if (firstSchema.hasReference && refContext.isDefined) {
              firstSchema.ref match {
                case Some(refString) =>
                  val resolvedSchema = resolveReference(refString, refContext.get)
                  convertPropertyToField(fieldName, resolvedSchema, isRequired = true, refContext, s"$schemaSource->$refString")
                    .map(List(_))
                    .getOrElse(List(createSimpleField(fieldName, StringType)))
                case None =>
                  List(createSimpleField(fieldName, StringType))
              }
            } else {
              convertPropertyToField(fieldName, firstSchema, isRequired = true, refContext, schemaSource)
                .map(List(_))
                .getOrElse(List(createSimpleField(fieldName, StringType)))
            }
          case _ =>
            List(createSimpleField(fieldName, StringType))
        }
    }
  }

  /**
   * Convert composition field (for property-level composition)
   */
  def convertCompositionField(
                               fieldName: String,
                               property: JsonSchemaProperty,
                               isRequired: Boolean,
                               refContext: Option[ReferenceResolutionContext],
                               schemaSource: String
                             ): Field = {
    property.allOf match {
      case Some(allOfSchemas) =>
        LOGGER.info(s"Converting allOf composition field '$fieldName' with ${allOfSchemas.size} schemas")
        mergeAllOfSchemas(fieldName, allOfSchemas, refContext, schemaSource, isRequired)
      case None =>
        property.oneOf.orElse(property.anyOf) match {
          case Some(schemas) if schemas.nonEmpty =>
            LOGGER.debug(s"Converting oneOf/anyOf composition field '$fieldName', using first schema of ${schemas.size}")
            // For oneOf/anyOf, we take the first schema for simplicity
            val firstSchema = schemas.head
            if (firstSchema.hasReference && refContext.isDefined) {
              firstSchema.ref match {
                case Some(refString) =>
                  val resolvedSchema = resolveReference(refString, refContext.get)
                  convertPropertyToField(fieldName, resolvedSchema, isRequired, refContext, s"$schemaSource->$refString")
                    .getOrElse(createSimpleField(fieldName, StringType, isRequired))
                case None =>
                  createSimpleField(fieldName, StringType, isRequired)
              }
            } else {
              convertPropertyToField(fieldName, firstSchema, isRequired, refContext, schemaSource)
                .getOrElse(createSimpleField(fieldName, StringType, isRequired))
            }
          case _ =>
            createSimpleField(fieldName, StringType, isRequired)
        }
    }
  }

  /**
   * Merge allOf schemas into a single field
   */
  private def mergeAllOfSchemas(
                                 fieldName: String,
                                 allOfSchemas: List[JsonSchemaProperty],
                                 refContext: Option[ReferenceResolutionContext],
                                 schemaSource: String,
                                 isRequired: Boolean
                               ): Field = {
    val mergedProperties = mutable.Map[String, JsonSchemaProperty]()
    val mergedRequired = mutable.Set[String]()
    
    allOfSchemas.foreach { subSchema =>
      val resolvedSchema = if (subSchema.hasReference && refContext.isDefined) {
        subSchema.ref match {
          case Some(refString) =>
            LOGGER.debug(s"Resolving reference in allOf for field '$fieldName': $refString")
            resolveReference(refString, refContext.get)
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
      val nestedFields = convertObjectPropertiesForComposition(mergedProperties.toMap, mergedRequired.toList, refContext, schemaSource)
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
   * Convert object properties for composition (similar to main object conversion but specific for composition)
   */
  private def convertObjectPropertiesForComposition(
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
        convertPropertyToField(propertyName, propertySchema, required.contains(propertyName), refContext, schemaSource)
      }
    }.toList
  }

  /**
   * Convert a property to field (simplified version for composition)
   */
  private def convertPropertyToField(
                                      fieldName: String, 
                                      property: JsonSchemaProperty, 
                                      isRequired: Boolean,
                                      refContext: Option[ReferenceResolutionContext],
                                      schemaSource: String
                                    ): Option[Field] = {
    LOGGER.debug(s"Converting composition property '$fieldName' with type '${property.`type`.getOrElse("undefined")}' from $schemaSource")

    // Check for reference first if we have a reference context
    if (property.hasReference && refContext.isDefined) {
      property.ref match {
        case Some(refString) =>
          LOGGER.debug(s"Resolving reference for composition field '$fieldName': $refString")
          try {
            val resolvedSchema = resolveReference(refString, refContext.get)
            return convertPropertyToField(fieldName, resolvedSchema, isRequired, refContext, s"$schemaSource->$refString")
          } catch {
            case ex: JsonSchemaReferenceResolutionException =>
              LOGGER.error(s"Failed to resolve reference '$refString' for composition field '$fieldName' in $schemaSource", ex)
              return Some(createSimpleField(fieldName, StringType, isRequired))
          }
        case None =>
          LOGGER.warn(s"Composition property '$fieldName' marked as having reference but ref field is empty in $schemaSource")
          return Some(createSimpleField(fieldName, StringType, isRequired))
      }
    }

    // Handle typed properties
    property.`type` match {
      case Some("object") =>
        Some(convertObjectForComposition(fieldName, property, isRequired, refContext, schemaSource))
      case Some("array") =>
        Some(convertArrayForComposition(fieldName, property, isRequired, refContext, schemaSource))
      case Some(primitiveType) =>
        Some(convertPrimitiveForComposition(fieldName, primitiveType, property, isRequired))
      case None =>
        if (property.hasComposition) {
          // Recursive composition - handle with care to avoid infinite recursion
          Some(convertCompositionField(fieldName, property, isRequired, refContext, schemaSource))
        } else {
          LOGGER.warn(s"Composition property '$fieldName' has no type defined in $schemaSource, defaulting to string")
          Some(createSimpleField(fieldName, StringType, isRequired))
        }
    }
  }

  /**
   * Convert object within composition
   */
  private def convertObjectForComposition(
                                           fieldName: String, 
                                           property: JsonSchemaProperty, 
                                           isRequired: Boolean,
                                           refContext: Option[ReferenceResolutionContext],
                                           schemaSource: String
                                         ): Field = {
    property.properties match {
      case Some(nestedProps) =>
        val nestedFields = convertObjectPropertiesForComposition(nestedProps, property.required.getOrElse(List()), refContext, s"$schemaSource.$fieldName")
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
   * Convert array within composition
   */
  private def convertArrayForComposition(
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
        LOGGER.warn(s"Array field '$fieldName' in composition has no items schema defined in $schemaSource")
        createSimpleField(fieldName, ArrayType, isRequired)
    }
  }

  /**
   * Convert primitive within composition
   */
  private def convertPrimitiveForComposition(
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