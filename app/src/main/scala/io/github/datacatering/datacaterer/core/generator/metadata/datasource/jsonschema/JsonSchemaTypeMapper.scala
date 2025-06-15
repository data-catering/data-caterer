package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.model._
import org.apache.log4j.Logger

/**
 * Utility for mapping types between JSON Schema and Data Caterer
 */
object JsonSchemaTypeMapper {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Map JSON Schema type to Data Caterer DataType
   */
  def mapJsonSchemaTypeToDataCaterer(jsonSchemaType: String): DataType = {
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
  def mapToDataCatererDataType(fieldType: Option[String]): DataType = {
    fieldType.map(DataType.fromString).getOrElse(StringType)
  }

  /**
   * Get DataType from Field, handling nested structures properly
   */
  def getDataTypeFromField(field: Field): DataType = {
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
  def parseArrayType(typeStr: String): DataType = {
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
   * Create a simple field with basic properties
   */
  def createSimpleField(name: String, dataType: DataType, isRequired: Boolean = true): Field = {
    Field(
      name = name,
      `type` = Some(dataType.toString),
      options = Map(),
      nullable = !isRequired
    )
  }
} 