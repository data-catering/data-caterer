package io.github.datacatering.datacaterer.core.foreignkey.util

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.annotation.tailrec

/**
 * Utilities for working with nested fields in Spark DataFrames.
 */
object NestedFieldUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Check if a DataFrame contains a field (supports nested fields with dot notation).
   *
   * @param field Field name (supports dot notation like "address.city")
   * @param fields Array of struct fields to search
   * @return true if field exists, false otherwise
   */
  def hasDfContainField(field: String, fields: Array[StructField]): Boolean = {
    if (field.contains(".")) {
      val spt = field.split("\\.")
      fields.find(_.name == spt.head)
        .exists(field => checkNestedFields(spt, field.dataType))
    } else {
      fields.exists(_.name == field)
    }
  }

  /**
   * Recursively check nested fields.
   */
  @tailrec
  private def checkNestedFields(spt: Array[String], dataType: DataType): Boolean = {
    val tailColName = spt.tail
    dataType match {
      case StructType(nestedFields) =>
        hasDfContainField(tailColName.mkString("."), nestedFields)
      case ArrayType(elementType, _) =>
        checkNestedFields(spt, elementType)
      case _ => false
    }
  }

  /**
   * Update a nested field in a DataFrame using struct operations.
   *
   * @param df DataFrame to update
   * @param fieldPath Field path with dot notation (e.g., "address.city")
   * @param newValue Column expression for the new value
   * @return Updated DataFrame
   */
  def updateNestedField(df: DataFrame, fieldPath: String, newValue: Column): DataFrame = {
    val parts = fieldPath.split("\\.")

    if (parts.length == 1) {
      // Not actually nested
      df.withColumn(fieldPath, newValue)
    } else if (parts.length == 2) {
      // Simple nested case: parent.child
      updateSimpleNestedField(df, parts(0), parts(1), newValue)
    } else {
      // Deep nesting: recursively build struct
      updateDeepNestedField(df, parts, newValue)
    }
  }

  /**
   * Update a simple nested field (2 levels deep).
   */
  private def updateSimpleNestedField(
    df: DataFrame,
    parent: String,
    child: String,
    newValue: Column
  ): DataFrame = {
    val parentSchema = df.schema(parent).dataType.asInstanceOf[StructType]
    val updatedFields = parentSchema.fields.map { field =>
      if (field.name == child) {
        newValue.alias(child)
      } else {
        col(s"$parent.${field.name}").alias(field.name)
      }
    }

    df.withColumn(parent, struct(updatedFields: _*))
  }

  /**
   * Update a deeply nested field (3+ levels).
   */
  private def updateDeepNestedField(
    df: DataFrame,
    pathParts: Array[String],
    newValue: Column
  ): DataFrame = {
    val topLevel = pathParts(0)
    val topLevelSchema = df.schema(topLevel).dataType.asInstanceOf[StructType]

    val updatedStruct = buildNestedStructWithUpdate(
      topLevel,
      pathParts.tail,
      topLevelSchema,
      newValue
    )

    df.withColumn(topLevel, updatedStruct)
  }

  /**
   * Recursively build a struct with a field update at arbitrary depth.
   */
  private def buildNestedStructWithUpdate(
    basePath: String,
    remainingPath: Array[String],
    schema: StructType,
    newValue: Column
  ): Column = {
    if (remainingPath.length == 1) {
      // We've reached the target field
      val targetField = remainingPath(0)
      val updatedFields = schema.fields.map { field =>
        if (field.name == targetField) {
          newValue.alias(targetField)
        } else {
          col(s"$basePath.${field.name}").alias(field.name)
        }
      }
      struct(updatedFields: _*)
    } else {
      // Need to go deeper
      val currentField = remainingPath(0)
      val nestedSchema = schema(currentField).dataType.asInstanceOf[StructType]

      val nestedStruct = buildNestedStructWithUpdate(
        s"$basePath.$currentField",
        remainingPath.tail,
        nestedSchema,
        newValue
      )

      val updatedFields = schema.fields.map { field =>
        if (field.name == currentField) {
          nestedStruct.alias(currentField)
        } else {
          col(s"$basePath.${field.name}").alias(field.name)
        }
      }
      struct(updatedFields: _*)
    }
  }

  /**
   * Get the data type of a nested field by traversing the schema.
   *
   * @param schema Root schema
   * @param fieldPath Field path with dot notation
   * @return DataType of the nested field
   */
  def getNestedFieldType(schema: StructType, fieldPath: String): DataType = {
    val parts = fieldPath.split("\\.")

    @tailrec
    def traverse(currentSchema: StructType, remainingParts: List[String]): DataType = {
      remainingParts match {
        case Nil => throw new IllegalArgumentException(s"Empty field path")
        case head :: Nil =>
          currentSchema(head).dataType
        case head :: tail =>
          currentSchema(head).dataType match {
            case nested: StructType => traverse(nested, tail)
            case ArrayType(elementType: StructType, _) => traverse(elementType, tail)
            case other => throw new IllegalArgumentException(s"Cannot traverse non-struct type: $other")
          }
      }
    }

    traverse(schema, parts.toList)
  }
}
