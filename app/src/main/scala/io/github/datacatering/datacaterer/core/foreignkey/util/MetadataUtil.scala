package io.github.datacatering.datacaterer.core.foreignkey.util

import io.github.datacatering.datacaterer.api.model.Constants.OMIT
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.annotation.tailrec

/**
 * Utilities for manipulating DataFrame metadata, particularly for foreign key operations.
 */
object MetadataUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Combine metadata from source and target DataFrames, removing OMIT marker from source.
   *
   * @param sourceDf Source DataFrame
   * @param sourceCols Source column names
   * @param targetDf Target DataFrame
   * @param targetCols Target column names
   * @param df Result DataFrame to apply metadata to
   * @return DataFrame with combined metadata
   */
  def combineMetadata(
    sourceDf: DataFrame,
    sourceCols: List[String],
    targetDf: DataFrame,
    targetCols: List[String],
    df: DataFrame
  ): DataFrame = {
    val sourceColsMetadata = sourceCols.map(c => {
      val baseMetadata = getMetadata(c, sourceDf.schema.fields)
      new MetadataBuilder().withMetadata(baseMetadata).remove(OMIT).build()
    })
    val targetColsMetadata = targetCols.map(c => (c, getMetadata(c, targetDf.schema.fields)))
    val newMetadata = sourceColsMetadata.zip(targetColsMetadata).map(meta =>
      (meta._2._1, new MetadataBuilder().withMetadata(meta._2._2).withMetadata(meta._1).build())
    )

    newMetadata.foldLeft(df)((metaDf, meta) => withMetadata(metaDf, meta._1, meta._2))
  }

  /**
   * Apply metadata to a specific column in a DataFrame.
   *
   * @param df DataFrame
   * @param columnName Column name
   * @param metadata Metadata to apply
   * @return DataFrame with updated metadata
   */
  def withMetadata(df: DataFrame, columnName: String, metadata: Metadata): DataFrame = {
    val existingField = df.schema(columnName)
    val updatedField = StructField(columnName, existingField.dataType, existingField.nullable, metadata)
    val updatedSchema = StructType(
      df.schema.fields.map(field =>
        if (field.name == columnName) updatedField else field
      )
    )

    val sparkSession = df.sparkSession
    val rdd = df.rdd
    sparkSession.createDataFrame(rdd, updatedSchema)
  }

  /**
   * Get metadata for a field (supports nested fields with dot notation).
   *
   * @param field Field name (can use dot notation)
   * @param fields Array of struct fields
   * @return Metadata for the field, or empty metadata if not found
   */
  def getMetadata(field: String, fields: Array[StructField]): Metadata = {
    val optMetadata = if (field.contains(".")) {
      val spt = field.split("\\.")
      val optField = fields.find(_.name == spt.head)
      optField.map(field => checkNestedForMetadata(spt, field.dataType))
    } else {
      fields.find(_.name == field).map(_.metadata)
    }

    if (optMetadata.isEmpty) {
      LOGGER.warn(s"Unable to find metadata for field, defaulting to empty metadata, field-name=$field")
      Metadata.empty
    } else optMetadata.get
  }

  /**
   * Recursively traverse nested structures to find metadata.
   */
  @tailrec
  private def checkNestedForMetadata(spt: Array[String], dataType: DataType): Metadata = {
    dataType match {
      case StructType(nestedFields) => getMetadata(spt.tail.mkString("."), nestedFields)
      case ArrayType(elementType, _) => checkNestedForMetadata(spt, elementType)
      case _ => Metadata.empty
    }
  }
}
