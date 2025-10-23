package io.github.datacatering.datacaterer.core.ui.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.github.datacatering.datacaterer.api.model.Field
import org.apache.spark.sql.types.StructType

@JsonIgnoreProperties(ignoreUnknown = true)
case class TaskFileSampleRequest(
  taskYamlPath: String,
  stepName: Option[String] = None,
  sampleSize: Int = 10,
  fastMode: Boolean = true,
  enableRelationships: Boolean = false
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class SchemaSampleRequest(
  fields: List[Field],
  format: String = "json",
  sampleSize: Int = 10,
  fastMode: Boolean = true,
  enableRelationships: Boolean = false
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class TaskYamlSampleRequest(
  taskYamlContent: String,
  stepName: Option[String] = None,
  sampleSize: Int = 10,
  fastMode: Boolean = true,
  enableRelationships: Boolean = false
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class SampleResponse(
  success: Boolean,
  executionId: String,
  schema: Option[SchemaInfo] = None,
  sampleData: Option[List[Map[String, Any]]] = None,
  metadata: Option[SampleMetadata] = None,
  error: Option[SampleError] = None,
  format: Option[String] = None
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class MultiSchemaSampleResponse(
  success: Boolean,
  executionId: String,
  samples: Map[String, List[Map[String, Any]]],
  metadata: Option[SampleMetadata] = None,
  error: Option[SampleError] = None
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class SampleError(
  code: String,
  message: String,
  details: Option[String] = None
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class SampleMetadata(
  sampleSize: Int,
  actualRecords: Int,
  generatedInMs: Long,
  fastModeEnabled: Boolean,
  relationshipsEnabled: Boolean = false
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class SchemaInfo(
  fields: List[SchemaField]
)

object SchemaInfo {
  def fromSparkSchema(schema: StructType): SchemaInfo = {
    SchemaInfo(schema.fields.map(SchemaField.fromSparkField).toList)
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class SchemaField(
  name: String,
  `type`: String,
  nullable: Boolean,
  fields: Option[List[SchemaField]] = None
)

object SchemaField {
  def fromSparkField(field: org.apache.spark.sql.types.StructField): SchemaField = {
    val fieldType = field.dataType match {
      case st: StructType => 
        SchemaField(
          name = field.name,
          `type` = "struct",
          nullable = field.nullable,
          fields = Some(st.fields.map(fromSparkField).toList)
        )
      case other => 
        SchemaField(
          name = field.name,
          `type` = other.typeName,
          nullable = field.nullable
        )
    }
    fieldType
  }
}