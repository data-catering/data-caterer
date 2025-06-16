package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{JSON_SCHEMA, JSON_SCHEMA_FILE, METADATA_IDENTIFIER}
import io.github.datacatering.datacaterer.api.model.Field
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

case class JsonSchemaDataSourceMetadata(
                                         name: String,
                                         format: String,
                                         connectionConfig: Map[String, String]
                                       ) extends DataSourceMetadata {

  require(
    connectionConfig.contains(JSON_SCHEMA_FILE),
    s"Configuration missing for JSON schema metadata source, metadata-source=$name, missing-configuration=$JSON_SCHEMA_FILE"
  )

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val JSON_SCHEMA_FILE_PATH = connectionConfig(JSON_SCHEMA_FILE)

  override val hasSourceData: Boolean = false

  override def getDataSourceValidations(dataSourceReadOptions: Map[String, String]): List[ValidationBuilder] = {
    val schemaFile = dataSourceReadOptions(JSON_SCHEMA_FILE)
    LOGGER.info(s"Retrieving JSON schema validations from file, file=$schemaFile")

    try {
      val schema = JsonSchemaParser.parseSchema(schemaFile)
      val validations = JsonSchemaDataValidations.getDataValidations(schema, schemaFile)
      if (validations.nonEmpty) {
        LOGGER.info(s"Found JSON schema validations and converted to data validations, num-validations=${validations.size}")
      } else {
        LOGGER.warn("No JSON schema validations found or generated")
      }
      validations
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to generate validations from JSON schema file: $schemaFile", ex)
        List()
    }
  }

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    try {
      LOGGER.info(s"Parsing JSON schema from file: $JSON_SCHEMA_FILE_PATH")
      val schema = JsonSchemaParser.parseSchema(JSON_SCHEMA_FILE_PATH)

      LOGGER.debug(s"Converting JSON schema to Data Caterer fields with $$ref support, schema-type=${schema.`type`}")
      val fields = JsonSchemaConverter.convertSchemaWithRefs(schema, JSON_SCHEMA_FILE_PATH)
      val baseOptions = connectionConfig ++ Map(
        METADATA_IDENTIFIER -> toMetadataIdentifier(JSON_SCHEMA_FILE_PATH)
      )

      LOGGER.debug(s"Converting fields to nested structure, root-fields-count=${fields.size}")
      val nestedFieldMetadata = convertToNestedFieldMetadata(fields)

      LOGGER.debug(s"Created field metadata: ${nestedFieldMetadata.size} nested fields total")

      Array(SubDataSourceMetadata(baseOptions, Some(sparkSession.createDataset(nestedFieldMetadata))))
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to parse JSON schema file: $JSON_SCHEMA_FILE_PATH", ex)
        throw ex
    }
  }

  /**
   * Convert nested Field objects to FieldMetadata with proper nesting (without flattening)
   * This preserves the hierarchical structure which is essential for correct SQL generation
   */
  private def convertToNestedFieldMetadata(fields: List[Field]): List[FieldMetadata] = {
    fields.map(field => {
      val metadata = Map(
        "type" -> field.`type`.getOrElse("string"),
        "nullable" -> field.nullable.toString
      ) ++ field.options.map { case (k, v) => k -> v.toString }
      
      val nestedFieldMetadata = if (field.fields.nonEmpty) {
        convertToNestedFieldMetadata(field.fields)
      } else {
        List.empty
      }
      
      FieldMetadata(
        field = field.name,
        dataSourceReadOptions = Map(METADATA_IDENTIFIER -> toMetadataIdentifier(JSON_SCHEMA_FILE_PATH)),
        metadata = metadata,
        nestedFields = nestedFieldMetadata
      )
    })
  }

  private def toMetadataIdentifier(schemaFilePath: String): String = {
    // Extract just the filename from the path to create a clean identifier
    val fileName = schemaFilePath.split("/").last.replaceAll("[^a-zA-Z0-9_-]", "_")
    s"${JSON_SCHEMA}_$fileName"
  }
} 