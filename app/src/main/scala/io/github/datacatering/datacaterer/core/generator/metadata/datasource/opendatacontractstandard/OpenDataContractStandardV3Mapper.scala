package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard

import io.github.datacatering.datacaterer.api.model.Constants.{ENABLED_NULL, FIELD_DATA_TYPE, FORMAT, IS_NULLABLE, IS_PRIMARY_KEY, IS_UNIQUE, MAXIMUM, MAXIMUM_LENGTH, METADATA_IDENTIFIER, MINIMUM, MINIMUM_LENGTH, PRIMARY_KEY_POSITION, REGEX_GENERATOR}
import io.github.datacatering.datacaterer.api.model.{ArrayType, BooleanType, DataType, DateType, DoubleType, IntegerType, StringType, StructType}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.SubDataSourceMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard.model.{LogicalTypeEnum, OpenDataContractStandardElementV3, OpenDataContractStandardLogicalTypeOptionsV3, OpenDataContractStandardSchemaV3, OpenDataContractStandardV3}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object OpenDataContractStandardV3Mapper {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val ODCS_CLASSIFICATION = "odcsClassification"
  private val ODCS_EXAMPLES = "odcsExamples"

  implicit val fieldMetadataEncoder: Encoder[FieldMetadata] = Encoders.kryo[FieldMetadata]

  def toSubDataSourceMetadata(
                               value: OpenDataContractStandardV3,
                               schema: OpenDataContractStandardSchemaV3,
                               connectionConfig: Map[String, String]
                             )(implicit sparkSession: SparkSession): SubDataSourceMetadata = {
    val readOptions = getDataSourceOptions(value, connectionConfig)
    val propertyMetadata = schema.properties.map(props => {
      val mappedProperties = props.map(property => toFieldMetadata(readOptions, property)).toList
      sparkSession.createDataset(mappedProperties)
    })
    SubDataSourceMetadata(readOptions, propertyMetadata)
  }

  private def toFieldMetadata(
                               readOptions: Map[String, String],
                               property: OpenDataContractStandardElementV3
                             ): FieldMetadata = {
    val dataType = getDataType(property)
    val metadata = getBasePropertyMetadata(property, dataType)
    val nestedFields = if (dataType.isInstanceOf[StructType]) {
      property.properties.getOrElse(Array())
        .map(prop2 => toFieldMetadata(readOptions, prop2))
        .toList
    } else List()

    FieldMetadata(property.name, readOptions, metadata, nestedFields)
  }

  private def getDataSourceOptions(
                                    contract: OpenDataContractStandardV3,
                                    connectionConfig: Map[String, String]
                                  ): Map[String, String] = {
    val baseMap = Map(METADATA_IDENTIFIER -> contract.id)
    baseMap ++ connectionConfig
  }

  private def getBasePropertyMetadata(property: OpenDataContractStandardElementV3, dataType: DataType): Map[String, String] = {
    val baseMetadata = Map(
      FIELD_DATA_TYPE -> dataType.toString(),
      IS_NULLABLE -> property.required.getOrElse(false).toString,
      ENABLED_NULL -> property.required.getOrElse(false).toString,
      IS_PRIMARY_KEY -> property.primaryKey.getOrElse(false).toString,
      PRIMARY_KEY_POSITION -> property.primaryKeyPosition.getOrElse("-1").toString,
      IS_UNIQUE -> property.unique.getOrElse(false).toString,
    )

    // Add logicalTypeOptions metadata for data generation
    val typeOptionsMetadata = property.logicalTypeOptions.map(getLogicalTypeOptionsMetadata).getOrElse(Map.empty)

    // Add examples as metadata (for documentation/reference only, not for generation)
    val examplesMetadata = property.examples match {
      case Some(examples) if examples.nonEmpty =>
        val examplesStr = examples.map(_.toString).mkString(",")
        Map(ODCS_EXAMPLES -> examplesStr)
      case _ => Map.empty
    }

    // Add classification if present
    val classificationMetadata = property.classification match {
      case Some(classification) => Map(ODCS_CLASSIFICATION -> classification)
      case _ => Map.empty
    }

    baseMetadata ++ typeOptionsMetadata ++ examplesMetadata ++ classificationMetadata
  }

  private def getLogicalTypeOptionsMetadata(options: OpenDataContractStandardLogicalTypeOptionsV3): Map[String, String] = {
    var metadata = Map.empty[String, String]

    // String constraints
    options.minLength.foreach(v => metadata += (MINIMUM_LENGTH -> v.toString))
    options.maxLength.foreach(v => metadata += (MAXIMUM_LENGTH -> v.toString))
    options.pattern.foreach(v => metadata += (REGEX_GENERATOR -> v))
    options.format.foreach(v => metadata += (FORMAT -> v))

    // Numeric constraints (min/max supported by Data Caterer)
    options.minimum.foreach(v => metadata += (MINIMUM -> v.toString))
    options.maximum.foreach(v => metadata += (MAXIMUM -> v.toString))

    // Note: multipleOf is not supported by Data Caterer's generation system
    // Note: Array constraints (minItems, maxItems, uniqueItems) and object constraints
    // are not currently used in Data Caterer's field-level generation

    metadata
  }

  private def getDataType(element: OpenDataContractStandardElementV3): DataType = {
    element.logicalType match {
      case LogicalTypeEnum.string => StringType
      case LogicalTypeEnum.date => DateType
      case LogicalTypeEnum.number => DoubleType
      case LogicalTypeEnum.integer => IntegerType
      case LogicalTypeEnum.array => new ArrayType(StringType)
      case LogicalTypeEnum.`object` =>
        val innerType = element.properties.getOrElse(Array())
          .map(prop => prop.name -> getDataType(prop))
          .toList
        new StructType(innerType)
      case LogicalTypeEnum.boolean => BooleanType
      case x =>
        LOGGER.warn(s"Unable to find corresponding known data type for field in ODCS file, defaulting to string, property=$element, data-type=$x")
        StringType
    }
  }
}
