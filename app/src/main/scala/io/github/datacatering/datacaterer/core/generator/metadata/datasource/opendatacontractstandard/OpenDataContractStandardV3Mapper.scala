package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard

import io.github.datacatering.datacaterer.api.model.Constants.{ENABLED_NULL, FIELD_DATA_TYPE, IS_NULLABLE, IS_PRIMARY_KEY, IS_UNIQUE, METADATA_IDENTIFIER, PRIMARY_KEY_POSITION}
import io.github.datacatering.datacaterer.api.model.{ArrayType, BooleanType, DataType, DateType, DoubleType, IntegerType, StringType, StructType}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.SubDataSourceMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard.model.{LogicalTypeEnum, OpenDataContractStandardElementV3, OpenDataContractStandardSchemaV3, OpenDataContractStandardV3}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object OpenDataContractStandardV3Mapper {

  private val LOGGER = Logger.getLogger(getClass.getName)

  implicit val columnMetadataEncoder: Encoder[FieldMetadata] = Encoders.kryo[FieldMetadata]

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
    Map(
      FIELD_DATA_TYPE -> dataType.toString(),
      IS_NULLABLE -> property.required.getOrElse(false).toString,
      ENABLED_NULL -> property.required.getOrElse(false).toString,
      IS_PRIMARY_KEY -> property.primaryKey.getOrElse(false).toString,
      PRIMARY_KEY_POSITION -> property.primaryKeyPosition.getOrElse("-1").toString,
      IS_UNIQUE -> property.unique.getOrElse(false).toString,
    )
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
        LOGGER.warn(s"Unable to find corresponding known data type for column in ODCS file, defaulting to string, property=$element, data-type=$x")
        StringType
    }
  }
}
