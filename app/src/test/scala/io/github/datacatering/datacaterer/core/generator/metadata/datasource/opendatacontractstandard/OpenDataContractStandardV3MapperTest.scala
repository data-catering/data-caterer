package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard

import io.github.datacatering.datacaterer.api.model.Constants.FIELD_DATA_TYPE
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard.model.{ApiVersionEnum, KindEnum, LogicalTypeEnum, OpenDataContractStandardDescription, OpenDataContractStandardElementV3, OpenDataContractStandardSchemaV3, OpenDataContractStandardV3}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OpenDataContractStandardV3MapperTest extends SparkSuite {

  private val mapper = OpenDataContractStandardV3Mapper
  private val baseContract = createBaseContract

  test("OpenDataContractStandardV3Mapper correctly map to SubDataSourceMetadata") {
    val value = OpenDataContractStandardSchemaV3(
      name = "Test Dataset",
      properties = Some(Array(
        OpenDataContractStandardElementV3(
          name = "field1",
          logicalType = LogicalTypeEnum.string,
          physicalType = "string",
        )
      ))
    )
    val connectionConfig = Map("key" -> "value")

    val result = mapper.toSubDataSourceMetadata(baseContract, value, connectionConfig)(sparkSession)

    assert(result.optFieldMetadata.isDefined)
    val fields = result.optFieldMetadata.get.collect()
    assertResult(1)(fields.length)
    val headField = fields.head
    assertResult("field1")(headField.field)
    assertResult("string")(headField.metadata(FIELD_DATA_TYPE))
  }

  test("OpenDataContractStandardV3Mapper handle dataset with multiple fields") {
    val schemaFields = Array(
      OpenDataContractStandardElementV3(name = "field1", logicalType = LogicalTypeEnum.string, physicalType = "string"),
      OpenDataContractStandardElementV3(name = "field2", logicalType = LogicalTypeEnum.integer, physicalType = "int"),
      OpenDataContractStandardElementV3(name = "field3", logicalType = LogicalTypeEnum.boolean, physicalType = "bool"),
      OpenDataContractStandardElementV3(name = "field4", logicalType = LogicalTypeEnum.number, physicalType = "double"),
      OpenDataContractStandardElementV3(name = "field5", logicalType = LogicalTypeEnum.date, physicalType = "timestamp"),
      OpenDataContractStandardElementV3(name = "field6", logicalType = LogicalTypeEnum.array, physicalType = "array"),
      OpenDataContractStandardElementV3(
        name = "field7",
        logicalType = LogicalTypeEnum.`object`,
        physicalType = "struct<name:str,age:int>",
        properties = Some(Array(
          OpenDataContractStandardElementV3(name = "name", logicalType = LogicalTypeEnum.string, physicalType = "string"),
          OpenDataContractStandardElementV3(name = "age", logicalType = LogicalTypeEnum.integer, physicalType = "int"),
        ))
      )
    )
    val value = OpenDataContractStandardSchemaV3(name = "my big data", properties = Some(schemaFields))
    val connectionConfig = Map("format" -> "parquet")

    val result = mapper.toSubDataSourceMetadata(baseContract, value, connectionConfig)(sparkSession)

    assert(result.optFieldMetadata.isDefined)
    val fields = result.optFieldMetadata.get.collect()
    assertResult(7)(fields.length)
    assertResult("field1")(fields.head.field)
    assertResult("string")(fields.head.metadata(FIELD_DATA_TYPE))
    assertResult("field2")(fields(1).field)
    assertResult("integer")(fields(1).metadata(FIELD_DATA_TYPE))
    assertResult("field3")(fields(2).field)
    assertResult("boolean")(fields(2).metadata(FIELD_DATA_TYPE))
    assertResult("field4")(fields(3).field)
    assertResult("double")(fields(3).metadata(FIELD_DATA_TYPE))
    assertResult("field5")(fields(4).field)
    assertResult("date")(fields(4).metadata(FIELD_DATA_TYPE))
    assertResult("field6")(fields(5).field)
    assertResult("array<string>")(fields(5).metadata(FIELD_DATA_TYPE))
    assertResult("field7")(fields(6).field)
    assertResult("struct<name: string,age: integer>")(fields(6).metadata(FIELD_DATA_TYPE))
    assertResult(2)(fields(6).nestedFields.size)
    assertResult("name")(fields(6).nestedFields.head.field)
    assertResult("string")(fields(6).nestedFields.head.metadata(FIELD_DATA_TYPE))
    assertResult("age")(fields(6).nestedFields(1).field)
    assertResult("integer")(fields(6).nestedFields(1).metadata(FIELD_DATA_TYPE))
  }

  private def createBaseContract: OpenDataContractStandardV3 = {
    OpenDataContractStandardV3(
      apiVersion = ApiVersionEnum.`v3.0.0`,
      id = "abc123",
      kind = KindEnum.DataContract,
      status = "current",
      version = "1.0",
      name = Some("Test Dataset"),
      description = Some(OpenDataContractStandardDescription(purpose = Some("Test Description")))
    )
  }
}
