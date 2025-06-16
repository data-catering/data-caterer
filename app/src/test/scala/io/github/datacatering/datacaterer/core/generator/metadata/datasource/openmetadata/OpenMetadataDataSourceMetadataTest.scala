package io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata

import io.github.datacatering.datacaterer.api.model.Constants.METADATA_IDENTIFIER
import io.github.datacatering.datacaterer.api.model.{ArrayType, BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampType}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.openmetadata.client.api.{TablesApi, TestCasesApi}
import org.openmetadata.client.model.Column.DataTypeEnum
import org.openmetadata.client.model.Table.ServiceTypeEnum
import org.openmetadata.client.model.{Column, ColumnProfile, EntityError, Table, TableList, TestCase, TestCaseList, TestCaseParameterValue}
import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.jdk.CollectionConverters.seqAsJavaListConverter
import scala.language.postfixOps

class OpenMetadataDataSourceMetadataTest extends SparkSuite with Matchers with MockFactory {

  private val dataTypeDataSourceMetadata = OpenMetadataDataSourceMetadata("name", "postgres", Map.empty)

  test("getSubDataSourcesMetadata should return correct metadata for tables") {
    val mockTablesApi = mock[TablesApi]
    val mockConnectionConfig = Map("key" -> "value")
    val dataSourceMetadata = new OpenMetadataDataSourceMetadata("name", "postgres", mockConnectionConfig) {
      override protected def getTablesClient: TablesApi = mockTablesApi
    }

    val mockTable = mock[Table]
    val mockColumn = mock[Column]
    val mockTableList = mock[TableList]
    val mockColumnProfile = mock[ColumnProfile]
    (mockTablesApi.listTables(_: TablesApi.ListTablesQueryParams)).expects(*).once().returns(mockTableList)
    (() => mockTableList.getData).expects().once().returns(List(mockTable).asJava)
    (() => mockTable.getServiceType).expects().once().returns(ServiceTypeEnum.POSTGRES)
    (() => mockTable.getFullyQualifiedName).expects().twice().returns("my-postgres.data-caterer.customer.accounts")
    (() => mockTable.getColumns).expects().once().returns(List(mockColumn).asJava)
    (() => mockColumn.getDataType).expects().twice().returns(DataTypeEnum.STRING)
    (() => mockColumn.getName).expects().once().returns("name")
    (() => mockColumn.getDescription).expects().twice().returns("desc")
    (() => mockColumn.getProfile).expects().twice().returns(mockColumnProfile)
    (() => mockColumnProfile.getMinLength).expects().once().returns(1)
    (() => mockColumnProfile.getMaxLength).expects().once().returns(10)
    (() => mockColumnProfile.getNullCount).expects().once().returns(0)

    val result = dataSourceMetadata.getSubDataSourcesMetadata(sparkSession)

    result.length shouldBe 1
    result.head.readOptions shouldBe Map("dbtable" -> "customer.accounts", "metadataIdentifier" -> "my-postgres.data-caterer.customer.accounts")
    result.head.optFieldMetadata shouldBe defined
    val fieldMetadata = result.head.optFieldMetadata.get.collect()
    fieldMetadata.length shouldBe 1
    fieldMetadata.head.field shouldBe "name"
    fieldMetadata.head.nestedFields.length shouldBe 0
    fieldMetadata.head.dataSourceReadOptions shouldBe Map("dbtable" -> "customer.accounts", "metadataIdentifier" -> "my-postgres.data-caterer.customer.accounts")
    fieldMetadata.head.metadata shouldBe Map("sourceDataType" -> "string", "type" -> "string", "description" -> "desc", "minLen" -> "1.0", "maxLen" -> "10.0", "isNullable" -> "false")
  }

  test("getDataSourceValidation should return correct validations") {
    //had to swap to use Mockito instead of ScalaMock due to the following error:
    //ScalaMock: Can't handle methods with more than 22 parameters
    val mockTestCasesApi = Mockito.mock(classOf[TestCasesApi])
    val mockConnectionConfig = Map("key" -> "value")
    val dataSourceMetadata = new OpenMetadataDataSourceMetadata("name", "postgres", mockConnectionConfig) {
      override protected def getTestCasesClient: TestCasesApi = mockTestCasesApi
    }

    val mockTestCases = Mockito.mock(classOf[TestCaseList])
    val mockTestCase = Mockito.mock(classOf[TestCase])
    val mockTestCaseParameterValue = Mockito.mock(classOf[TestCaseParameterValue])
    Mockito.when(mockTestCasesApi.listTestCases(any())).thenReturn(mockTestCases)
    Mockito.when(mockTestCases.getErrors).thenReturn(List.empty[EntityError].asJava)
    Mockito.when(mockTestCases.getData).thenReturn(List(mockTestCase).asJava)
    Mockito.when(mockTestCase.getEntityFQN).thenReturn("my-postgres.data-caterer.customer.accounts.name")
    Mockito.when(mockTestCase.getParameterValues).thenReturn(List(mockTestCaseParameterValue).asJava)
    Mockito.when(mockTestCase.getDescription).thenReturn("validation-desc")
    Mockito.when(mockTestCaseParameterValue.getName).thenReturn("value")
    Mockito.when(mockTestCaseParameterValue.getValue).thenReturn("100")

    val result = dataSourceMetadata.getDataSourceValidations(Map(METADATA_IDENTIFIER -> "my-postgres"))

    result.length shouldBe 1
    result.head.validation.description shouldBe Some("validation-desc")
  }

  test("dataTypeMapping should correctly map number data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.NUMBER)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe DoubleType
  }

  test("dataTypeMapping should correctly map numeric data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.NUMERIC)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe DoubleType
  }

  test("dataTypeMapping should correctly map double data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.DOUBLE)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe DoubleType
  }

  test("dataTypeMapping should correctly map tinyint data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.TINYINT)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe ShortType
  }

  test("dataTypeMapping should correctly map smallint data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.SMALLINT)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe ShortType
  }

  test("dataTypeMapping should correctly map bigint data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.BIGINT)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe IntegerType
  }

  test("dataTypeMapping should correctly map int data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.INT)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe IntegerType
  }

  test("dataTypeMapping should correctly map year data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.YEAR)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe IntegerType
  }

  test("dataTypeMapping should correctly map long data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.LONG)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe LongType
  }

  test("dataTypeMapping should correctly map decimal data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.DECIMAL)
    column.setScale(2)
    column.setPrecision(12)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result.toString shouldBe new DecimalType(12, 2).toString
  }

  test("dataTypeMapping should correctly map float data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.FLOAT)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe FloatType
  }

  test("dataTypeMapping should correctly map boolean data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.BOOLEAN)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe BooleanType
  }

  test("dataTypeMapping should correctly map binary data type") {
    List(DataTypeEnum.BLOB, DataTypeEnum.MEDIUMBLOB, DataTypeEnum.LONGBLOB, DataTypeEnum.BYTEA,
      DataTypeEnum.BYTES, DataTypeEnum.VARBINARY).foreach { dataType =>
      val column = new Column()
      column.setDataType(dataType)
      val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
      result shouldBe BinaryType
    }
  }

  test("dataTypeMapping should correctly map byte data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.BYTEINT)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe ByteType
  }

  test("dataTypeMapping should correctly map string data type") {
    List(DataTypeEnum.STRING, DataTypeEnum.TEXT, DataTypeEnum.VARCHAR, DataTypeEnum.CHAR,
      DataTypeEnum.JSON, DataTypeEnum.XML, DataTypeEnum.NTEXT, DataTypeEnum.IPV4, DataTypeEnum.IPV6,
      DataTypeEnum.CIDR, DataTypeEnum.UUID, DataTypeEnum.INET, DataTypeEnum.CLOB, DataTypeEnum.MACADDR,
      DataTypeEnum.ENUM, DataTypeEnum.MEDIUMTEXT).foreach { dataType =>
      val column = new Column()
      column.setDataType(dataType)
      val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
      result shouldBe StringType
    }
  }

  test("dataTypeMapping should correctly map date data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.DATE)
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result shouldBe DateType
  }

  test("dataTypeMapping should correctly map timestamp data type") {
    List(DataTypeEnum.TIMESTAMP, DataTypeEnum.TIMESTAMPZ, DataTypeEnum.DATETIME, DataTypeEnum.TIME).foreach { dataType =>
      val column = new Column()
      column.setDataType(dataType)
      val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
      result shouldBe TimestampType
    }
  }

  test("dataTypeMapping should correctly map array data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.ARRAY)
    column.setDataTypeDisplay("array<int>")
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result.toString shouldBe new ArrayType(IntegerType).toString
  }

  test("dataTypeMapping should correctly map array of varying characters data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.ARRAY)
    column.setDataTypeDisplay("array<character varying(255)>")
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result.toString shouldBe new ArrayType(StringType).toString
  }

  test("dataTypeMapping should correctly map array of characters data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.ARRAY)
    column.setDataTypeDisplay("array<char(255)>")
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result.toString shouldBe new ArrayType(StringType).toString
  }

  test("dataTypeMapping should correctly parse map data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.MAP)
    column.setDataTypeDisplay("map<string,string>")
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result.toString shouldBe new MapType(StringType, StringType).toString
  }

  test("dataTypeMapping should correctly parse struct data type") {
    val column = new Column()
    column.setDataType(DataTypeEnum.STRUCT)
    column.setDataTypeDisplay("struct<name:string,price:int>")
    val result = dataTypeDataSourceMetadata.dataTypeMapping(column)
    result.toString shouldBe new StructType(List("name" -> StringType, "price" -> IntegerType)).toString
  }
}
