package io.github.datacatering.datacaterer.core.generator.delete

import io.github.datacatering.datacaterer.api.model.Constants.MYSQL_DRIVER
import io.github.datacatering.datacaterer.core.exception.InvalidDataSourceOptions
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, PreparedStatement}

class JdbcDeleteRecordServiceTest extends SparkSuite with Matchers with MockFactory {

  private val options = Map("url" -> "jdbc:mysql://localhost:3306/test", "user" -> "root", "password" -> "password", "dbtable" -> "test_table", "driver" -> MYSQL_DRIVER)

  private val schema = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true)
  ))

  private val data: Seq[Row] = Seq(
    Row(1, "Alice"),
    Row(2, "Bob")
  )

  private val trackedRecords: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), schema).repartition(1)

  test("deleteRecords should delete records successfully") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    service.deleteRecords("testDataSource", trackedRecords, options)

    Mockito.verify(SerializableMockConnectionProvider.mockConnection, Mockito.times(1)).prepareStatement(ArgumentMatchers.anyString())
    Mockito.verify(mockPreparedStatement, Mockito.times(2)).addBatch()
    Mockito.verify(mockPreparedStatement, Mockito.times(1)).executeBatch()
    Mockito.verify(mockPreparedStatement, Mockito.times(1)).clearBatch()
    Mockito.verify(SerializableMockConnectionProvider.mockConnection, Mockito.times(1)).close()
  }

  test("deleteRecords should throw InvalidDataSourceOptions when table name is not provided") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val optionsWithoutTable = options - "dbtable"

    val exception = intercept[InvalidDataSourceOptions] {
      service.deleteRecords("testDataSource", trackedRecords, optionsWithoutTable)
    }

    exception should have message "Missing config for data source connection, data-source-name=testDataSource, missing-config=dbtable"
  }

  test("dataTypeMapping should set the prepared statement correctly for StringType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row("Alice")
    val field = (StructField("name", StringType, nullable = true), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setString(1, "Alice")
  }

  test("dataTypeMapping should set the prepared statement correctly for IntegerType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(1)
    val field = (StructField("id", IntegerType, nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setInt(1, 1)
  }

  test("dataTypeMapping should set the prepared statement correctly for ShortType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(1.toShort)
    val field = (StructField("id", ShortType, nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setShort(1, 1)
  }

  test("dataTypeMapping should set the prepared statement correctly for LongType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(1L)
    val field = (StructField("id", LongType, nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setLong(1, 1L)
  }

  test("dataTypeMapping should set the prepared statement correctly for DecimalType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(new java.math.BigDecimal(1.0))
    val field = (StructField("id", DecimalType(10, 2), nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setBigDecimal(1, new java.math.BigDecimal(1.0))
  }

  test("dataTypeMapping should set the prepared statement correctly for DoubleType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(1.0)
    val field = (StructField("id", DoubleType, nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setDouble(1, 1.0)
  }

  test("dataTypeMapping should set the prepared statement correctly for FloatType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(1.0f)
    val field = (StructField("id", FloatType, nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setFloat(1, 1.0f)
  }

  test("dataTypeMapping should set the prepared statement correctly for BooleanType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(true)
    val field = (StructField("id", BooleanType, nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setBoolean(1, true)
  }

  test("dataTypeMapping should set the prepared statement correctly for ByteType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(1.toByte)
    val field = (StructField("id", ByteType, nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setByte(1, 1.toByte)
  }

  test("dataTypeMapping should set the prepared statement correctly for TimestampType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(java.sql.Timestamp.valueOf("2021-01-01 00:00:00"))
    val field = (StructField("id", TimestampType, nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setTimestamp(1, java.sql.Timestamp.valueOf("2021-01-01 00:00:00"))
  }

  test("dataTypeMapping should set the prepared statement correctly for TimestampType for MySQL with nanos greater than 500000000") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)
    val optionsWithDriver = options + ("driver" -> MYSQL_DRIVER)

    val row = Row(java.sql.Timestamp.valueOf("2021-01-01 00:00:00.500000001"))
    val field = (StructField("id", TimestampType, nullable = false), 0)

    service.dataTypeMapping(optionsWithDriver, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setTimestamp(1, java.sql.Timestamp.valueOf("2021-01-01 00:00:01"))
  }

  test("dataTypeMapping should set the prepared statement correctly for DateType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(java.sql.Date.valueOf("2021-01-01"))
    val field = (StructField("id", DateType, nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setDate(1, java.sql.Date.valueOf("2021-01-01"))
  }

  test("dataTypeMapping should set the prepared statement correctly for BinaryType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)
    val byteArray = Array[Byte](1, 2, 3)

    val row = Row(byteArray)
    val field = (StructField("id", BinaryType, nullable = false), 0)

    service.dataTypeMapping(options, "test_table", SerializableMockConnectionProvider.mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setBinaryStream(ArgumentMatchers.eq(1), ArgumentMatchers.any())
  }

  test("dataTypeMapping should set the prepared statement correctly for ArrayType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    val mockConnection = Mockito.mock(classOf[Connection])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)
    Mockito.when(SerializableMockConnectionProvider.mockConnection.createArrayOf(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(null)

    val row = Row(Seq(1, 2, 3))
    val field = (StructField("id", ArrayType(IntegerType), nullable = false), 0)

    service.dataTypeMapping(options, "test_table", mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setArray(1, null)
  }

  test("dataTypeMapping should set the prepared statement correctly for MapType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    val mockConnection = Mockito.mock(classOf[Connection])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(Map("key" -> "value"))
    val field = (StructField("id", MapType(StringType, StringType), nullable = false), 0)

    service.dataTypeMapping(options, "test_table", mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setObject(1, java.util.Map.of("key", "value"))
  }

  test("dataTypeMapping should set the prepared statement correctly for StructType") {
    val service = new JdbcDeleteRecordService(SerializableMockConnectionProvider)
    val mockPreparedStatement = Mockito.mock(classOf[PreparedStatement])
    val mockConnection = Mockito.mock(classOf[Connection])
    Mockito.when(SerializableMockConnectionProvider.mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)

    val row = Row(Row(1, "Alice"))
    val field = (StructField("id", StructType(Seq(StructField("id", IntegerType), StructField("name", StringType))), nullable = false), 0)

    service.dataTypeMapping(options, "test_table", mockConnection, mockPreparedStatement, row, field, 1)

    Mockito.verify(mockPreparedStatement, Mockito.times(1)).setObject(1, Row(1, "Alice"))
  }

  object TestConnectionProvider extends JdbcConnectionProvider with Serializable {
    override def getDbConnection(dataSourceName: String, options: Map[String, String]): Connection = {
      val mockConnection = mock[Connection]
      mockConnection
    }
  }

}

object SerializableMockConnectionProvider extends JdbcConnectionProvider with Serializable {
  @transient lazy val mockConnection: Connection = Mockito.mock(classOf[Connection])

  override def getDbConnection(dataSourceName: String, options: Map[String, String]): Connection = mockConnection
}
