package io.github.datacatering.datacaterer.core.generator.delete

import io.github.datacatering.datacaterer.api.model.Constants.{DRIVER, JDBC_TABLE, MYSQL_DRIVER, PASSWORD, URL, USERNAME}
import io.github.datacatering.datacaterer.core.exception.{InvalidDataSourceOptions, UnsupportedJdbcDeleteDataType}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.ByteArrayInputStream
import java.sql.{DriverManager, Timestamp}

class JdbcDeleteRecordService extends DeleteRecordService {

  override def deleteRecords(dataSourceName: String, trackedRecords: DataFrame, options: Map[String, String])(implicit sparkSession: SparkSession): Unit = {
    val table = options.getOrElse(JDBC_TABLE, throw InvalidDataSourceOptions(dataSourceName, JDBC_TABLE))
    val whereClauseFields = trackedRecords.columns.map(c => s"$c = ?").mkString(" AND ")

    trackedRecords.rdd.foreachPartition(partition => {
      val url = options.getOrElse(URL, throw InvalidDataSourceOptions(dataSourceName, URL))
      val username = options.getOrElse(USERNAME, throw InvalidDataSourceOptions(dataSourceName, USERNAME))
      val password = options.getOrElse(PASSWORD, throw InvalidDataSourceOptions(dataSourceName, PASSWORD))
      val connection = DriverManager.getConnection(url, username, password)
      val preparedStatement = connection.prepareStatement(s"DELETE FROM $table WHERE $whereClauseFields")

      partition.grouped(BATCH_SIZE).foreach(batch => {
        batch.foreach(r => {
          r.schema.fields.zipWithIndex.foreach(field => {
            val preparedIndex = field._2 + 1
            field._1.dataType match {
              case StringType => preparedStatement.setString(preparedIndex, r.getString(field._2))
              case ShortType => preparedStatement.setShort(preparedIndex, r.getShort(field._2))
              case IntegerType => preparedStatement.setInt(preparedIndex, r.getInt(field._2))
              case LongType => preparedStatement.setLong(preparedIndex, r.getLong(field._2))
              case DecimalType() => preparedStatement.setBigDecimal(preparedIndex, r.getDecimal(field._2))
              case DoubleType => preparedStatement.setDouble(preparedIndex, r.getDouble(field._2))
              case FloatType => preparedStatement.setFloat(preparedIndex, r.getFloat(field._2))
              case BooleanType => preparedStatement.setBoolean(preparedIndex, r.getBoolean(field._2))
              case ByteType => preparedStatement.setByte(preparedIndex, r.getByte(field._2))
              case TimestampType => preparedStatement.setTimestamp(preparedIndex, getTimestamp(r, field, options(DRIVER)))
              case DateType => preparedStatement.setDate(preparedIndex, r.getDate(field._2))
              case BinaryType =>
                val byteArray = new ByteArrayInputStream(r.get(field._2).asInstanceOf[Array[Byte]])
                preparedStatement.setBinaryStream(preparedIndex, byteArray)
              case ArrayType(elementType, _) =>
                val array = connection.createArrayOf(elementType.sql, r.getList[Any](field._2).toArray)
                preparedStatement.setArray(preparedIndex, array)
              case MapType(_, _, _) => preparedStatement.setObject(preparedIndex, r.getJavaMap(field._2))
              case StructType(_) => preparedStatement.setObject(preparedIndex, r.getStruct(field._2))
              case x => throw UnsupportedJdbcDeleteDataType(x, table)
            }
          })
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        preparedStatement.clearBatch()
      })
      connection.close()
    })
  }

  private def getTimestamp(r: Row, field: (StructField, Int), driver: String) = {
    if (driver.equalsIgnoreCase(MYSQL_DRIVER)) {
      val localDateTime = r.getTimestamp(field._2).toLocalDateTime
      val roundedTime = if (localDateTime.getNano >= 500000000) {
        localDateTime.plusSeconds(1)
      } else {
        localDateTime
      }
      Timestamp.valueOf(roundedTime.withNano(0))
    } else {
      r.getTimestamp(field._2)
    }
  }
}
