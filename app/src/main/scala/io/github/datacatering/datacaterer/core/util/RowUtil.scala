package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.core.exception.{InvalidFieldAsDataTypeException, MissingFieldException}
import org.apache.log4j.Logger
import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}

object RowUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getRowValue[T](row: Row, colName: String, default: T = null): T = {
    val hasField = row.schema.fields.exists(_.name.equalsIgnoreCase(colName))
    if (hasField) {
      val tryGetAsType = Try(row.getAs[T](colName))
      tryGetAsType match {
        case Failure(exception) =>
          val message = s"Failed to get field as data type, field-name=$colName, exception=$exception"
          LOGGER.error(message)
          throw InvalidFieldAsDataTypeException(colName, exception)
        case Success(value) => value
      }
    } else if (default == null) {
      throw MissingFieldException(colName)
    } else {
      LOGGER.debug(s"Field missing from schema definition, will revert to default value, field-name=$colName, default=$default")
      default
    }
  }

}
