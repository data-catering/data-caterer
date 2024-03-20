package io.github.datacatering.datacaterer.core.ui.mapper

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import spray.json.{JsString, JsValue, RootJsonFormat}

import scala.sys.error
import scala.util.Try

object DateTimeFormat extends RootJsonFormat[DateTime] {

  val parser: DateTimeFormatter = ISODateTimeFormat.dateOptionalTimeParser()

  def write(obj: DateTime): JsValue = {
    JsString(ISODateTimeFormat.dateTimeNoMillis().print(obj))
  }

  def read(json: JsValue): DateTime = json match {
    case JsString(s) => Try(parser.parseDateTime(s)).getOrElse(error(s))
    case _ => error(json.toString())
  }
}
