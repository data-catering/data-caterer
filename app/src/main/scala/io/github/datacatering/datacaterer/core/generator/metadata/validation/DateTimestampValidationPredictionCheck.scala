package io.github.datacatering.datacaterer.core.generator.metadata.validation

import io.github.datacatering.datacaterer.api.ValidationBuilder
import org.apache.spark.sql.types.{DateType, StructField, TimestampType}

class DateTimestampValidationPredictionCheck extends ValidationPredictionCheck {

  private val startFieldNames = List("starting", "started", "start", "opening", "opened", "open", "beginning", "begin",
    "creating", "created", "create", "placed", "first", "initiated", "initiate", "embark", "depart", "activate")
  private val intermediateFieldNames = List("updating", "updated", "update", "dispatched", "dispatch")
  private val endFieldNames = List("stopping", "stopped", "stop", "closing", "closed", "close", "ending",
    "ended", "end", "delivered", "deliver", "last", "completed", "complete", "finishing", "finished", "finish", "terminated",
    "terminate", "settled", "fulfilled", "fulfil", "arrive", "arrival")

  override def check(fields: Array[StructField]): List[ValidationBuilder] = {
    val dateFields = fields.filter(_.dataType == DateType)
    val timestampFields = fields.filter(_.dataType == TimestampType)

    val (startWithInterDate, startWithEndDate, interWithEndDate) = getGroupingOfFields(dateFields)
    val (startWithInterTs, startWithEndTs, interWithEndTs) = getGroupingOfFields(timestampFields)

    mapToValidation(startWithInterDate ++ startWithEndDate ++ interWithEndDate) ++
      mapToValidation(startWithInterTs ++ startWithEndTs ++ interWithEndTs) ++
      fields.flatMap(check)
  }

  override def check(field: StructField): List[ValidationBuilder] = {
    //maybe add in date of birth check is within 150 years?
    List()
  }

  private def getGroupingOfFields(fields: Array[StructField]) = {
    val startFields = fields.filter(f => startFieldNames.exists(f.name.contains))
    val intermediateFields = fields.filter(f => intermediateFieldNames.exists(f.name.contains))
    val endFields = fields.filter(f => endFieldNames.exists(f.name.contains))

    val startWithInter = getPrevAndNextFields(startFields, intermediateFields)
    val startWithEnd = getPrevAndNextFields(startFields, endFields)
    val interWithEnd = getPrevAndNextFields(intermediateFields, endFields)
    (startWithInter, startWithEnd, interWithEnd)
  }

  private def mapToValidation(prevAndNextFields: Array[(StructField, StructField)]): List[ValidationBuilder] = {
    prevAndNextFields.map(fields => {
      val cast = fields._1.dataType.sql
      val expr = s"$cast(${fields._1.name}) <= $cast(${fields._2.name})"
      ValidationBuilder().expr(expr)
    }).toList
  }

  private def getPrevAndNextFields(prevFields: Array[StructField], nextFields: Array[StructField]): Array[(StructField, StructField)] = {
    prevFields.map(prevF => {
      val prevName = cleanName(prevF)
      val nextField = nextFields.filter(nextF => {
        val nextName = cleanName(nextF)
        prevName == nextName
      })
      (prevF, nextField.headOption)
    })
      .filter(_._2.isDefined)
      .map(x => (x._1, x._2.get))
  }

  private def cleanName(field: StructField): String = {
    val allFilterWords = s"${startFieldNames.mkString("|")}|${intermediateFieldNames.mkString("|")}|${endFieldNames.mkString("|")}"
    field.name.toLowerCase.replaceAll(allFilterWords, "")
  }
}