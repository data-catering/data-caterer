package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{DataType, Field, IntegerType, Task}
import io.github.datacatering.datacaterer.api.{FieldBuilder, HttpMethodEnum, HttpQueryParameterStyleEnum}
import io.github.datacatering.datacaterer.core.generator.provider.OneOfDataGenerator
import io.github.datacatering.datacaterer.core.util.PlanImplicits.FieldOps

import scala.jdk.CollectionConverters._

object TaskConversionRegistry {

  private case class FieldConversionRule(
                                          name: String,
                                          isApplicable: Field => Boolean,
                                          convert: Field => List[Field]
                                        )

  private val taskConversions: List[Task => Task] = List(
    convertTaskNumbersToString,
    applyFieldConversions
  )

  private val fieldConversionRules: List[FieldConversionRule] = List(
    FieldConversionRule(
      "message-headers",
      field => field.name == YAML_REAL_TIME_HEADERS_FIELD && field.fields.nonEmpty,
      field => {
        val headerFields = field.fields.flatMap(innerField => {
          convertField(innerField).map(convertedField => {
            val sqlExpr = convertedField.static.getOrElse(convertedField.options.getOrElse(SQL_GENERATOR, "").toString)
            FieldBuilder().messageHeader(convertedField.name, sqlExpr)
          })
        })
        val messageHeaders = FieldBuilder().messageHeaders(headerFields: _*)
        List(messageHeaders.field)
      }
    ),
    FieldConversionRule(
      "message-body",
      field => field.name == YAML_REAL_TIME_BODY_FIELD && field.fields.nonEmpty,
      field => {
        val innerFields = field.fields.flatMap(innerField => convertField(innerField).map(FieldBuilder))
        val messageBodyBuilder = FieldBuilder().messageBody(innerFields: _*)
        messageBodyBuilder.map(_.field)
      }
    ),
    FieldConversionRule(
      "http-url",
      field => field.name == YAML_HTTP_URL_FIELD && field.fields.nonEmpty,
      field => {
        val innerFields = field.fields.flatMap(innerField => convertField(innerField).map(FieldBuilder))
        val urlField = innerFields.find(f => f.field.name == REAL_TIME_URL_FIELD).flatMap(f => f.field.static)
        val methodField = innerFields.find(f => f.field.name == REAL_TIME_METHOD_FIELD).flatMap(f => f.field.static)
        val pathParams = innerFields.find(f => f.field.name == HTTP_PATH_PARAM_FIELD_PREFIX)
          .map(f => f.field.fields.map(f1 => FieldBuilder(f1).httpPathParam(f1.name)))
          .getOrElse(List())
        val queryParams = innerFields.find(f => f.field.name == HTTP_QUERY_PARAM_FIELD_PREFIX)
          .map(f => f.field.fields.map(f1 =>
            FieldBuilder(f1).httpQueryParam(
              f1.name,
              DataType.fromString(f1.`type`.getOrElse("string")),
              f1.options.get("style")
                .map(style => HttpQueryParameterStyleEnum.withName(style.toString))
                .getOrElse(HttpQueryParameterStyleEnum.FORM),
              f1.options.get("explode").forall(_.toString.toBoolean)
            )
          ))
          .getOrElse(List())
        FieldBuilder().httpUrl(
          urlField.get,
          HttpMethodEnum.withName(methodField.get),
          pathParams,
          queryParams
        ).map(_.field)
      }
    ),
    FieldConversionRule(
      "http-headers",
      field => field.name == YAML_HTTP_HEADERS_FIELD && field.fields.nonEmpty,
      field => field.fields.map(innerField => FieldBuilder(innerField).httpHeader(innerField.name).field)
    ),
    FieldConversionRule(
      "http-body",
      field => field.name == YAML_HTTP_BODY_FIELD && field.fields.nonEmpty,
      field => {
        val innerFields = field.fields.flatMap(innerField => convertField(innerField).map(FieldBuilder))
        val httpBody = FieldBuilder().httpBody(innerFields: _*)
        httpBody.map(_.field)
      }
    ),
    FieldConversionRule(
      "email-helper",
      field => field.fields.isEmpty && field.options.contains("email"),
      field => {
        val domainOpt = extractStringOrMap(field.options("email"), "domain").filter(_.nonEmpty)
        val baseField = removeOption(field, "email")
        val updatedField = domainOpt match {
          case Some(domain) => FieldBuilder(baseField).email(domain).field
          case None => FieldBuilder(baseField).email().field
        }
        List(updatedField)
      }
    ),
    FieldConversionRule(
      "phone-helper",
      field => field.fields.isEmpty && field.options.contains("phone"),
      field => {
        val format = extractStringOrMap(field.options("phone"), "format").filter(_.nonEmpty).getOrElse("US")
        val baseField = removeOption(field, "phone")
        List(FieldBuilder(baseField).phone(format).field)
      }
    ),
    FieldConversionRule(
      "url-helper",
      field => field.fields.isEmpty && field.options.contains("url"),
      field => {
        val protocol = extractStringOrMap(field.options("url"), "protocol").filter(_.nonEmpty).getOrElse("https")
        val baseField = removeOption(field, "url")
        List(FieldBuilder(baseField).url(protocol).field)
      }
    ),
    FieldConversionRule(
      "uuid-pattern-helper",
      field => field.fields.isEmpty && field.options.contains("uuidPattern"),
      field => {
        val baseField = removeOption(field, "uuidPattern")
        List(FieldBuilder(baseField).uuidPattern().field)
      }
    ),
    FieldConversionRule(
      "ipv4-helper",
      field => field.fields.isEmpty && field.options.contains("ipv4"),
      field => {
        val baseField = removeOption(field, "ipv4")
        List(FieldBuilder(baseField).ipv4().field)
      }
    ),
    FieldConversionRule(
      "ipv6-helper",
      field => field.fields.isEmpty && field.options.contains("ipv6"),
      field => {
        val baseField = removeOption(field, "ipv6")
        List(FieldBuilder(baseField).ipv6().field)
      }
    ),
    FieldConversionRule(
      "ssn-helper",
      field => field.fields.isEmpty && field.options.contains("ssnPattern"),
      field => {
        val baseField = removeOption(field, "ssnPattern")
        List(FieldBuilder(baseField).ssnPattern().field)
      }
    ),
    FieldConversionRule(
      "credit-card-helper",
      field => field.fields.isEmpty && field.options.contains("creditCard"),
      field => {
        val cardType = extractStringOrMap(field.options("creditCard"), "cardType").filter(_.nonEmpty).getOrElse("visa")
        val baseField = removeOption(field, "creditCard")
        List(FieldBuilder(baseField).creditCard(cardType).field)
      }
    ),
    FieldConversionRule(
      "within-days-helper",
      field => field.fields.isEmpty && field.options.contains("withinDays"),
      field => {
        val days = extractIntOrMap(field.options("withinDays"), "days").getOrElse(30)
        val baseField = removeOption(field, "withinDays")
        List(FieldBuilder(baseField).withinDays(days).field)
      }
    ),
    FieldConversionRule(
      "future-days-helper",
      field => field.fields.isEmpty && field.options.contains("futureDays"),
      field => {
        val days = extractIntOrMap(field.options("futureDays"), "days").getOrElse(90)
        val baseField = removeOption(field, "futureDays")
        List(FieldBuilder(baseField).futureDays(days).field)
      }
    ),
    FieldConversionRule(
      "exclude-weekends-helper",
      field => field.fields.isEmpty && field.options.contains("excludeWeekends"),
      field => {
        val enabled = extractBoolean(field.options("excludeWeekends")).getOrElse(true)
        val baseField = removeOption(field, "excludeWeekends")
        if (enabled) List(FieldBuilder(baseField).excludeWeekends().field) else List(baseField)
      }
    ),
    FieldConversionRule(
      "business-hours-helper",
      field => field.fields.isEmpty && field.options.contains("businessHours"),
      field => {
        val (startHour, endHour) = extractHourRange(field.options("businessHours"))
        val baseField = removeOption(field, "businessHours")
        List(FieldBuilder(baseField).businessHours(startHour, endHour).field)
      }
    ),
    FieldConversionRule(
      "time-between-helper",
      field => field.fields.isEmpty && field.options.contains("timeBetween"),
      field => {
        val (start, end) = extractTimeRange(field.options("timeBetween"))
        val baseField = removeOption(field, "timeBetween")
        List(FieldBuilder(baseField).timeBetween(start, end).field)
      }
    ),
    FieldConversionRule(
      "sequence-helper",
      field => field.fields.isEmpty && field.options.contains("sequence"),
      field => {
        val sequenceValue = field.options("sequence")
        val sequenceConfig = extractMap(sequenceValue).getOrElse(Map())
        val startCandidate = if (sequenceConfig.nonEmpty) sequenceConfig.getOrElse("start", 1L) else sequenceValue
        val start = extractLong(startCandidate).getOrElse(1L)
        val step = extractLong(sequenceConfig.getOrElse("step", 1L)).getOrElse(1L)
        val prefix = extractString(sequenceConfig.getOrElse("prefix", "")).getOrElse("")
        val padding = extractInt(sequenceConfig.getOrElse("padding", 0)).getOrElse(0)
        val suffix = extractString(sequenceConfig.getOrElse("suffix", "")).getOrElse("")
        val baseField = removeOption(field, "sequence")
        List(FieldBuilder(baseField).sequence(start, step, prefix, padding, suffix).field)
      }
    ),
    FieldConversionRule(
      "daily-batch-sequence-helper",
      field => field.fields.isEmpty && field.options.contains("dailyBatchSequence"),
      field => {
        val config = extractMap(field.options("dailyBatchSequence")).getOrElse(Map())
        val prefix = extractString(config.getOrElse("prefix", "BATCH-")).getOrElse("BATCH-")
        val dateFormat = extractString(config.getOrElse("dateFormat", "yyyyMMdd")).getOrElse("yyyyMMdd")
        val baseField = removeOption(field, "dailyBatchSequence")
        List(FieldBuilder(baseField).dailyBatchSequence(prefix, dateFormat).field)
      }
    ),
    FieldConversionRule(
      "semantic-version-helper",
      field => field.fields.isEmpty && field.options.contains("semanticVersion"),
      field => {
        val config = extractMap(field.options("semanticVersion")).getOrElse(Map())
        val major = extractInt(config.getOrElse("major", 1)).getOrElse(1)
        val minor = extractInt(config.getOrElse("minor", 0)).getOrElse(0)
        val patchIncrement = extractBoolean(config.getOrElse("patchIncrement", true)).getOrElse(true)
        val baseField = removeOption(field, "semanticVersion")
        List(FieldBuilder(baseField).semanticVersion(major, minor, patchIncrement).field)
      }
    ),
    FieldConversionRule(
      "conditional-value-helper",
      field => field.fields.isEmpty && field.options.contains("conditionalValue"),
      field => {
        val conditionalConfig = extractMap(field.options("conditionalValue")).getOrElse(Map())
        val cases = extractCases(conditionalConfig.getOrElse("cases", List()))
        val elseValue = conditionalConfig.getOrElse("else", conditionalConfig.getOrElse("elseValue", "NULL"))
        val sql = buildConditionalSql(cases, elseValue)
        val baseField = removeOption(field, "conditionalValue")
        List(FieldBuilder(baseField).sql(sql).field)
      }
    ),
    FieldConversionRule(
      "mapping-helper",
      field => field.fields.isEmpty && field.options.contains("mapping"),
      field => {
        val mappingConfig = extractMap(field.options("mapping")).getOrElse(Map())
        val sourceField = extractString(mappingConfig.getOrElse("sourceField", "")).getOrElse("")
        val mappings = extractMap(mappingConfig.getOrElse("mappings", Map())).getOrElse(Map())
        val defaultValue = mappingConfig.getOrElse("defaultValue", "NULL")
        val sql = buildMappingSql(sourceField, mappings, defaultValue)
        val baseField = removeOption(field, "mapping")
        List(FieldBuilder(baseField).sql(sql).field)
      }
    ),
    FieldConversionRule(
      "one-of-weighted",
      field => field.fields.isEmpty && field.options.contains(ONE_OF_GENERATOR),
      field => {
        val baseArray = field.options(ONE_OF_GENERATOR) match {
          case s: String => s.split(",").map(_.trim).toList
          case l: List[_] => l.map(_.toString)
          case other => List(other.toString)
        }
        if (OneOfDataGenerator.isWeightedOneOf(baseArray.toArray)) {
          val valuesWithWeights = baseArray.map(value => {
            val split = value.split("->")
            (split(0), split(1).toDouble)
          })
          FieldBuilder().name(field.name).oneOfWeighted(valuesWithWeights).map(_.field)
        } else {
          List(field)
        }
      }
    ),
    FieldConversionRule(
      "uuid-incremental",
      field => field.fields.isEmpty && field.options.contains(UUID) && field.options.contains(INCREMENTAL),
      field => {
        val incrementalStartNumber = field.options(INCREMENTAL).toString.toLong
        List(FieldBuilder().name(field.name).uuid().incremental(incrementalStartNumber).options(field.options).field)
      }
    ),
    FieldConversionRule(
      "uuid",
      field => field.fields.isEmpty && field.options.contains(UUID),
      field => {
        val uuidFieldName = field.options(UUID).toString
        if (uuidFieldName.nonEmpty) List(FieldBuilder().name(field.name).uuid(uuidFieldName).options(field.options).field)
        else List(FieldBuilder().name(field.name).uuid().options(field.options).field)
      }
    ),
    FieldConversionRule(
      "incremental",
      field => field.fields.isEmpty && field.options.contains(INCREMENTAL),
      field => {
        val incrementalStartNumber = field.options(INCREMENTAL).toString.toLong
        List(FieldBuilder().name(field.name).`type`(IntegerType).incremental(incrementalStartNumber).options(field.options).field)
      }
    )
  )

  def applyTaskConversions(task: Task, includeFieldConversions: Boolean = true): Task = {
    val conversions = if (includeFieldConversions) taskConversions else List(convertTaskNumbersToString _)
    conversions.foldLeft(task)((current, conversion) => conversion(current))
  }

  def applyFieldConversions(task: Task): Task = {
    val specificFields = task.steps.map(step => {
      val mappedFields = step.fields.flatMap(convertField)
      step.copy(fields = mappedFields)
    })
    task.copy(steps = specificFields)
  }

  private def convertField(field: Field): List[Field] = {
    fieldConversionRules.foldLeft(List(field)) { (fields, rule) =>
      fields.flatMap(current =>
        if (rule.isApplicable(current)) rule.convert(current) else List(current)
      )
    }
  }

  private def convertTaskNumbersToString(task: Task): Task = {
    val stringSteps = task.steps.map(step => {
      val countPerColGenerator = step.count.perField.map(perFieldCount => {
        val stringOpts = toStringValues(perFieldCount.options)
        perFieldCount.copy(options = stringOpts)
      })
      val countStringOpts = toStringValues(step.count.options)
      val mappedSchema = step.fields.map(_.fieldToStringOptions)
      step.copy(
        count = step.count.copy(perField = countPerColGenerator, options = countStringOpts),
        fields = mappedSchema
      )
    })
    task.copy(steps = stringSteps)
  }

  private def toStringValues(options: Map[String, Any]): Map[String, String] = {
    options.map(x => {
      x._2 match {
        case list: List[_] => (x._1, list.mkString(","))
        case _ => (x._1, x._2.toString)
      }
    })
  }

  private def removeOption(field: Field, key: String): Field =
    field.copy(options = field.options - key)

  private def extractMap(value: Any): Option[Map[String, Any]] = value match {
    case null => None
    case m: Map[_, _] => Some(m.asInstanceOf[Map[String, Any]])
    case m: java.util.Map[_, _] => Some(m.asScala.toMap.asInstanceOf[Map[String, Any]])
    case _ => None
  }

  private def extractString(value: Any): Option[String] = value match {
    case null => None
    case s: String => Some(s)
    case s: CharSequence => Some(s.toString)
    case _ => None
  }

  private def extractStringOrMap(value: Any, key: String): Option[String] =
    extractMap(value).flatMap(map => extractString(map.getOrElse(key, ""))).orElse(extractString(value))

  private def extractBoolean(value: Any): Option[Boolean] = value match {
    case null => None
    case b: Boolean => Some(b)
    case s: String => scala.util.Try(s.toBoolean).toOption
    case n: java.lang.Number => Some(n.intValue() != 0)
    case _ => None
  }

  private def extractInt(value: Any): Option[Int] = value match {
    case null => None
    case i: Int => Some(i)
    case l: Long => Some(l.toInt)
    case d: Double => Some(d.toInt)
    case f: Float => Some(f.toInt)
    case n: java.lang.Number => Some(n.intValue())
    case s: String => scala.util.Try(s.toInt).toOption
    case _ => None
  }

  private def extractLong(value: Any): Option[Long] = value match {
    case null => None
    case i: Int => Some(i.toLong)
    case l: Long => Some(l)
    case d: Double => Some(d.toLong)
    case f: Float => Some(f.toLong)
    case n: java.lang.Number => Some(n.longValue())
    case s: String => scala.util.Try(s.toLong).toOption
    case _ => None
  }

  private def extractDouble(value: Any): Option[Double] = value match {
    case null => None
    case d: Double => Some(d)
    case f: Float => Some(f.toDouble)
    case i: Int => Some(i.toDouble)
    case l: Long => Some(l.toDouble)
    case n: java.lang.Number => Some(n.doubleValue())
    case s: String => scala.util.Try(s.toDouble).toOption
    case _ => None
  }

  private def extractIntOrMap(value: Any, key: String): Option[Int] =
    extractMap(value).flatMap(map => extractInt(map.getOrElse(key, null))).orElse(extractInt(value))

  private def extractHourRange(value: Any): (Int, Int) = {
    val defaults = (9, 17)
    val config = extractMap(value).getOrElse(Map())
    val start = extractInt(config.getOrElse("startHour", defaults._1)).getOrElse(defaults._1)
    val end = extractInt(config.getOrElse("endHour", defaults._2)).getOrElse(defaults._2)
    (start, end)
  }

  private def extractTimeRange(value: Any): (String, String) = {
    extractMap(value) match {
      case Some(config) =>
        val start = extractString(config.getOrElse("start", "09:00")).getOrElse("09:00")
        val end = extractString(config.getOrElse("end", "17:00")).getOrElse("17:00")
        (start, end)
      case None =>
        value match {
          case list: List[_] if list.size >= 2 =>
            (list.head.toString, list(1).toString)
          case list: java.util.List[_] if list.size >= 2 =>
            (list.get(0).toString, list.get(1).toString)
          case s: String if s.contains("-") =>
            val split = s.split("-", 2).map(_.trim)
            (split(0), split(1))
          case _ =>
            ("09:00", "17:00")
        }
    }
  }

  private def extractCases(value: Any): List[Map[String, Any]] = value match {
    case list: List[_] => list.flatMap(item => extractMap(item))
    case list: java.util.List[_] => list.asScala.toList.flatMap(item => extractMap(item))
    case _ => List()
  }

  private def toSqlLiteral(value: Any): String = value match {
    case null => "NULL"
    case s: String if s == "NULL" => "NULL"
    case s: String => s"'${s.replace("'", "''")}'"
    case b: Boolean => b.toString
    case other => other.toString
  }

  private def buildConditionalSql(cases: List[Map[String, Any]], elseValue: Any): String = {
    require(cases.nonEmpty, "conditionalValue requires at least one case")
    val caseSql = cases.map { entry =>
      val condition = extractString(entry.getOrElse("when", "")).getOrElse("")
      val thenValue = entry.getOrElse("then", "NULL")
      s"WHEN $condition THEN ${toSqlLiteral(thenValue)}"
    }.mkString(" ")
    val elseSql = toSqlLiteral(elseValue)
    s"CASE $caseSql ELSE $elseSql END"
  }

  private def buildMappingSql(sourceField: String, mappings: Map[String, Any], defaultValue: Any): String = {
    require(sourceField.nonEmpty, "mapping requires sourceField")
    require(mappings.nonEmpty, "mapping requires mappings")
    val caseSql = mappings.map { case (sourceValue, targetValue) =>
      val condition = s"$sourceField = ${toSqlLiteral(sourceValue)}"
      s"WHEN $condition THEN ${toSqlLiteral(targetValue)}"
    }.mkString(" ")
    val elseSql = toSqlLiteral(defaultValue)
    s"CASE $caseSql ELSE $elseSql END"
  }
}
