package io.github.datacatering.datacaterer.core.validator

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_COUNT, FORMAT, VALIDATION_FIELD_NAME_COUNT_BETWEEN, VALIDATION_FIELD_NAME_COUNT_EQUAL, VALIDATION_FIELD_NAME_MATCH_ORDER, VALIDATION_FIELD_NAME_MATCH_SET, VALIDATION_PREFIX_JOIN_EXPRESSION, VALIDATION_UNIQUE}
import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.exception.{FailedFieldDataValidationException, UnsupportedDataValidationTypeException}
import io.github.datacatering.datacaterer.core.validator.ValidationHelper.getValidationType
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

abstract class ValidationOps(validation: Validation) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def validate(df: DataFrame, dfCount: Long, numErrorSamples: Int = 10): List[ValidationResult]

  def filterData(df: DataFrame): DataFrame = {
    validation.preFilter.map(preFilter => {
      val isValidFilter = preFilter.validate()
      if (isValidFilter) {
        val preFilterExpression = preFilter.toExpression
        LOGGER.debug(s"Using pre-filter before running data validation, pre-filter-expression=$preFilterExpression")
        df.where(preFilterExpression)
      } else {
        LOGGER.warn(s"Invalid pre-filter defined for validation, defaulting to using unfiltered dataset")
        df
      }
    }).getOrElse(df)
  }

  def validateWithExpression(df: DataFrame, dfCount: Long, expression: String, numErrorSamples: Int): ValidationResult = {
    val notEqualDf = df.where(s"!($expression)")
    val (isSuccess, sampleErrors, numErrors) = getIsSuccessAndSampleErrors(notEqualDf, dfCount, numErrorSamples)
    ValidationResult(validation, isSuccess, numErrors, dfCount, sampleErrors)
  }

  private def getIsSuccessAndSampleErrors(notEqualDf: Dataset[Row], dfCount: Long, numErrorSamples: Int): (Boolean, Option[Array[Map[String, Any]]], Long) = {
    val numErrors = notEqualDf.count()
    val (isSuccess, sampleErrors) = (numErrors, validation.errorThreshold) match {
      case (c, Some(threshold)) if c > 0 =>
        if ((threshold >= 1 && c > threshold) || (threshold < 1 && c.toDouble / dfCount > threshold)) {
          (false, Some(notEqualDf))
        } else (true, None)
      case (c, None) if c > 0 => (false, Some(notEqualDf))
      case _ => (true, None)
    }
    val errorsAsMap = sampleErrors.map(ds => {
      ds.take(numErrorSamples)
        .map(r => {
          val valuesMap = r.getValuesMap(ds.columns)
          valuesMap.flatMap(parseValueMap)
        })
    })
    (isSuccess, errorsAsMap, numErrors)
  }

  private def parseValueMap[T](valMap: (String, T)): Map[String, Any] = {
    valMap._2 match {
      case genericRow: GenericRowWithSchema =>
        val innerValueMap = genericRow.getValuesMap[T](genericRow.schema.fields.map(_.name))
        Map(valMap._1 -> innerValueMap.flatMap(parseValueMap[T]))
      case wrappedArray: mutable.WrappedArray[_] =>
        Map(valMap._1 -> wrappedArray.map {
          case genericRowWithSchema: GenericRowWithSchema =>
            val innerValueMap = genericRowWithSchema.getValuesMap[T](genericRowWithSchema.schema.fields.map(_.name))
            innerValueMap.flatMap(parseValueMap[T])
          case x => x
        })
      case _ =>
        Map(valMap)
    }
  }
}


object ValidationHelper {

  def getValidationType(validation: Validation, recordTrackingForValidationFolderPath: String): ValidationOps = {
    validation match {
      case fieldValid: FieldValidations => new FieldValidationsOps(fieldValid)
      case exprValid: ExpressionValidation => new ExpressionValidationOps(exprValid)
      case grpValid: GroupByValidation => new GroupByValidationOps(grpValid)
      case upValid: UpstreamDataSourceValidation => new UpstreamDataSourceValidationOps(upValid, recordTrackingForValidationFolderPath)
      case colNames: FieldNamesValidation => new FieldNamesValidationOps(colNames)
      case x => throw UnsupportedDataValidationTypeException(x.getClass.getName)
    }
  }
}

class FieldValidationsOps(fieldValidations: FieldValidations) extends ValidationOps(fieldValidations) {
  override def validate(df: DataFrame, dfCount: Long, numErrorSamples: Int): List[ValidationResult] = {
    val field = fieldValidations.field
    val build = ValidationBuilder().field(field)
    fieldValidations.validation.flatMap(v => {
      val baseValidation = v match {
        case EqualFieldValidation(value, negate) => build.isEqual(value, negate)
        case NullFieldValidation(negate) => build.isNull(negate)
        case ContainsFieldValidation(value, negate) => build.contains(value, negate)
        case UniqueFieldValidation(negate) => ValidationBuilder().unique(field)
        case LessThanFieldValidation(value, strictly) => build.lessThan(value, strictly)
        case GreaterThanFieldValidation(value, strictly) => build.greaterThan(value, strictly)
        case BetweenFieldValidation(min, max, negate) => build.between(min, max, negate)
        case InFieldValidation(values, negate) => build.in(values, negate)
        case MatchesFieldValidation(regex, negate) => build.matches(regex, negate)
        case MatchesListFieldValidation(regexes, matchAll, negate) => build.matchesList(regexes, matchAll, negate)
        case StartsWithFieldValidation(value, negate) => build.startsWith(value, negate)
        case EndsWithFieldValidation(value, negate) => build.endsWith(value, negate)
        case SizeFieldValidation(size, negate) => build.size(size, negate)
        case LessThanSizeFieldValidation(size, strictly) => build.lessThanSize(size, strictly)
        case GreaterThanSizeFieldValidation(size, strictly) => build.greaterThanSize(size, strictly)
        case LuhnCheckFieldValidation(negate) => build.luhnCheck(negate)
        case HasTypeFieldValidation(value, negate) => build.hasType(value, negate)
        case HasTypesFieldValidation(values, negate) => build.hasTypes(values, negate)
        case DistinctInSetFieldValidation(values, negate) => build.distinctInSet(values, negate)
        case DistinctContainsSetFieldValidation(values, negate) => build.distinctContainsSet(values, negate)
        case DistinctEqualFieldValidation(values, negate) => build.distinctEqual(values, negate)
        case MaxBetweenFieldValidation(min, max, negate) => build.maxBetween(min, max, negate)
        case MeanBetweenFieldValidation(min, max, negate) => build.meanBetween(min, max, negate)
        case MedianBetweenFieldValidation(min, max, negate) => build.medianBetween(min, max, negate)
        case MinBetweenFieldValidation(min, max, negate) => build.minBetween(min, max, negate)
        case StdDevBetweenFieldValidation(min, max, negate) => build.stdDevBetween(min, max, negate)
        case SumBetweenFieldValidation(min, max, negate) => build.sumBetween(min, max, negate)
        case LengthBetweenFieldValidation(min, max, negate) => build.lengthBetween(min, max, negate)
        case LengthEqualFieldValidation(value, negate) => build.lengthEqual(value, negate)
        case IsDecreasingFieldValidation(strictly) => build.isDecreasing(strictly)
        case IsIncreasingFieldValidation(strictly) => build.isIncreasing(strictly)
        case IsJsonParsableFieldValidation(negate) => build.isJsonParsable(negate)
        case MatchJsonSchemaFieldValidation(schema, negate) => build.matchJsonSchema(schema, negate)
        case MatchDateTimeFormatFieldValidation(format, negate) => build.matchDateTimeFormat(format, negate)
        case MostCommonValueInSetFieldValidation(values, negate) => build.mostCommonValueInSet(values, negate)
        case UniqueValuesProportionBetweenFieldValidation(min, max, negate) => build.uniqueValuesProportionBetween(min, max, negate)
        case QuantileValuesBetweenFieldValidation(quantileRanges, negate) => build.quantileValuesBetween(quantileRanges, negate)
      }

      val validationWithThreshold = v.errorThreshold.map(e => baseValidation.errorThreshold(e)).getOrElse(baseValidation)
      val validationWithDescription = v.description.map(d => baseValidation.description(d)).getOrElse(validationWithThreshold)
      val validationWithPreFilter = v.preFilter.map(f => validationWithDescription.preFilter(f)).getOrElse(validationWithDescription)

      val tryRunValidation = Try(validationWithPreFilter.validation match {
        case e: ExpressionValidation => new ExpressionValidationOps(e).validate(df, dfCount, numErrorSamples)
        case g: GroupByValidation => new GroupByValidationOps(g).validate(df, dfCount, numErrorSamples)
      })
      tryRunValidation match {
        case Success(value) => value
        case Failure(exception) => throw FailedFieldDataValidationException(field, validationWithPreFilter.validation, exception)
      }
    })
  }
}

class ExpressionValidationOps(expressionValidation: ExpressionValidation) extends ValidationOps(expressionValidation) {
  override def validate(df: DataFrame, dfCount: Long, numErrorSamples: Int): List[ValidationResult] = {
    //TODO allow for pre-filter? can technically be done via custom sql validation using CASE WHERE ... ELSE true END
    val dfWithSelectExpr = df.selectExpr(expressionValidation.selectExpr: _*)
    List(validateWithExpression(dfWithSelectExpr, dfCount, expressionValidation.expr, numErrorSamples))
  }
}

class GroupByValidationOps(groupByValidation: GroupByValidation) extends ValidationOps(groupByValidation) {
  override def validate(df: DataFrame, dfCount: Long, numErrorSamples: Int): List[ValidationResult] = {
    //TODO allow for pre and post group filter?
    val groupByDf = df.groupBy(groupByValidation.groupByFields.map(col): _*)
    val (aggregateDf, validationCount) = if ((groupByValidation.aggField == VALIDATION_UNIQUE || groupByValidation.aggField.isEmpty) && groupByValidation.aggType == AGGREGATION_COUNT) {
      val countDf = groupByDf.count()
      (countDf, Math.max(1L, countDf.count()))
    } else {
      val aggDf = groupByDf.agg(Map(
        groupByValidation.aggField -> groupByValidation.aggType
      ))
      (aggDf, aggDf.count())
    }
    List(validateWithExpression(aggregateDf, validationCount, groupByValidation.aggExpr, numErrorSamples))
  }
}

class UpstreamDataSourceValidationOps(
                                       upstreamDataSourceValidation: UpstreamDataSourceValidation,
                                       recordTrackingForValidationFolderPath: String
                                     ) extends ValidationOps(upstreamDataSourceValidation) {
  override def validate(df: DataFrame, dfCount: Long, numErrorSamples: Int): List[ValidationResult] = {
    val upstreamDf = getUpstreamData(df.sparkSession)
    val joinedDf = getJoinedDf(df, upstreamDf)
    val joinedCount = joinedDf.count()

    upstreamDataSourceValidation.validations.flatMap(v => {
      val baseValidationOp = getValidationType(v.validation, recordTrackingForValidationFolderPath)
      val result = baseValidationOp.validate(joinedDf, joinedCount, numErrorSamples)
      result.map(r => ValidationResult.fromValidationWithBaseResult(upstreamDataSourceValidation, r))
    })
  }

  private def getJoinedDf(df: DataFrame, upstreamDf: DataFrame): DataFrame = {
    val joinCols = upstreamDataSourceValidation.joinFields
    val joinType = upstreamDataSourceValidation.joinType
    val upstreamName = upstreamDataSourceValidation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName

    val upstreamColsToRename = upstreamDf.columns.filter(c => !joinCols.contains(c))
      .map(c => c -> s"${upstreamName}_$c").toMap
    val renamedUpstreamDf = upstreamDf.withColumnsRenamed(upstreamColsToRename)

    val joinedDf = if (joinCols.size == 1 && joinCols.head.startsWith(VALIDATION_PREFIX_JOIN_EXPRESSION)) {
      df.join(renamedUpstreamDf, expr(joinCols.head.replaceFirst(VALIDATION_PREFIX_JOIN_EXPRESSION, "")), joinType)
    } else {
      df.join(renamedUpstreamDf, joinCols, joinType)
    }
    if (!joinedDf.storageLevel.useMemory) joinedDf.cache()
    joinedDf
  }

  private def getUpstreamData(sparkSession: SparkSession): DataFrame = {
    val upstreamConnectionOptions = upstreamDataSourceValidation.upstreamDataSource.connectionConfigWithTaskBuilder.options ++
      upstreamDataSourceValidation.upstreamReadOptions
    val upstreamFormat = upstreamConnectionOptions(FORMAT)
    sparkSession.read
      .format(upstreamFormat)
      .options(upstreamConnectionOptions)
      .load()
  }
}


class FieldNamesValidationOps(fieldNamesValidation: FieldNamesValidation) extends ValidationOps(fieldNamesValidation) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override def validate(df: DataFrame, dfCount: Long, numErrorSamples: Int): List[ValidationResult] = {
    val (isSuccess, errorSamples, total) = fieldNamesValidation.fieldNameType match {
      case VALIDATION_FIELD_NAME_COUNT_EQUAL =>
        val isEqualLength = df.columns.length == fieldNamesValidation.count
        val sample = if (isEqualLength) Array[Map[String, Any]]() else Array(Map[String, Any]("columnLength" -> df.columns.length.toString))
        (isEqualLength, sample, 1)
      case VALIDATION_FIELD_NAME_COUNT_BETWEEN =>
        val colLength = df.columns.length
        val isBetween = colLength >= fieldNamesValidation.min && colLength <= fieldNamesValidation.max
        val sample = if (isBetween) Array[Map[String, Any]]() else Array(Map[String, Any]("columnLength" -> colLength.toString))
        (isBetween, sample, 1)
      case VALIDATION_FIELD_NAME_MATCH_ORDER =>
        val zippedNames = df.columns.zip(fieldNamesValidation.names).zipWithIndex
        val misalignedNames = zippedNames.filter(n => n._1._1 != n._1._2)
        val errorSample = misalignedNames.map(n => Map[String, Any](s"field_index_${n._2}" -> s"${n._1._1} -> ${n._1._2}"))
        (misalignedNames.isEmpty, errorSample, zippedNames.length)
      case VALIDATION_FIELD_NAME_MATCH_SET =>
        val missingNames = fieldNamesValidation.names.filter(n => !df.columns.contains(n))
          .map(n => Map("missing_field" -> n))
          .asInstanceOf[Array[Map[String, Any]]]
        (missingNames.isEmpty, missingNames, fieldNamesValidation.names.length)
      case x =>
        LOGGER.error(s"Unknown field name validation type, returning as a failed validation, type=$x")
        (false, Array[Map[String, Any]](), 1)
    }

    val optErrorSample = if (isSuccess) None else Some(errorSamples)
    List(ValidationResult(fieldNamesValidation, isSuccess, errorSamples.length, total, optErrorSample))
  }
}

case class CustomErrorSample(value: String)
