package io.github.datacatering.datacaterer.core.validator

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_COUNT, FORMAT, VALIDATION_FIELD_NAME_COUNT_BETWEEN, VALIDATION_FIELD_NAME_COUNT_EQUAL, VALIDATION_FIELD_NAME_MATCH_ORDER, VALIDATION_FIELD_NAME_MATCH_SET, VALIDATION_PREFIX_JOIN_EXPRESSION, VALIDATION_UNIQUE}
import io.github.datacatering.datacaterer.api.model.{BetweenFieldValidation, ContainsFieldValidation, DistinctContainsSetFieldValidation, DistinctEqualFieldValidation, DistinctInSetFieldValidation, EndsWithFieldValidation, EqualFieldValidation, ExpressionValidation, FieldNamesValidation, FieldValidations, GreaterThanFieldValidation, GreaterThanSizeFieldValidation, GroupByValidation, HasTypeFieldValidation, HasTypesFieldValidation, InFieldValidation, IsDecreasingFieldValidation, IsIncreasingFieldValidation, IsJsonParsableFieldValidation, LengthBetweenFieldValidation, LengthEqualFieldValidation, LessThanFieldValidation, LessThanSizeFieldValidation, LuhnCheckFieldValidation, MatchDateTimeFormatFieldValidation, MatchJsonSchemaFieldValidation, MatchesFieldValidation, MaxBetweenFieldValidation, MeanBetweenFieldValidation, MedianBetweenFieldValidation, MinBetweenFieldValidation, MostCommonValueInSetFieldValidation, NullFieldValidation, QuantileValuesBetweenFieldValidation, SizeFieldValidation, StartsWithFieldValidation, StdDevBetweenFieldValidation, SumBetweenFieldValidation, UniqueFieldValidation, UniqueValuesProportionBetweenFieldValidation, UpstreamDataSourceValidation, Validation, YamlUpstreamDataSourceValidation}
import io.github.datacatering.datacaterer.core.exception.UnsupportedDataValidationTypeException
import io.github.datacatering.datacaterer.core.model.ValidationResult
import io.github.datacatering.datacaterer.core.validator.ValidationHelper.getValidationType
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}

abstract class ValidationOps(validation: Validation) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def validate(df: DataFrame, dfCount: Long): List[ValidationResult]

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

  def validateWithExpression(df: DataFrame, dfCount: Long, expression: String): ValidationResult = {
    val notEqualDf = df.where(s"!($expression)")
    val (isSuccess, sampleErrors, numErrors) = getIsSuccessAndSampleErrors(notEqualDf, dfCount)
    ValidationResult(validation, isSuccess, numErrors, dfCount, sampleErrors)
  }

  private def getIsSuccessAndSampleErrors(notEqualDf: Dataset[Row], dfCount: Long): (Boolean, Option[DataFrame], Long) = {
    val numErrors = notEqualDf.count()
    val (isSuccess, sampleErrors) = (numErrors, validation.errorThreshold) match {
      case (c, Some(threshold)) if c > 0 =>
        if ((threshold >= 1 && c > threshold) || (threshold < 1 && c.toDouble / dfCount > threshold)) {
          (false, Some(notEqualDf))
        } else (true, None)
      case (c, None) if c > 0 => (false, Some(notEqualDf))
      case _ => (true, None)
    }
    (isSuccess, sampleErrors, numErrors)
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
  override def validate(df: DataFrame, dfCount: Long): List[ValidationResult] = {
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

      validationWithPreFilter.validation match {
        case e: ExpressionValidation => new ExpressionValidationOps(e).validate(df, dfCount)
        case g: GroupByValidation => new GroupByValidationOps(g).validate(df, dfCount)
      }
    })
  }
}

class ExpressionValidationOps(expressionValidation: ExpressionValidation) extends ValidationOps(expressionValidation) {
  override def validate(df: DataFrame, dfCount: Long): List[ValidationResult] = {
    //TODO allow for pre-filter? can technically be done via custom sql validation using CASE WHERE ... ELSE true END
    val dfWithSelectExpr = df.selectExpr(expressionValidation.selectExpr: _*)
    List(validateWithExpression(dfWithSelectExpr, dfCount, expressionValidation.expr))
  }
}

class GroupByValidationOps(groupByValidation: GroupByValidation) extends ValidationOps(groupByValidation) {
  override def validate(df: DataFrame, dfCount: Long): List[ValidationResult] = {
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
    List(validateWithExpression(aggregateDf, validationCount, groupByValidation.aggExpr))
  }
}

class UpstreamDataSourceValidationOps(
                                       upstreamDataSourceValidation: UpstreamDataSourceValidation,
                                       recordTrackingForValidationFolderPath: String
                                     ) extends ValidationOps(upstreamDataSourceValidation) {
  override def validate(df: DataFrame, dfCount: Long): List[ValidationResult] = {
    val upstreamDf = getUpstreamData(df.sparkSession)
    val joinedDf = getJoinedDf(df, upstreamDf)
    val joinedCount = joinedDf.count()

    upstreamDataSourceValidation.validations.flatMap(v => {
      val baseValidationOp = getValidationType(v.validation, recordTrackingForValidationFolderPath)
      val result = baseValidationOp.validate(joinedDf, joinedCount)
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

  override def validate(df: DataFrame, dfCount: Long): List[ValidationResult] = {
    implicit val stringEncoder: Encoder[CustomErrorSample] = Encoders.kryo[CustomErrorSample]

    val (isSuccess, errorSamples, total) = fieldNamesValidation.fieldNameType match {
      case VALIDATION_FIELD_NAME_COUNT_EQUAL =>
        val isEqualLength = df.columns.length == fieldNamesValidation.count
        val sample = if (isEqualLength) List() else List(CustomErrorSample(df.columns.length.toString))
        (isEqualLength, sample, 1)
      case VALIDATION_FIELD_NAME_COUNT_BETWEEN =>
        val colLength = df.columns.length
        val isBetween = colLength >= fieldNamesValidation.min && colLength <= fieldNamesValidation.max
        val sample = if (isBetween) List() else List(CustomErrorSample(colLength.toString))
        (isBetween, sample, 1)
      case VALIDATION_FIELD_NAME_MATCH_ORDER =>
        val zippedNames = df.columns.zip(fieldNamesValidation.names).zipWithIndex
        val misalignedNames = zippedNames.filter(n => n._1._1 != n._1._2)
        (misalignedNames.isEmpty, misalignedNames.map(n => CustomErrorSample(s"${n._2}: ${n._1._1} -> ${n._1._2}")).toList, zippedNames.length)
      case VALIDATION_FIELD_NAME_MATCH_SET =>
        val missingNames = fieldNamesValidation.names.filter(n => !df.columns.contains(n)).map(CustomErrorSample)
        (missingNames.isEmpty, missingNames.toList, fieldNamesValidation.names.length)
      case x =>
        LOGGER.error(s"Unknown field name validation type, returning as a failed validation, type=$x")
        (false, List(), 1)
    }

    val optErrorSample = if (isSuccess) {
      None
    } else {
      Some(df.sparkSession.createDataFrame(errorSamples))
    }
    List(ValidationResult(fieldNamesValidation, isSuccess, errorSamples.size, total, optErrorSample))
  }
}

case class CustomErrorSample(value: String)
