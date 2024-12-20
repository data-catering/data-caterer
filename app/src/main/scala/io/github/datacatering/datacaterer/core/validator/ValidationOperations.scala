package io.github.datacatering.datacaterer.core.validator

import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_COUNT, FORMAT, VALIDATION_COLUMN_NAME_COUNT_BETWEEN, VALIDATION_COLUMN_NAME_COUNT_EQUAL, VALIDATION_COLUMN_NAME_MATCH_ORDER, VALIDATION_COLUMN_NAME_MATCH_SET, VALIDATION_PREFIX_JOIN_EXPRESSION, VALIDATION_UNIQUE}
import io.github.datacatering.datacaterer.api.model.{ColumnNamesValidation, ExpressionValidation, GroupByValidation, UpstreamDataSourceValidation, Validation}
import io.github.datacatering.datacaterer.core.exception.UnsupportedDataValidationTypeException
import io.github.datacatering.datacaterer.core.model.ValidationResult
import io.github.datacatering.datacaterer.core.validator.ValidationHelper.getValidationType
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}

abstract class ValidationOps(validation: Validation) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def validate(df: DataFrame, dfCount: Long): ValidationResult

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

  def getIsSuccessAndSampleErrors(notEqualDf: Dataset[Row], dfCount: Long): (Boolean, Option[DataFrame], Long) = {
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
      case exprValid: ExpressionValidation => new ExpressionValidationOps(exprValid)
      case grpValid: GroupByValidation => new GroupByValidationOps(grpValid)
      case upValid: UpstreamDataSourceValidation => new UpstreamDataSourceValidationOps(upValid, recordTrackingForValidationFolderPath)
      case colNames: ColumnNamesValidation => new ColumnNamesValidationOps(colNames)
      case x => throw UnsupportedDataValidationTypeException(x.getClass.getName)
    }
  }
}

class ExpressionValidationOps(expressionValidation: ExpressionValidation) extends ValidationOps(expressionValidation) {
  override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
    //TODO allow for pre-filter? can technically be done via custom sql validation using CASE WHERE ... ELSE true END
    val dfWithSelectExpr = df.selectExpr(expressionValidation.selectExpr: _*)
    validateWithExpression(dfWithSelectExpr, dfCount, expressionValidation.expr)
  }
}

class GroupByValidationOps(groupByValidation: GroupByValidation) extends ValidationOps(groupByValidation) {
  override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
    //TODO allow for pre and post group filter?
    val groupByDf = df.groupBy(groupByValidation.groupByCols.map(col): _*)
    val (aggregateDf, validationCount) = if ((groupByValidation.aggCol == VALIDATION_UNIQUE || groupByValidation.aggCol.isEmpty) && groupByValidation.aggType == AGGREGATION_COUNT) {
      val countDf = groupByDf.count()
      (countDf, Math.max(1L, countDf.count()))
    } else {
      val aggDf = groupByDf.agg(Map(
        groupByValidation.aggCol -> groupByValidation.aggType
      ))
      (aggDf, aggDf.count())
    }
    validateWithExpression(aggregateDf, validationCount, groupByValidation.aggExpr)
  }
}

class UpstreamDataSourceValidationOps(
                                       upstreamDataSourceValidation: UpstreamDataSourceValidation,
                                       recordTrackingForValidationFolderPath: String
                                     ) extends ValidationOps(upstreamDataSourceValidation) {
  override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
    val upstreamDf = getUpstreamData(df.sparkSession)
    val joinedDf = getJoinedDf(df, upstreamDf)
    val joinedCount = joinedDf.count()

    val baseValidationOp = getValidationType(upstreamDataSourceValidation.validation.validation, recordTrackingForValidationFolderPath)
    val result = baseValidationOp.validate(joinedDf, joinedCount)
    ValidationResult.fromValidationWithBaseResult(upstreamDataSourceValidation, result)
  }

  private def getJoinedDf(df: DataFrame, upstreamDf: DataFrame): DataFrame = {
    val joinCols = upstreamDataSourceValidation.joinColumns
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


class ColumnNamesValidationOps(columnNamesValidation: ColumnNamesValidation) extends ValidationOps(columnNamesValidation) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
    implicit val stringEncoder: Encoder[CustomErrorSample] = Encoders.kryo[CustomErrorSample]

    val (isSuccess, errorSamples, total) = columnNamesValidation.columnNameType match {
      case VALIDATION_COLUMN_NAME_COUNT_EQUAL =>
        val isEqualLength = df.columns.length == columnNamesValidation.count
        val sample = if (isEqualLength) List() else List(CustomErrorSample(df.columns.length.toString))
        (isEqualLength, sample, 1)
      case VALIDATION_COLUMN_NAME_COUNT_BETWEEN =>
        val colLength = df.columns.length
        val isBetween = colLength >= columnNamesValidation.minCount && colLength <= columnNamesValidation.maxCount
        val sample = if (isBetween) List() else List(CustomErrorSample(df.columns.length.toString))
        (isBetween, sample, 1)
      case VALIDATION_COLUMN_NAME_MATCH_ORDER =>
        val zippedNames = df.columns.zip(columnNamesValidation.names).zipWithIndex
        val misalignedNames = zippedNames.filter(n => n._1._1 != n._1._2)
        (misalignedNames.isEmpty, misalignedNames.map(n => CustomErrorSample(s"${n._2}: ${n._1._1} -> ${n._1._2}")).toList, zippedNames.length)
      case VALIDATION_COLUMN_NAME_MATCH_SET =>
        val missingNames = columnNamesValidation.names.filter(n => !df.columns.contains(n)).map(CustomErrorSample)
        (missingNames.isEmpty, missingNames.toList, columnNamesValidation.names.length)
      case x =>
        LOGGER.error(s"Unknown field name validation type, returning as a failed validation, type=$x")
        (false, List(), 1)
    }

    val optErrorSample = if (isSuccess) {
      None
    } else {
      Some(df.sparkSession.createDataFrame(errorSamples))
    }
    ValidationResult(columnNamesValidation, isSuccess, errorSamples.size, total, optErrorSample)
  }
}

case class CustomErrorSample(value: String)
