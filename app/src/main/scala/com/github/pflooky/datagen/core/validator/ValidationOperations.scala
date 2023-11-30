package com.github.pflooky.datagen.core.validator

import com.github.pflooky.datacaterer.api.model.Constants.{AGGREGATION_COUNT, FORMAT, VALIDATION_COLUMN_NAME_COUNT_BETWEEN, VALIDATION_COLUMN_NAME_COUNT_EQUAL, VALIDATION_COLUMN_NAME_MATCH_ORDER, VALIDATION_COLUMN_NAME_MATCH_SET, VALIDATION_PREFIX_JOIN_EXPRESSION, VALIDATION_UNIQUE}
import com.github.pflooky.datacaterer.api.model.{ColumnNamesValidation, ExpressionValidation, GroupByValidation, UpstreamDataSourceValidation, Validation}
import com.github.pflooky.datagen.core.model.ValidationResult
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}

abstract class ValidationOps(validation: Validation) {
  def validate(df: DataFrame, dfCount: Long): ValidationResult

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

class ExpressionValidationOps(expressionValidation: ExpressionValidation) extends ValidationOps(expressionValidation) {
  override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
    validateWithExpression(df, dfCount, expressionValidation.expr)
  }
}

class GroupByValidationOps(groupByValidation: GroupByValidation) extends ValidationOps(groupByValidation) {
  override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
    val groupByDf = df.groupBy(groupByValidation.groupByCols.map(col): _*)
    val (aggregateDf, validationCount) = if ((groupByValidation.aggCol == VALIDATION_UNIQUE || groupByValidation.aggCol.isEmpty) && groupByValidation.aggType == AGGREGATION_COUNT) {
      (groupByDf.count(), 1L)
    } else {
      val aggDf = groupByDf.agg(Map(
        groupByValidation.aggCol -> groupByValidation.aggType
      ))
      (aggDf, aggDf.count())
    }
    validateWithExpression(aggregateDf, validationCount, groupByValidation.expr)
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

    val baseValidationOp = upstreamDataSourceValidation.validationBuilder.validation match {
      case expr: ExpressionValidation => new ExpressionValidationOps(expr)
      case grp: GroupByValidation => new GroupByValidationOps(grp)
      case up: UpstreamDataSourceValidation => new UpstreamDataSourceValidationOps(up, recordTrackingForValidationFolderPath)
      case colNames: ColumnNamesValidation => new ColumnNamesValidationOps(colNames)
      case x => throw new RuntimeException(s"Unsupported validation type, validation=$x")
    }
    val result = baseValidationOp.validate(joinedDf, joinedCount)
    ValidationResult.fromValidationWithBaseResult(upstreamDataSourceValidation, result)
  }

  private def getJoinedDf(df: DataFrame, upstreamDf: DataFrame): DataFrame = {
    val joinCols = upstreamDataSourceValidation.joinCols
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

    val (isSuccess, errorSamples, total) = columnNamesValidation.`type` match {
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
        LOGGER.error(s"Unknown column name validation type, returning as a failed validation, type=$x")
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
