package io.github.datacatering.datacaterer.core.generator.metadata.validation

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.VALIDATION_GROUP_BY
import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, GroupByValidation}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.DataSourceMetadata
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructField

/**
 * Based on a schema, provide suggestions on what validations should be run against the data set.
 * Mostly based on data type and labels (labels can come from {{{ExpressionPredictor.getFakerExpressionAndLabel}}}).
 * Can take into account other fields within the same data source. For example,
 * `open_date` and `close_date` fields defined, should create an expression validation checking if
 * {{{DATE(open_date) < DATE(close_date)}}}. Can check for date/timestamp fields that have start -> end, open -> close,
 * open -> updated,
 *
 * Later on, should be able to take into account other data sources and create relationship validations based on
 * relationship predictor.
 */
object ValidationPredictor {

  private val LOGGER = Logger.getLogger(getClass.getName)

  private val VALIDATION_PREDICTION_CHECKS: List[ValidationPredictionCheck] =
    List(
      new DateTimestampValidationPredictionCheck(),
      new PrimaryKeyValidationPredictionCheck(),
      new ExpressionValidationPredictionCheck()
    )

  def suggestValidations(dataSourceMetadata: DataSourceMetadata, dataSourceReadOptions: Map[String, String],
                         fields: Array[StructField]): List[ValidationBuilder] = {
    val baseLogInfo = s"data-source-name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}, details=$dataSourceReadOptions"
    LOGGER.info(s"Getting suggested validations based on metadata, $baseLogInfo")
    val validations = VALIDATION_PREDICTION_CHECKS.flatMap(_.check(fields))
    validations.foreach(validation => {
      val validationLogInfo = validation.validation match {
        case ExpressionValidation(expr, selectExpr) => s"validation-type=expression, expr=$expr"
        case GroupByValidation(groupByCols, aggCol, aggType, expr) => s"validation-type=$VALIDATION_GROUP_BY, " +
          s"group-by-cols=${groupByCols.mkString(",")}, aggregate-col=$aggCol, aggregate-type=$aggType, expr=$expr"
        case _ => "message=Unknown validation type"
      }
      LOGGER.info(s"Generated validation, $baseLogInfo, $validationLogInfo")
    })
    validations
  }

}
