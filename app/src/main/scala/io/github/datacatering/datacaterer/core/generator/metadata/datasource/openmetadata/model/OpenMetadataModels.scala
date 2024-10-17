package io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata.model

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_AVG, AGGREGATION_MAX, AGGREGATION_MIN, AGGREGATION_STDDEV, AGGREGATION_SUM}
import io.github.datacatering.datacaterer.core.exception.InvalidOpenMetadataTableRowCountBetweenException
import io.github.datacatering.datacaterer.core.model.ExternalDataValidation


trait OpenMetadataDataQuality extends ExternalDataValidation {

  def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder]

  protected def getBetweenValidations(minName: String, maxName: String, functionName: String, inputParams: Map[String, String],
                                      optColumnName: Option[String]): List[ValidationBuilder] = {

    val minVal = inputParams.get(minName).map(stringToNumber).map(p => getAggregatedValidation(functionName, optColumnName).greaterThanOrEqual(p))
    val maxVal = inputParams.get(maxName).map(stringToNumber).map(p => getAggregatedValidation(functionName, optColumnName).lessThanOrEqual(p))
    List(minVal, maxVal).filter(_.isDefined).map(_.get)
  }

  protected def stringToNumber(str: String): Double = {
    if (str.contains(".")) str.toDouble else str.toLong
  }
}

case class TableCustomSqlDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("sqlExpression")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    List(ValidationBuilder().expr(inputParams("sqlExpression")))
  }
}

case class TableRowCountBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValue", "maxValue")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    val optMinVal = inputParams.get("minValue").map(stringToNumber)
    val optMaxVal = inputParams.get("maxValue").map(stringToNumber)
    (optMinVal, optMaxVal) match {
      case (Some(min), Some(max)) =>
        List(
          ValidationBuilder().groupBy().count().greaterThanOrEqual(min),
          ValidationBuilder().groupBy().count().lessThanOrEqual(max)
        )
      case (None, Some(max)) => List(ValidationBuilder().groupBy().count().lessThanOrEqual(max))
      case (Some(min), None) => List(ValidationBuilder().groupBy().count().greaterThanOrEqual(min))
      case _ => throw InvalidOpenMetadataTableRowCountBetweenException()
    }
  }
}

case class TableRowCountEqualDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("value")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    inputParams.get("value").map(stringToNumber)
      .map(v => List(ValidationBuilder().groupBy().count().isEqual(v)))
      .getOrElse(List())
  }
}

case class ColumnValuesBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValue", "maxValue")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    val minKey = inputParams.get("minValue").map(stringToNumber)
    val maxKey = inputParams.get("maxValue").map(stringToNumber)
    val minValidation = minKey
      .map(minVal => ValidationBuilder().col(optColumnName.get).greaterThanOrEqual(minVal))
    val maxValidation = maxKey
      .map(maxVal => ValidationBuilder().col(optColumnName.get).lessThanOrEqual(maxVal))
    List(minValidation, maxValidation).filter(_.isDefined).map(_.get)
  }
}

case class ColumnMaxBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForMaxInCol", "maxValueForMaxInCol")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    getBetweenValidations(params.head, params.last, AGGREGATION_MAX, inputParams, optColumnName)
  }
}

case class ColumnMeanBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForMeanInCol", "maxValueForMeanInCol")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    getBetweenValidations(params.head, params.last, AGGREGATION_AVG, inputParams, optColumnName)
  }
}

case class ColumnMedianBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForMedianInCol", "maxValueForMedianInCol")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    //    getBetweenValidations(params.head, params.last, "MEDIAN", inputParams, optColumnName)
    List()
  }
}

case class ColumnMinBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForMinInCol", "maxValueForMinInCol")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    getBetweenValidations(params.head, params.last, AGGREGATION_MIN, inputParams, optColumnName)
  }
}

case class ColumnStdDevBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForStdDevInCol", "maxValueForStdDevInCol")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    getBetweenValidations(params.head, params.last, AGGREGATION_STDDEV, inputParams, optColumnName)
  }
}

case class ColumnSumBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForColSum", "maxValueForColSum")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    getBetweenValidations(params.head, params.last, AGGREGATION_SUM, inputParams, optColumnName)
  }
}

case class ColumnMissingCountDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("missingCountValue", "missingValueMatch")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    val additionalNotMatches = inputParams.get("missingValueMatch").map(missingVals => {
      val additionalMatches = missingVals.split(",").toList
      additionalMatches.map(addMissingMatch => ValidationBuilder().col(optColumnName.get).isNotEqual(addMissingMatch))
    }).getOrElse(List())
    List(
      ValidationBuilder().col(optColumnName.get).isNotNull,
      ValidationBuilder().col(optColumnName.get).isNotEqual(""),
    ) ++ additionalNotMatches
  }
}

case class ColumnInSetDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("allowedValues")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    val allowedValues = inputParams("allowedValues").split(",")
      .map(entry => entry.replace("\\\"", ""))
    List(ValidationBuilder().col(optColumnName.get).in(allowedValues: _*))
  }
}

case class ColumnNotInSetDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("forbiddenValues")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    val forbiddenValues = inputParams("forbiddenValues").split(",")
      .map(entry => entry.replace("\\\"", ""))
    List(ValidationBuilder().col(optColumnName.get).notIn(forbiddenValues: _*))
  }
}

case class ColumnNotNullDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("columnValuesToBeNotNull")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    List(ValidationBuilder().col(optColumnName.get).isNotNull)
  }
}

case class ColumnUniqueDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("columnValuesToBeUnique")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    List(ValidationBuilder().unique(optColumnName.get))
  }
}

case class ColumnMatchRegexDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("regex")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    List(ValidationBuilder().col(optColumnName.get).matches(inputParams("regex")))
  }
}

case class ColumnNotMatchRegexDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("forbiddenRegex")

  override def getValidation(inputParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    List(ValidationBuilder().col(optColumnName.get).notMatches(inputParams("forbiddenRegex")))
  }
}

