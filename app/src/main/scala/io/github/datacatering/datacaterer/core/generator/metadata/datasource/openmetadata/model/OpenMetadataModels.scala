package io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata.model

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_AVG, AGGREGATION_MAX, AGGREGATION_MIN, AGGREGATION_STDDEV, AGGREGATION_SUM}
import io.github.datacatering.datacaterer.core.exception.InvalidOpenMetadataTableRowCountBetweenException
import io.github.datacatering.datacaterer.core.model.ExternalDataValidation


trait OpenMetadataDataQuality extends ExternalDataValidation {

  def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder]

  protected def getBetweenValidations(minName: String, maxName: String, functionName: String, inputParams: Map[String, String],
                                      optFieldName: Option[String]): List[ValidationBuilder] = {

    val minVal = inputParams.get(minName).map(stringToNumber).map(p => getAggregatedValidation(functionName, optFieldName).greaterThan(p, false))
    val maxVal = inputParams.get(maxName).map(stringToNumber).map(p => getAggregatedValidation(functionName, optFieldName).lessThan(p, false))
    List(minVal, maxVal).filter(_.isDefined).map(_.get)
  }

  protected def stringToNumber(str: String): Double = {
    str.toDouble
  }
}

case class TableCustomSqlDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("sqlExpression")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    List(ValidationBuilder().expr(inputParams("sqlExpression")))
  }
}

case class TableRowCountBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValue", "maxValue")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    val optMinVal = inputParams.get("minValue").map(stringToNumber)
    val optMaxVal = inputParams.get("maxValue").map(stringToNumber)
    (optMinVal, optMaxVal) match {
      case (Some(min), Some(max)) =>
        List(
          ValidationBuilder().groupBy().count().greaterThan(min, false),
          ValidationBuilder().groupBy().count().lessThan(max, false)
        )
      case (None, Some(max)) => List(ValidationBuilder().groupBy().count().lessThan(max, false))
      case (Some(min), None) => List(ValidationBuilder().groupBy().count().greaterThan(min, false))
      case _ => throw InvalidOpenMetadataTableRowCountBetweenException()
    }
  }
}

case class TableRowCountEqualDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("value")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    inputParams.get("value").map(stringToNumber)
      .map(v => List(ValidationBuilder().groupBy().count().isEqual(v)))
      .getOrElse(List())
  }
}

case class FieldValuesBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValue", "maxValue")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    val minKey = inputParams.get("minValue").map(stringToNumber)
    val maxKey = inputParams.get("maxValue").map(stringToNumber)
    val minValidation = minKey
      .map(minVal => ValidationBuilder().field(optFieldName.get).greaterThan(minVal, false))
    val maxValidation = maxKey
      .map(maxVal => ValidationBuilder().field(optFieldName.get).lessThan(maxVal, false))
    List(minValidation, maxValidation).filter(_.isDefined).map(_.get)
  }
}

case class FieldMaxBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForMaxInCol", "maxValueForMaxInCol")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    getBetweenValidations(params.head, params.last, AGGREGATION_MAX, inputParams, optFieldName)
  }
}

case class FieldMeanBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForMeanInCol", "maxValueForMeanInCol")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    getBetweenValidations(params.head, params.last, AGGREGATION_AVG, inputParams, optFieldName)
  }
}

case class FieldMedianBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForMedianInCol", "maxValueForMedianInCol")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    //    getBetweenValidations(params.head, params.last, "MEDIAN", inputParams, optFieldName)
    List()
  }
}

case class FieldMinBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForMinInCol", "maxValueForMinInCol")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    getBetweenValidations(params.head, params.last, AGGREGATION_MIN, inputParams, optFieldName)
  }
}

case class FieldStdDevBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForStdDevInCol", "maxValueForStdDevInCol")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    getBetweenValidations(params.head, params.last, AGGREGATION_STDDEV, inputParams, optFieldName)
  }
}

case class FieldSumBetweenDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("minValueForColSum", "maxValueForColSum")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    getBetweenValidations(params.head, params.last, AGGREGATION_SUM, inputParams, optFieldName)
  }
}

case class FieldMissingCountDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("missingCountValue", "missingValueMatch")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    val additionalNotMatches = inputParams.get("missingValueMatch").map(missingVals => {
      val additionalMatches = missingVals.split(",").toList
      additionalMatches.map(addMissingMatch => ValidationBuilder().field(optFieldName.get).isEqual(addMissingMatch, true))
    }).getOrElse(List())
    List(
      ValidationBuilder().field(optFieldName.get).isNull(true),
      ValidationBuilder().field(optFieldName.get).isEqual("", true),
    ) ++ additionalNotMatches
  }
}

case class FieldInSetDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("allowedValues")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    val allowedValues = inputParams("allowedValues").split(",")
      .map(entry => entry.replace("\\\"", ""))
    List(ValidationBuilder().field(optFieldName.get).in(allowedValues: _*))
  }
}

case class FieldNotInSetDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("forbiddenValues")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    val forbiddenValues = inputParams("forbiddenValues").split(",")
      .map(entry => entry.replace("\\\"", ""))
    List(ValidationBuilder().field(optFieldName.get).in(forbiddenValues.toList, true))
  }
}

case class FieldNotNullDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("fieldValuesToBeNotNull")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(optFieldName.get).isNull(true))
  }
}

case class FieldUniqueDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("fieldValuesToBeUnique")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    List(ValidationBuilder().unique(optFieldName.get))
  }
}

case class FieldMatchRegexDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("regex")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(optFieldName.get).matches(inputParams("regex")))
  }
}

case class FieldNotMatchRegexDataQuality() extends OpenMetadataDataQuality {
  override val params: List[String] = List("forbiddenRegex")

  override def getValidation(inputParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(optFieldName.get).matches(inputParams("forbiddenRegex"), true))
  }
}

