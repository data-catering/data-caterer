package io.github.datacatering.datacaterer.core.alert

import io.github.datacatering.datacaterer.api.model.Constants.{ALERT_TRIGGER_ON_ALL, ALERT_TRIGGER_ON_FAILURE, ALERT_TRIGGER_ON_GENERATION_FAILURE, ALERT_TRIGGER_ON_GENERATION_SUCCESS, ALERT_TRIGGER_ON_SUCCESS, ALERT_TRIGGER_ON_VALIDATION_FAILURE, ALERT_TRIGGER_ON_VALIDATION_SUCCESS}
import io.github.datacatering.datacaterer.api.model.{AlertConfig, DataCatererConfiguration}
import io.github.datacatering.datacaterer.core.model.{DataSourceResult, DataSourceValidationResult, SinkResult, ValidationConfigResult, ValidationResult}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AlertProcessorTest extends AnyFunSuite {

  test("Can send alerts when trigger condition is met") {
    val expectedResults = List(
      (ALERT_TRIGGER_ON_ALL, true, true, true), (ALERT_TRIGGER_ON_ALL, false, false, true), (ALERT_TRIGGER_ON_ALL, true, false, true),
      (ALERT_TRIGGER_ON_SUCCESS, true, true, true), (ALERT_TRIGGER_ON_SUCCESS, false, false, false), (ALERT_TRIGGER_ON_SUCCESS, true, false, false),
      (ALERT_TRIGGER_ON_FAILURE, true, true, false), (ALERT_TRIGGER_ON_FAILURE, false, false, true), (ALERT_TRIGGER_ON_FAILURE, true, false, false),
      (ALERT_TRIGGER_ON_GENERATION_SUCCESS, true, true, true), (ALERT_TRIGGER_ON_GENERATION_SUCCESS, false, false, false), (ALERT_TRIGGER_ON_GENERATION_SUCCESS, true, false, true),
      (ALERT_TRIGGER_ON_GENERATION_FAILURE, true, true, false), (ALERT_TRIGGER_ON_GENERATION_FAILURE, false, false, true), (ALERT_TRIGGER_ON_GENERATION_FAILURE, true, false, false),
      (ALERT_TRIGGER_ON_VALIDATION_SUCCESS, true, true, true), (ALERT_TRIGGER_ON_VALIDATION_SUCCESS, false, false, false), (ALERT_TRIGGER_ON_VALIDATION_SUCCESS, false, true, true),
      (ALERT_TRIGGER_ON_VALIDATION_FAILURE, true, true, false), (ALERT_TRIGGER_ON_VALIDATION_FAILURE, false, false, true), (ALERT_TRIGGER_ON_VALIDATION_FAILURE, true, false, true),
    )

    expectedResults.foreach(e => {
      val generationResult = List(DataSourceResult(sinkResult = SinkResult(isSuccess = e._2)))
      val validationResult = List(ValidationConfigResult(dataSourceValidationResults =
        List(DataSourceValidationResult(validationResults =
          List(ValidationResult(isSuccess = e._3))
        ))
      ))

      val result = new AlertProcessor(DataCatererConfiguration(alertConfig = AlertConfig(e._1))).shouldTriggerAlert(generationResult, validationResult)
      if (e._4) assert(result) else assert(!result)
    })
  }

}
