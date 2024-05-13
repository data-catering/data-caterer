package io.github.datacatering.datacaterer.core.validator

import io.github.datacatering.datacaterer.api.model.{FoldersConfig, ValidationConfig}
import io.github.datacatering.datacaterer.api.{PreFilterBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.util.{SparkSuite, Transaction}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.Date

@RunWith(classOf[JUnitRunner])
class ValidationProcessorTest extends SparkSuite {

  private val sampleData = Seq(
    Transaction("acc123", "peter", "txn1", Date.valueOf("2020-01-01"), 10.0),
    Transaction("acc123", "peter", "txn2", Date.valueOf("2020-01-01"), 50.0),
    Transaction("acc123", "peter", "txn3", Date.valueOf("2020-01-02"), 200.0),
    Transaction("acc123", "peter", "txn4", Date.valueOf("2020-01-03"), 500.0)
  )
  private val df = sparkSession.createDataFrame(sampleData)

  test("Can pre-filter data before running validation") {
    val validation = ValidationBuilder()
      .preFilter(PreFilterBuilder().filter(ValidationBuilder().col("transaction_id").in("txn3", "txn4")))
      .col("amount").greaterThan(100)
    val validationProcessor = new ValidationProcessor(Map(), None, ValidationConfig(), FoldersConfig())
    val result = validationProcessor.tryValidate(df, validation)

    assert(result.isSuccess)
    assertResult(2)(result.total)
    assert(result.sampleErrorValues.isEmpty)
  }

  test("Can pre-filter data with multiple conditions before running validation") {
    val validation = ValidationBuilder()
      .preFilter(PreFilterBuilder()
        .filter(ValidationBuilder().col("transaction_id").in("txn3", "txn4"))
        .and(ValidationBuilder().col("account_id").isEqual("acc123"))
        .and(ValidationBuilder().col("name").isEqual("peter"))
      )
      .col("amount").greaterThan(100)
    val validationProcessor = new ValidationProcessor(Map(), None, ValidationConfig(), FoldersConfig())
    val result = validationProcessor.tryValidate(df, validation)

    assert(result.isSuccess)
    assertResult(2)(result.total)
    assert(result.sampleErrorValues.isEmpty)
  }
}
