package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.{ColumnNamesValidation, ExpressionValidation}
import io.github.datacatering.datacaterer.core.util.{SparkSuite, Transaction}
import io.github.datacatering.datacaterer.core.validator.{ColumnNamesValidationOps, ExpressionValidationOps}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.Date

@RunWith(classOf[JUnitRunner])
class ValidationOperationsTest extends SparkSuite {

  private val sampleData = Seq(
    Transaction("acc123", "peter", "txn1", Date.valueOf("2020-01-01"), 10.0),
    Transaction("acc123", "peter", "txn2", Date.valueOf("2020-01-01"), 50.0),
    Transaction("acc123", "peter", "txn3", Date.valueOf("2020-01-01"), 200.0),
    Transaction("acc123", "peter", "txn4", Date.valueOf("2020-01-01"), 500.0)
  )
  private val df = sparkSession.createDataFrame(sampleData)

  test("Can return empty sample rows when validation is successful") {
    val validation = ExpressionValidation("amount < 1000")
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assert(result.isSuccess)
    assert(result.sampleErrorValues.isEmpty)
  }

  test("Can define select expression to run before where expression") {
    val validation = ExpressionValidation("median_amount < 1000", List("PERCENTILE(amount, 0.5) AS median_amount"))
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assert(result.isSuccess)
    assert(result.sampleErrorValues.isEmpty)
  }

  test("Can return empty sample rows when validation is successful from error threshold") {
    val validation = new ValidationBuilder().expr("amount < 400").errorThreshold(1).validation.asInstanceOf[ExpressionValidation]
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assert(result.isSuccess)
    assert(result.sampleErrorValues.isEmpty)
  }

  test("Can get sample rows when validation is not successful") {
    val validation = ExpressionValidation("amount < 100")
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assert(!result.isSuccess)
    assert(result.sampleErrorValues.isDefined)
    assertResult(2)(result.sampleErrorValues.get.count())
    assertResult(2)(result.sampleErrorValues.get.filter(r => r.getAs[Double]("amount") >= 100).count())
  }

  test("Can get sample rows when validation is not successful by error threshold greater than 1") {
    val validation = new ValidationBuilder().expr("amount < 20").errorThreshold(2).validation.asInstanceOf[ExpressionValidation]
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assert(!result.isSuccess)
    assert(result.sampleErrorValues.isDefined)
    assertResult(3)(result.sampleErrorValues.get.count())
    assertResult(3)(result.sampleErrorValues.get.filter(r => r.getAs[Double]("amount") >= 20).count())
  }

  test("Can get sample rows when validation is not successful by error threshold less than 1") {
    val validation = new ValidationBuilder().expr("amount < 100").errorThreshold(0.1).validation.asInstanceOf[ExpressionValidation]
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assert(!result.isSuccess)
    assert(result.sampleErrorValues.isDefined)
    assertResult(2)(result.sampleErrorValues.get.count())
    assertResult(2)(result.sampleErrorValues.get.filter(r => r.getAs[Double]("amount") >= 100).count())
  }

  test("Can check column names count is equal") {
    val validation = new ValidationBuilder().columnNames.countEqual(5).validation.asInstanceOf[ColumnNamesValidation]
    val result = new ColumnNamesValidationOps(validation).validate(df, 4)

    assert(result.isSuccess)
    assertResult(1)(result.total)
    assertResult(0)(result.numErrors)
    assert(result.sampleErrorValues.isEmpty)
  }

  test("Can check column names count is between") {
    val validation = new ValidationBuilder().columnNames.countBetween(3, 5).validation.asInstanceOf[ColumnNamesValidation]
    val result = new ColumnNamesValidationOps(validation).validate(df, 4)

    assert(result.isSuccess)
    assertResult(1)(result.total)
    assertResult(0)(result.numErrors)
    assert(result.sampleErrorValues.isEmpty)
  }

  test("Can show error when column name order fails") {
    val validation = new ValidationBuilder().columnNames.matchOrder("account_id", "name", "transaction_id", "amount", "created_date").validation.asInstanceOf[ColumnNamesValidation]
    val result = new ColumnNamesValidationOps(validation).validate(df, 4)

    assert(!result.isSuccess)
    assertResult(5)(result.total)
    assertResult(2)(result.numErrors)
    assert(result.sampleErrorValues.isDefined)
    assertResult(2)(result.sampleErrorValues.get.count())
  }

  test("Can show error when column name not in set") {
    val validation = new ValidationBuilder().columnNames.matchSet("account_id", "name", "transaction_id", "my_amount").validation.asInstanceOf[ColumnNamesValidation]
    val result = new ColumnNamesValidationOps(validation).validate(df, 4)

    assert(!result.isSuccess)
    assertResult(4)(result.total)
    assertResult(1)(result.numErrors)
    assert(result.sampleErrorValues.isDefined)
    assertResult(1)(result.sampleErrorValues.get.count())
  }
}
