package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.{DistinctContainsSetFieldValidation, DistinctEqualFieldValidation, DistinctInSetFieldValidation, ExpressionValidation, FieldNamesValidation, FieldValidations, IsDecreasingFieldValidation, IsIncreasingFieldValidation, IsJsonParsableFieldValidation, LengthBetweenFieldValidation, LengthEqualFieldValidation, MatchDateTimeFormatFieldValidation, MatchJsonSchemaFieldValidation, MatchesListFieldValidation, MaxBetweenFieldValidation, MeanBetweenFieldValidation, MedianBetweenFieldValidation, MinBetweenFieldValidation, MostCommonValueInSetFieldValidation, QuantileValuesBetweenFieldValidation, StdDevBetweenFieldValidation, SumBetweenFieldValidation, UniqueValuesProportionBetweenFieldValidation}
import io.github.datacatering.datacaterer.core.util.{SparkSuite, Transaction}
import io.github.datacatering.datacaterer.core.validator.{ExpressionValidationOps, FieldNamesValidationOps, FieldValidationsOps}
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

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define select expression to run before where expression") {
    val validation = ExpressionValidation("median_amount < 1000", List("PERCENTILE(amount, 0.5) AS median_amount"))
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define matches list field validation") {
    val validation = MatchesListFieldValidation(List("peter"))
    val result = new FieldValidationsOps(FieldValidations("name", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define distinct in set field validation") {
    val validation = DistinctInSetFieldValidation(List("peter"))
    val result = new FieldValidationsOps(FieldValidations("name", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define distinct contains set field validation") {
    val validation = DistinctContainsSetFieldValidation(List("peter"))
    val result = new FieldValidationsOps(FieldValidations("name", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define distinct equal field validation") {
    val validation = DistinctEqualFieldValidation(List("peter"))
    val result = new FieldValidationsOps(FieldValidations("name", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define max between field validation") {
    val validation = MaxBetweenFieldValidation(0, 1000)
    val result = new FieldValidationsOps(FieldValidations("amount", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define mean between field validation") {
    val validation = MeanBetweenFieldValidation(0, 1000)
    val result = new FieldValidationsOps(FieldValidations("amount", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define median between field validation") {
    val validation = MedianBetweenFieldValidation(0, 1000)
    val result = new FieldValidationsOps(FieldValidations("amount", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define min between field validation") {
    val validation = MinBetweenFieldValidation(0, 1000)
    val result = new FieldValidationsOps(FieldValidations("amount", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define standard deviation between field validation") {
    val validation = StdDevBetweenFieldValidation(0, 1000)
    val result = new FieldValidationsOps(FieldValidations("amount", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define sum between field validation") {
    val validation = SumBetweenFieldValidation(0, 1000)
    val result = new FieldValidationsOps(FieldValidations("amount", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define length between field validation") {
    val validation = LengthBetweenFieldValidation(0, 10)
    val result = new FieldValidationsOps(FieldValidations("name", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define length equal field validation") {
    val validation = LengthEqualFieldValidation(5)
    val result = new FieldValidationsOps(FieldValidations("name", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define is decreasing field validation") {
    val validation = IsDecreasingFieldValidation()
    val result = new FieldValidationsOps(FieldValidations("amount", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(!result.head.isSuccess)
    assertResult(1)(result.head.sampleErrorValues.size)
  }

  test("Can define is increasing field validation") {
    val validation = IsIncreasingFieldValidation()
    val result = new FieldValidationsOps(FieldValidations("amount", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define is JSON parsable field validation") {
    val validation = IsJsonParsableFieldValidation(true)
    val result = new FieldValidationsOps(FieldValidations("name", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define match JSON schema field validation") {
    val validation = MatchJsonSchemaFieldValidation("my_schema string", true)
    val result = new FieldValidationsOps(FieldValidations("name", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(!result.head.isSuccess)
    assertResult(1)(result.head.sampleErrorValues.size)
  }

  test("Can define match date time format field validation") {
    val validation = MatchDateTimeFormatFieldValidation("YYYY-MM-dd")
    val result = new FieldValidationsOps(FieldValidations("created_date", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define most common value in set field validation") {
    val validation = MostCommonValueInSetFieldValidation(List("peter"))
    val result = new FieldValidationsOps(FieldValidations("name", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define unique values proportion between field validation") {
    val validation = UniqueValuesProportionBetweenFieldValidation(0.1, 0.3)
    val result = new FieldValidationsOps(FieldValidations("name", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can define quantile values between field validation") {
    val quantileRanges = Map(0.1 -> (0.0, 100.0))
    val validation = QuantileValuesBetweenFieldValidation(quantileRanges)
    val result = new FieldValidationsOps(FieldValidations("amount", List(validation))).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can return empty sample rows when validation is successful from error threshold") {
    val validation = new ValidationBuilder().expr("amount < 400").errorThreshold(1).validation.asInstanceOf[ExpressionValidation]
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can get sample rows when validation is not successful") {
    val validation = ExpressionValidation("amount < 100")
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assertResult(1)(result.size)
    assert(!result.head.isSuccess)
    assert(result.head.sampleErrorValues.isDefined)
    assertResult(2)(result.head.sampleErrorValues.get.length)
    assertResult(2)(result.head.sampleErrorValues.get.count(r => r("amount").asInstanceOf[Double] >= 100))
  }

  test("Can get sample rows when validation is not successful by error threshold greater than 1") {
    val validation = new ValidationBuilder().expr("amount < 20").errorThreshold(2).validation.asInstanceOf[ExpressionValidation]
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assertResult(1)(result.size)
    assert(!result.head.isSuccess)
    assert(result.head.sampleErrorValues.isDefined)
    assertResult(3)(result.head.sampleErrorValues.get.length)
    assertResult(3)(result.head.sampleErrorValues.get.count(r => r("amount").asInstanceOf[Double] >= 20))
  }

  test("Can get sample rows when validation is not successful by error threshold less than 1") {
    val validation = new ValidationBuilder().expr("amount < 100").errorThreshold(0.1).validation.asInstanceOf[ExpressionValidation]
    val result = new ExpressionValidationOps(validation).validate(df, 4)

    assertResult(1)(result.size)
    assert(!result.head.isSuccess)
    assert(result.head.sampleErrorValues.isDefined)
    assertResult(2)(result.head.sampleErrorValues.get.length)
    assertResult(2)(result.head.sampleErrorValues.get.count(r => r("amount").asInstanceOf[Double] >= 100))
  }

  test("Can check field names count is equal") {
    val validation = new ValidationBuilder().fieldNames.countEqual(6).validation.asInstanceOf[FieldNamesValidation]
    val result = new FieldNamesValidationOps(validation).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assertResult(1)(result.head.total)
    assertResult(0)(result.head.numErrors)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can check field names count is between") {
    val validation = new ValidationBuilder().fieldNames.countBetween(3, 6).validation.asInstanceOf[FieldNamesValidation]
    val result = new FieldNamesValidationOps(validation).validate(df, 4)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assertResult(1)(result.head.total)
    assertResult(0)(result.head.numErrors)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can show error when field name order fails") {
    val validation = new ValidationBuilder().fieldNames.matchOrder("account_id", "name", "transaction_id", "amount", "created_date").validation.asInstanceOf[FieldNamesValidation]
    val result = new FieldNamesValidationOps(validation).validate(df, 4)

    assertResult(1)(result.size)
    assert(!result.head.isSuccess)
    assertResult(5)(result.head.total)
    assertResult(2)(result.head.numErrors)
    assert(result.head.sampleErrorValues.isDefined)
    assertResult(2)(result.head.sampleErrorValues.get.length)
  }

  test("Can show error when field name not in set") {
    val validation = new ValidationBuilder().fieldNames.matchSet("account_id", "name", "transaction_id", "my_amount").validation.asInstanceOf[FieldNamesValidation]
    val result = new FieldNamesValidationOps(validation).validate(df, 4)

    assertResult(1)(result.size)
    assert(!result.head.isSuccess)
    assertResult(4)(result.head.total)
    assertResult(1)(result.head.numErrors)
    assert(result.head.sampleErrorValues.isDefined)
    assertResult(1)(result.head.sampleErrorValues.get.length)
  }
}
