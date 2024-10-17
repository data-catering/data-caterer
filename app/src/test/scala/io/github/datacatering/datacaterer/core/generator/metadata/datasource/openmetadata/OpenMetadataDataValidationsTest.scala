package io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata

import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, GroupByValidation}
import org.junit.runner.RunWith
import org.openmetadata.client.model.{TestCase, TestCaseParameterValue}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OpenMetadataDataValidationsTest extends AnyFunSuite {

  test("Can read test cases from OpenMetadata and convert to data validations") {
    val params = Map("minValue" -> "123", "maxValue" -> "321")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("id"))

    assert(result.size == 2)
    assert(result.forall(_.validation.description.isEmpty))
    assert(result.forall(_.validation.errorThreshold.isEmpty))
    assert(result.forall(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "id >= 123.0"))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "id <= 321.0"))
  }

  test("Can convert to min check when only minValue is defined") {
    val params = Map("minValue" -> "123")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("id"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "id >= 123.0"))
  }

  test("Can convert to max check when only maxValue is defined") {
    val params = Map("maxValue" -> "321")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("id"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "id <= 321.0"))
  }

  test("Can convert from generic sql expression") {
    val params = Map("sqlExpression" -> "my sql")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, None)

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "my sql"))
  }

  test("Can convert to table count check with only minValue") {
    val params = Map("minValue" -> "1")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, None)

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "count >= 1.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggType == "count"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggCol.isEmpty))
  }

  test("Can convert to table count check with only maxValue") {
    val params = Map("maxValue" -> "100")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, None)

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "count <= 100.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggType == "count"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggCol.isEmpty))
  }

  test("Can convert to table count check") {
    val params = Map("minValue" -> "1", "maxValue" -> "100")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, None)

    assert(result.size == 2)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "count >= 1.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "count <= 100.0"))
    assert(result.forall(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.forall(v => v.validation.asInstanceOf[GroupByValidation].aggType == "count"))
    assert(result.forall(v => v.validation.asInstanceOf[GroupByValidation].aggCol.isEmpty))
  }

  test("Can convert to table count equal check") {
    val params = Map("value" -> "10")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, None)

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "count == 10.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggCol.isEmpty))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggType == "count"))
  }

  test("Can convert to column max between validation") {
    val params = Map("minValueForMaxInCol" -> "10", "maxValueForMaxInCol" -> "20")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("amount"))

    assert(result.size == 2)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "max(amount) >= 10.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "max(amount) <= 20.0"))
    assert(result.forall(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.forall(v => v.validation.asInstanceOf[GroupByValidation].aggType == "max"))
    assert(result.forall(v => v.validation.asInstanceOf[GroupByValidation].aggCol == "amount"))
  }

  test("Can convert to column max between validation with only min") {
    val params = Map("minValueForMaxInCol" -> "10")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("amount"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "max(amount) >= 10.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggCol == "amount"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggType == "max"))
  }

  test("Can convert to column max between validation with only max") {
    val params = Map("maxValueForMaxInCol" -> "20")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("amount"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "max(amount) <= 20.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggType == "max"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggCol == "amount"))
  }

  test("Can convert to column mean between validation") {
    val params = Map("minValueForMeanInCol" -> "10")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("amount"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "avg(amount) >= 10.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggType == "avg"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggCol == "amount"))
  }

  test("Can convert to column median between validation") {
    val params = Map("minValueForMedianInCol" -> "10")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("amount"))

    assert(result.isEmpty)
  }

  test("Can convert to column min between validation") {
    val params = Map("minValueForMinInCol" -> "10")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("amount"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "min(amount) >= 10.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggCol == "amount"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggType == "min"))
  }

  test("Can convert to column stddev between validation") {
    val params = Map("minValueForStdDevInCol" -> "10")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("amount"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "stddev(amount) >= 10.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggType == "stddev"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggCol == "amount"))
  }

  test("Can convert to column sum between validation") {
    val params = Map("minValueForColSum" -> "10")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("amount"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "sum(amount) >= 10.0"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].groupByCols.isEmpty))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggType == "sum"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggCol == "amount"))
  }

  test("Can convert to column missing count validation") {
    val params = Map("missingCountValue" -> "10", "missingValueMatch" -> "N/A")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("name"))

    assert(result.size == 3)
    assert(result.forall(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "ISNOTNULL(name)"))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "name != ''"))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "name != 'N/A'"))
  }

  test("Can convert to column in set validation") {
    val params = Map("allowedValues" -> "\\\"peter\\\",\\\"flook\\\"")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("name"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "name IN ('peter','flook')"))
  }

  test("Can convert to column not in set validation") {
    val params = Map("forbiddenValues" -> "\\\"peter\\\",\\\"flook\\\"")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("name"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "NOT name IN ('peter','flook')"))
  }

  test("Can convert to column not null validation") {
    val params = Map("columnValuesToBeNotNull" -> "")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("name"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "ISNOTNULL(name)"))
  }

  test("Can convert to column unique validation") {
    val params = Map("columnValuesToBeUnique" -> "")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("name"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[GroupByValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggExpr == "count == 1"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggType == "count"))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].groupByCols == Seq("name")))
    assert(result.exists(v => v.validation.asInstanceOf[GroupByValidation].aggCol == "unique"))
  }

  test("Can convert to column match regex validation") {
    val params = Map("regex" -> "^abc123$")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("name"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "REGEXP(name, '^abc123$')"))
  }

  test("Can convert to column not match regex validation") {
    val params = Map("forbiddenRegex" -> "^abc123$")
    val testCase = createTestCaseWithParams(params)

    val result = OpenMetadataDataValidations.getDataValidations(testCase, params, Some("name"))

    assert(result.size == 1)
    assert(result.forall(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "!REGEXP(name, '^abc123$')"))
  }

  private def createTestCaseWithParams(params: Map[String, String]): TestCase = {
    val testCase = new TestCase()
    params.foldLeft(testCase)((tc, p) => {
      val tcParam = new TestCaseParameterValue()
      tcParam.setName(p._1)
      tcParam.setValue(p._2)
      tc.addParameterValuesItem(tcParam)
    })
  }
}
