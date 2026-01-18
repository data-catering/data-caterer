package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.{ConditionalBuilder, ConditionalBranch, ConditionalCase, FieldBuilder}
import io.github.datacatering.datacaterer.api.model.Constants._
import org.scalatest.funsuite.AnyFunSuite

class ConditionalHelpersTest extends AnyFunSuite {

  // ========== ConditionalBuilder Tests ==========

  test("ConditionalBuilder.greaterThan() should create correct condition") {
    val condBuilder = ConditionalBuilder("total")
    val branch = condBuilder.greaterThan(100)

    assert(branch.condition == "total > 100")
  }

  test("ConditionalBuilder.greaterThan() should quote string values") {
    val condBuilder = ConditionalBuilder("status")
    val branch = condBuilder.greaterThan("ACTIVE")

    assert(branch.condition == "status > 'ACTIVE'")
  }

  test("ConditionalBuilder.lessThan() should create correct condition") {
    val condBuilder = ConditionalBuilder("age")
    val branch = condBuilder.lessThan(18)

    assert(branch.condition == "age < 18")
  }

  test("ConditionalBuilder.equalTo() should create correct condition with numbers") {
    val condBuilder = ConditionalBuilder("count")
    val branch = condBuilder.equalTo(5)

    assert(branch.condition == "count = 5")
  }

  test("ConditionalBuilder.equalTo() should quote string values") {
    val condBuilder = ConditionalBuilder("status")
    val branch = condBuilder.equalTo("PENDING")

    assert(branch.condition == "status = 'PENDING'")
  }

  test("ConditionalBuilder.between() should create correct condition") {
    val condBuilder = ConditionalBuilder("price")
    val branch = condBuilder.between(10, 100)

    assert(branch.condition == "price BETWEEN 10 AND 100")
  }

  test("ConditionalBuilder.in() should create correct condition with single value") {
    val condBuilder = ConditionalBuilder("category")
    val branch = condBuilder.in("A")

    assert(branch.condition == "category IN ('A')")
  }

  test("ConditionalBuilder.in() should create correct condition with multiple values") {
    val condBuilder = ConditionalBuilder("status")
    val branch = condBuilder.in("ACTIVE", "PENDING", "APPROVED")

    assert(branch.condition == "status IN ('ACTIVE', 'PENDING', 'APPROVED')")
  }

  test("ConditionalBuilder.in() should handle mixed types") {
    val condBuilder = ConditionalBuilder("priority")
    val branch = condBuilder.in(1, 2, 3)

    assert(branch.condition == "priority IN (1, 2, 3)")
  }

  test("ConditionalBuilder.greaterThanOrEqual() should create correct condition") {
    val condBuilder = ConditionalBuilder("score")
    val branch = condBuilder.greaterThanOrEqual(75)

    assert(branch.condition == "score >= 75")
  }

  test("ConditionalBuilder.lessThanOrEqual() should create correct condition") {
    val condBuilder = ConditionalBuilder("discount")
    val branch = condBuilder.lessThanOrEqual(50)

    assert(branch.condition == "discount <= 50")
  }

  test("ConditionalBuilder.notEqualTo() should create correct condition") {
    val condBuilder = ConditionalBuilder("status")
    val branch = condBuilder.notEqualTo("DELETED")

    assert(branch.condition == "status != 'DELETED'")
  }

  // ========== ConditionalBranch Tests ==========

  test("ConditionalBranch ->: operator should create ConditionalCase with numeric value") {
    val branch = ConditionalBranch("total > 100")
    val conditionalCase = 50 ->: branch

    assert(conditionalCase.condition == "total > 100")
    assert(conditionalCase.thenValue == "50")
  }

  test("ConditionalBranch ->: operator should create ConditionalCase with string value") {
    val branch = ConditionalBranch("status = 'ACTIVE'")
    val conditionalCase = "APPROVED" ->: branch

    assert(conditionalCase.condition == "status = 'ACTIVE'")
    assert(conditionalCase.thenValue == "'APPROVED'")
  }

  test("ConditionalBranch ->: operator should handle double values") {
    val branch = ConditionalBranch("amount >= 1000")
    val conditionalCase = 99.99 ->: branch

    assert(conditionalCase.condition == "amount >= 1000")
    assert(conditionalCase.thenValue == "99.99")
  }

  // ========== ConditionalCase Tests ==========

  test("ConditionalCase.toSql should generate WHEN...THEN syntax") {
    val conditionalCase = ConditionalCase("total > 100", "50")

    assert(conditionalCase.toSql == "WHEN total > 100 THEN 50")
  }

  test("ConditionalCase.toSql should handle quoted string values") {
    val conditionalCase = ConditionalCase("status = 'ACTIVE'", "'APPROVED'")

    assert(conditionalCase.toSql == "WHEN status = 'ACTIVE' THEN 'APPROVED'")
  }

  // ========== FieldBuilder.when() Tests ==========

  test("FieldBuilder.when() should return ConditionalBuilder") {
    val fb = FieldBuilder()
    val condBuilder = fb.when("total")

    assert(condBuilder.isInstanceOf[ConditionalBuilder])
    assert(condBuilder.fieldName == "total")
  }

  // ========== FieldBuilder.conditionalValue() Tests ==========

  test("conditionalValue() with single condition should generate CASE statement") {
    val fb = FieldBuilder()
    val testField = fb
      .name("discount")
      .conditionalValue(
        fb.when("total").greaterThan(100) -> 10
      )(elseValue = 0)
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("CASE"))
    assert(sql.get.toString.contains("WHEN total > 100 THEN 10"))
    assert(sql.get.toString.contains("ELSE 0"))
    assert(sql.get.toString.contains("END"))
  }

  test("conditionalValue() with multiple conditions should generate cascading CASE") {
    val fb = FieldBuilder()
    val testField = fb
      .name("discount")
      .conditionalValue(
        fb.when("total").greaterThan(1000) -> 100,
        fb.when("total").greaterThan(500) -> 50,
        fb.when("total").greaterThan(100) -> 10
      )(elseValue = 0)
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("WHEN total > 1000 THEN 100"))
    assert(sqlStr.contains("WHEN total > 500 THEN 50"))
    assert(sqlStr.contains("WHEN total > 100 THEN 10"))
    assert(sqlStr.contains("ELSE 0"))
  }

  test("conditionalValue() with string values should quote properly") {
    val fb = FieldBuilder()
    val testField = fb
      .name("status_code")
      .conditionalValue(
        fb.when("status").equalTo("ACTIVE") -> "A",
        fb.when("status").equalTo("PENDING") -> "P"
      )(elseValue = "U")
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("WHEN status = 'ACTIVE' THEN 'A'"))
    assert(sqlStr.contains("WHEN status = 'PENDING' THEN 'P'"))
    assert(sqlStr.contains("ELSE 'U'"))
  }

  test("conditionalValue() with NULL elseValue should use NULL literal") {
    val fb = FieldBuilder()
    val testField = fb
      .name("optional_discount")
      .conditionalValue(
        fb.when("total").greaterThan(1000) -> 100
      )(elseValue = "NULL")
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("ELSE NULL"))
  }

  test("conditionalValue() with default elseValue should use NULL") {
    val fb = FieldBuilder()
    val testField = fb
      .name("discount")
      .conditionalValue(
        fb.when("total").greaterThan(100) -> 10
      )()
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("ELSE NULL"))
  }

  test("conditionalValue() with between condition should work") {
    val fb = FieldBuilder()
    val testField = fb
      .name("grade")
      .conditionalValue(
        fb.when("score").between(90, 100) -> "A",
        fb.when("score").between(80, 89) -> "B"
      )(elseValue = "C")
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("WHEN score BETWEEN 90 AND 100 THEN 'A'"))
    assert(sqlStr.contains("WHEN score BETWEEN 80 AND 89 THEN 'B'"))
  }

  test("conditionalValue() with in condition should work") {
    val fb = FieldBuilder()
    val testField = fb
      .name("region")
      .conditionalValue(
        fb.when("state").in("CA", "OR", "WA") -> "West",
        fb.when("state").in("NY", "NJ", "CT") -> "East"
      )(elseValue = "Other")
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("WHEN state IN ('CA', 'OR', 'WA') THEN 'West'"))
    assert(sqlStr.contains("WHEN state IN ('NY', 'NJ', 'CT') THEN 'East'"))
  }

  test("conditionalValue() with mixed condition types should work") {
    val fb = FieldBuilder()
    val testField = fb
      .name("shipping_cost")
      .conditionalValue(
        fb.when("weight").greaterThan(50) -> 25,
        fb.when("weight").between(20, 50) -> 15,
        fb.when("weight").lessThan(20) -> 5
      )(elseValue = 0)
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("WHEN weight > 50 THEN 25"))
    assert(sqlStr.contains("WHEN weight BETWEEN 20 AND 50 THEN 15"))
    assert(sqlStr.contains("WHEN weight < 20 THEN 5"))
  }

  // ========== FieldBuilder.mapping() Tests ==========

  test("mapping() should create lookup table for single mapping") {
    val fb = FieldBuilder()
    val testField = fb
      .name("priority_code")
      .mapping("priority",
        "HIGH" -> 1
      )(defaultValue = 0)
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("CASE"))
    assert(sqlStr.contains("WHEN priority = 'HIGH' THEN 1"))
    assert(sqlStr.contains("ELSE 0"))
  }

  test("mapping() should create lookup table for multiple mappings") {
    val fb = FieldBuilder()
    val testField = fb
      .name("priority_code")
      .mapping("priority",
        "HIGH" -> 1,
        "MEDIUM" -> 2,
        "LOW" -> 3
      )(defaultValue = 0)
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("WHEN priority = 'HIGH' THEN 1"))
    assert(sqlStr.contains("WHEN priority = 'MEDIUM' THEN 2"))
    assert(sqlStr.contains("WHEN priority = 'LOW' THEN 3"))
    assert(sqlStr.contains("ELSE 0"))
  }

  test("mapping() with string target values should quote properly") {
    val fb = FieldBuilder()
    val testField = fb
      .name("status_desc")
      .mapping("status",
        "A" -> "Active",
        "P" -> "Pending",
        "I" -> "Inactive"
      )(defaultValue = "Unknown")
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("WHEN status = 'A' THEN 'Active'"))
    assert(sqlStr.contains("WHEN status = 'P' THEN 'Pending'"))
    assert(sqlStr.contains("WHEN status = 'I' THEN 'Inactive'"))
    assert(sqlStr.contains("ELSE 'Unknown'"))
  }

  test("mapping() with NULL defaultValue should use NULL literal") {
    val fb = FieldBuilder()
    val testField = fb
      .name("code")
      .mapping("category",
        "A" -> 1,
        "B" -> 2
      )(defaultValue = "NULL")
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("ELSE NULL"))
  }

  test("mapping() with default defaultValue should use NULL") {
    val fb = FieldBuilder()
    val testField = fb
      .name("code")
      .mapping("category",
        "A" -> 1
      )()
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("ELSE NULL"))
  }

  // ========== Integration Tests ==========

  test("Conditional helpers should be chainable with other field options") {
    val fb = FieldBuilder()
    val testField = fb
      .name("discount")
      .`type`(io.github.datacatering.datacaterer.api.model.IntegerType)
      .conditionalValue(
        fb.when("total").greaterThan(1000) -> 100
      )(elseValue = 0)
      .field

    assert(testField.name == "discount")
    assert(testField.`type`.isDefined)
    assert(testField.options.get(SQL_GENERATOR).isDefined)
  }

  test("Multiple fields can have different conditional logic") {
    val fb1 = FieldBuilder()
    val testField1 = fb1
      .name("discount")
      .conditionalValue(
        fb1.when("total").greaterThan(1000) -> 100
      )(elseValue = 0)
      .field

    val fb2 = FieldBuilder()
    val testField2 = fb2
      .name("shipping")
      .conditionalValue(
        fb2.when("weight").greaterThan(50) -> 25
      )(elseValue = 5)
      .field

    assert(testField1.options.get(SQL_GENERATOR).isDefined)
    assert(testField2.options.get(SQL_GENERATOR).isDefined)
    assert(testField1.options.get(SQL_GENERATOR) != testField2.options.get(SQL_GENERATOR))
  }

  test("Conditional values should work with complex nested conditions") {
    val fb = FieldBuilder()
    val testField = fb
      .name("tier")
      .conditionalValue(
        fb.when("revenue").greaterThan(1000000) -> "Platinum",
        fb.when("revenue").greaterThan(500000) -> "Gold",
        fb.when("revenue").greaterThan(100000) -> "Silver",
        fb.when("revenue").greaterThan(10000) -> "Bronze"
      )(elseValue = "Basic")
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("WHEN revenue > 1000000 THEN 'Platinum'"))
    assert(sqlStr.contains("WHEN revenue > 500000 THEN 'Gold'"))
    assert(sqlStr.contains("WHEN revenue > 100000 THEN 'Silver'"))
    assert(sqlStr.contains("WHEN revenue > 10000 THEN 'Bronze'"))
    assert(sqlStr.contains("ELSE 'Basic'"))
  }

  // ========== Edge Cases ==========

  test("Conditional with field names containing special characters should work") {
    val fb = FieldBuilder()
    val testField = fb
      .name("result")
      .conditionalValue(
        fb.when("user_score").greaterThan(100) -> 1
      )(elseValue = 0)
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("user_score > 100"))
  }

  test("Conditional with nested field references should work") {
    val fb = FieldBuilder()
    val testField = fb
      .name("status")
      .conditionalValue(
        fb.when("account.balance").greaterThan(1000) -> "Premium"
      )(elseValue = "Standard")
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("account.balance > 1000"))
  }

  test("Conditional with zero values should work correctly") {
    val fb = FieldBuilder()
    val testField = fb
      .name("fee")
      .conditionalValue(
        fb.when("account_type").equalTo("PREMIUM") -> 0
      )(elseValue = 5)
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("THEN 0"))
  }

  test("Conditional with negative values should work correctly") {
    val fb = FieldBuilder()
    val testField = fb
      .name("adjustment")
      .conditionalValue(
        fb.when("error_count").greaterThan(5) -> -10
      )(elseValue = 0)
      .field

    val sql = testField.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("THEN -10"))
  }

  // ========== Java Compatibility Tests ==========

  test("Java compatibility - when() method") {
    val fb = FieldBuilder()
    val condBuilder = fb.when("total")

    assert(condBuilder.fieldName == "total")
  }

  test("Java compatibility - conditionalValue with default elseValue") {
    val fb = FieldBuilder()
    val testFieldBuilder = fb
      .name("discount")
      .conditionalValue(
        fb.when("total").greaterThan(100) -> 10
      )()

    assert(testFieldBuilder.field.options.get(SQL_GENERATOR).isDefined)
  }

  test("Java compatibility - mapping with default defaultValue") {
    val fb = FieldBuilder()
    val testFieldBuilder = fb
      .name("code")
      .mapping("status", "A" -> 1)()

    assert(testFieldBuilder.field.options.get(SQL_GENERATOR).isDefined)
  }
}
