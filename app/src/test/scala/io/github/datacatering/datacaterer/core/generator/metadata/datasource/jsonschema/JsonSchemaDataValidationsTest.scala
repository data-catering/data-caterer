package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, FieldNamesValidation, GroupByValidation}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model.{JsonSchemaDefinition, JsonSchemaProperty}
import org.scalatest.funsuite.AnyFunSuite

class JsonSchemaDataValidationsTest extends AnyFunSuite {

  test("can convert object schema with required fields to validations") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "name" -> JsonSchemaProperty(`type` = Some("string")),
        "age" -> JsonSchemaProperty(`type` = Some("integer"))
      )),
      required = Some(List("name", "age"))
    )

    val result = JsonSchemaDataValidations.getDataValidations(schema, "test-schema")

    assert(result.size >= 2)
    // Check for required field validations
    val requiredValidations = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .filter(_.expr.contains("ISNOTNULL"))
    
    assert(requiredValidations.size == 2)
    assert(requiredValidations.exists(_.expr == "ISNOTNULL(`name`)"))
    assert(requiredValidations.exists(_.expr == "ISNOTNULL(`age`)"))
  }

  test("can convert string property with pattern to validation") {
    val property = JsonSchemaProperty(
      `type` = Some("string"),
      pattern = Some("^[A-Z][a-z]+$")
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("name", property)

    assert(result.size >= 2) // type + pattern validation
    val patternValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(_.expr.contains("REGEXP"))
    
    assert(patternValidation.isDefined)
    assert(patternValidation.get.expr == "REGEXP(`name`, '^[A-Z][a-z]+$')")
  }

  test("can convert string property with length constraints to validations") {
    val property = JsonSchemaProperty(
      `type` = Some("string"),
      minLength = Some(5),
      maxLength = Some(20)
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("description", property)

    assert(result.size >= 2) // type + length validation
    val lengthValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(_.expr.contains("LENGTH"))
    
    assert(lengthValidation.isDefined)
    assert(lengthValidation.get.expr == "LENGTH(`description`) BETWEEN 5 AND 20")
  }

  test("can convert string property with format to validation") {
    val emailProperty = JsonSchemaProperty(
      `type` = Some("string"),
      format = Some("email")
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("email", emailProperty)

    assert(result.size >= 2) // type + format validation
    val formatValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(_.expr.contains("REGEXP"))
    
    assert(formatValidation.isDefined)
    assert(formatValidation.get.expr.contains("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
  }

  test("can convert numeric property with range constraints to validations") {
    val property = JsonSchemaProperty(
      `type` = Some("integer"),
      minimum = Some("1"),
      maximum = Some("100")
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("score", property)

    assert(result.size >= 2) // type + range validation
    val rangeValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(_.expr.contains("BETWEEN"))
    
    assert(rangeValidation.isDefined)
    assert(rangeValidation.get.expr == "`score` BETWEEN 1.0 AND 100.0")
  }

  test("can convert property with enum values to validation") {
    val property = JsonSchemaProperty(
      `type` = Some("string"),
      enum = Some(List("red", "green", "blue"))
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("color", property)

    assert(result.size >= 2) // type + enum validation
    val enumValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(_.expr.contains("IN"))
    
    assert(enumValidation.isDefined)
    assert(enumValidation.get.expr == "`color` IN ('red','green','blue')")
  }

  test("can convert property with const value to validation") {
    val property = JsonSchemaProperty(
      `type` = Some("string"),
      const = Some("fixed-value")
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("status", property)

    assert(result.size >= 2) // type + const validation
    val constValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(v => v.expr.contains("==") && !v.expr.contains("TYPEOF"))
    
    assert(constValidation.isDefined)
    assert(constValidation.get.expr == "`status` == 'fixed-value'")
  }

  test("can convert array schema with item constraints to validations") {
    val schema = JsonSchemaDefinition(
      `type` = Some("array"),
      minItems = Some(2),
      maxItems = Some(10),
      uniqueItems = Some(true)
    )

    val result = JsonSchemaDataValidations.getDataValidations(schema, "test-array")

    assert(result.size >= 2) // length + unique validations
    
    val lengthValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(_.expr.contains("BETWEEN"))
    
    val uniqueValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(_.expr.contains("ARRAY_DISTINCT"))
    
    assert(lengthValidation.isDefined, s"Length validation not found. Available validations: ${result.map(_.validation.asInstanceOf[ExpressionValidation].expr)}")
    assert(uniqueValidation.isDefined, s"Unique validation not found. Available validations: ${result.map(_.validation.asInstanceOf[ExpressionValidation].expr)}")
    assert(lengthValidation.get.expr == "LENGTH(`root_array`) BETWEEN 2 AND 10")
    assert(uniqueValidation.get.expr == "SIZE(root_array) == SIZE(ARRAY_DISTINCT(root_array))")
  }

  test("can convert object schema with additionalProperties false to validation") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "name" -> JsonSchemaProperty(`type` = Some("string")),
        "age" -> JsonSchemaProperty(`type` = Some("integer"))
      )),
      additionalProperties = Some(false)
    )

    val result = JsonSchemaDataValidations.getDataValidations(schema, "test-object")

    assert(result.nonEmpty)
    val fieldCountValidation = result.find(_.validation.isInstanceOf[FieldNamesValidation])
    assert(fieldCountValidation.isDefined)
  }

  test("can handle multiple format types") {
    val formats = Map(
      "email" -> "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
      "uri" -> "^https?://[^\\s/$.?#].[^\\s]*$",
      "uuid" -> "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
      "date" -> "^\\d{4}-\\d{2}-\\d{2}$",
      "date-time" -> "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z?$",
      "ipv4" -> "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    )

    formats.foreach { case (format, expectedPattern) =>
      val property = JsonSchemaProperty(
        `type` = Some("string"),
        format = Some(format)
      )

      val result = JsonSchemaDataValidations.getPropertyValidations("testField", property)
      val formatValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
        .map(_.validation.asInstanceOf[ExpressionValidation])
        .find(_.expr.contains("REGEXP"))
      
      assert(formatValidation.isDefined, s"Format validation not found for format: $format")
      assert(formatValidation.get.expr.contains(expectedPattern), s"Pattern mismatch for format: $format")
    }
  }

  test("can handle numeric constraints with only minimum") {
    val property = JsonSchemaProperty(
      `type` = Some("number"),
      minimum = Some("10")
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("value", property)

    val minValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(_.expr.contains(">="))
    
    assert(minValidation.isDefined)
    assert(minValidation.get.expr == "`value` >= 10.0")
  }

  test("can handle numeric constraints with only maximum") {
    val property = JsonSchemaProperty(
      `type` = Some("number"),
      maximum = Some("50")
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("value", property)

    val maxValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(_.expr.contains("<="))
    
    assert(maxValidation.isDefined)
    assert(maxValidation.get.expr == "`value` <= 50.0")
  }

  test("can handle string constraints with only minLength") {
    val property = JsonSchemaProperty(
      `type` = Some("string"),
      minLength = Some(5)
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("text", property)

    val minLengthValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(v => v.expr.contains("LENGTH") && v.expr.contains(">="))
    
    assert(minLengthValidation.isDefined)
    assert(minLengthValidation.get.expr == "LENGTH(`text`) >= 5")
  }

  test("can handle string constraints with only maxLength") {
    val property = JsonSchemaProperty(
      `type` = Some("string"),
      maxLength = Some(20)
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("text", property)

    val maxLengthValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(v => v.expr.contains("LENGTH") && v.expr.contains("<="))
    
    assert(maxLengthValidation.isDefined)
    assert(maxLengthValidation.get.expr == "LENGTH(`text`) <= 20")
  }

  test("can handle array constraints with only minItems") {
    val schema = JsonSchemaDefinition(
      `type` = Some("array"),
      minItems = Some(3)
    )

    val result = JsonSchemaDataValidations.getDataValidations(schema, "test-array")

    val minItemsValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(v => v.expr.contains("SIZE") && v.expr.contains(">="))
    
    assert(minItemsValidation.isDefined)
    assert(minItemsValidation.get.expr == "SIZE(root_array) >= 3")
  }

  test("can handle array constraints with only maxItems") {
    val schema = JsonSchemaDefinition(
      `type` = Some("array"),
      maxItems = Some(5)
    )

    val result = JsonSchemaDataValidations.getDataValidations(schema, "test-array")

    val maxItemsValidation = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .find(v => v.expr.contains("SIZE") && v.expr.contains("<="))
    
    assert(maxItemsValidation.isDefined)
    assert(maxItemsValidation.get.expr == "SIZE(root_array) <= 5")
  }

  test("can handle primitive type at root level") {
    val schema = JsonSchemaDefinition(
      `type` = Some("string"),
      pattern = Some("^test.*"),
      minLength = Some(4),
      maxLength = Some(20)
    )

    val result = JsonSchemaDataValidations.getDataValidations(schema, "test-primitive")

    assert(result.nonEmpty)
    // Should create validations for root_value field
    val validations = result.map(_.validation.asInstanceOf[ExpressionValidation])
    assert(validations.exists(_.expr.contains("root_value")))
  }

  test("handles unsupported format gracefully") {
    val property = JsonSchemaProperty(
      `type` = Some("string"),
      format = Some("unsupported-format")
    )

    val result = JsonSchemaDataValidations.getPropertyValidations("testField", property)

    // Should still have type validation, but no format validation
    assert(result.nonEmpty)
    val formatValidations = result.filter(_.validation.isInstanceOf[ExpressionValidation])
      .map(_.validation.asInstanceOf[ExpressionValidation])
      .filter(_.expr.contains("unsupported-format"))
    
    assert(formatValidations.isEmpty) // No validation for unsupported format
  }

  test("debug - check what validations are created for array with constraints") {
    val schema = JsonSchemaDefinition(
      `type` = Some("array"),
      minItems = Some(2),
      maxItems = Some(10),
      uniqueItems = Some(true)
    )

    val result = JsonSchemaDataValidations.getDataValidations(schema, "test-array")
    
    println(s"Number of validations: ${result.size}")
    result.foreach { vb =>
      val validation = vb.validation
      validation match {
        case ev: ExpressionValidation => println(s"Expression validation: ${ev.expr}")
        case _ => println(s"Other validation: ${validation.getClass.getSimpleName}")
      }
    }
    
    // This test is just for debugging
    assert(result.nonEmpty)
  }
} 