package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.ArrayType
import io.github.datacatering.datacaterer.core.generator.provider.RandomDataGenerator
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class ArrayHelperMethodsTest extends AnyFunSuite {

  test("arrayFixedSize should set both min and max length to same value") {
    val field = FieldBuilder()
      .name("test_array")
      .`type`(ArrayType)
      .arrayFixedSize(5)
      .field

    assert(field.options.get(ARRAY_MINIMUM_LENGTH).contains("5"))
    assert(field.options.get(ARRAY_MAXIMUM_LENGTH).contains("5"))
  }

  test("arrayUniqueFrom should store comma-separated quoted values") {
    val field = FieldBuilder()
      .name("tags")
      .arrayUniqueFrom("finance", "tech", "healthcare", "retail")
      .field

    val arrayUniqueFrom = field.options.get(ARRAY_UNIQUE_FROM)
    assert(arrayUniqueFrom.isDefined)
    assert(arrayUniqueFrom.get == "'finance','tech','healthcare','retail'")
  }

  test("arrayUniqueFrom should handle numeric values") {
    val field = FieldBuilder()
      .name("numbers")
      .arrayUniqueFrom(1, 2, 3, 4, 5)
      .field

    val arrayUniqueFrom = field.options.get(ARRAY_UNIQUE_FROM)
    assert(arrayUniqueFrom.isDefined)
    assert(arrayUniqueFrom.get == "1,2,3,4,5")
  }

  test("arrayOneOf should store comma-separated quoted values") {
    val field = FieldBuilder()
      .name("event_types")
      .arrayOneOf("LOGIN", "LOGOUT", "VIEW", "CLICK")
      .field

    val arrayOneOf = field.options.get(ARRAY_ONE_OF)
    assert(arrayOneOf.isDefined)
    assert(arrayOneOf.get == "'LOGIN','LOGOUT','VIEW','CLICK'")
  }

  test("arrayOneOf should handle numeric values") {
    val field = FieldBuilder()
      .name("dice_rolls")
      .arrayOneOf(1, 2, 3, 4, 5, 6)
      .field

    val arrayOneOf = field.options.get(ARRAY_ONE_OF)
    assert(arrayOneOf.isDefined)
    assert(arrayOneOf.get == "1,2,3,4,5,6")
  }

  test("arrayEmptyProbability should store probability value") {
    val field = FieldBuilder()
      .name("optional_tags")
      .arrayEmptyProbability(0.3)
      .field

    val emptyProb = field.options.get(ARRAY_EMPTY_PROBABILITY)
    assert(emptyProb.isDefined)
    assert(emptyProb.get == "0.3")
  }

  test("arrayWeightedOneOf should store weighted values in correct format") {
    val field = FieldBuilder()
      .name("priorities")
      .arrayWeightedOneOf(("HIGH", 0.2), ("MEDIUM", 0.5), ("LOW", 0.3))
      .field

    val weightedOneOf = field.options.get(ARRAY_WEIGHTED_ONE_OF)
    assert(weightedOneOf.isDefined)
    assert(weightedOneOf.get == "'HIGH':0.2,'MEDIUM':0.5,'LOW':0.3")
  }

  test("arrayWeightedOneOf should handle numeric values") {
    val field = FieldBuilder()
      .name("http_statuses")
      .arrayWeightedOneOf((200, 0.8), (404, 0.1), (500, 0.1))
      .field

    val weightedOneOf = field.options.get(ARRAY_WEIGHTED_ONE_OF)
    assert(weightedOneOf.isDefined)
    assert(weightedOneOf.get == "200:0.8,404:0.1,500:0.1")
  }

  test("arrayUniqueFrom should generate SQL with SLICE and SHUFFLE") {
    val metadata = new MetadataBuilder()
      .putString(ARRAY_UNIQUE_FROM, "'A','B','C','D','E'")
      .putLong(ARRAY_MINIMUM_LENGTH, 2)
      .putLong(ARRAY_MAXIMUM_LENGTH, 4)
      .build()

    val structField = StructField("tags", org.apache.spark.sql.types.ArrayType(StringType), nullable = false, metadata)
    val generator = new RandomDataGenerator.RandomArrayDataGenerator(structField, StringType)

    val sqlExpr = generator.generateSqlExpression
    assert(sqlExpr.contains("SLICE"))
    assert(sqlExpr.contains("SHUFFLE"))
    assert(sqlExpr.contains("ARRAY('A','B','C','D','E')"))
  }

  test("arrayOneOf should generate SQL with TRANSFORM and ELEMENT_AT") {
    val metadata = new MetadataBuilder()
      .putString(ARRAY_ONE_OF, "'LOGIN','LOGOUT','VIEW'")
      .putLong(ARRAY_MINIMUM_LENGTH, 5)
      .putLong(ARRAY_MAXIMUM_LENGTH, 10)
      .build()

    val structField = StructField("events", org.apache.spark.sql.types.ArrayType(StringType), nullable = false, metadata)
    val generator = new RandomDataGenerator.RandomArrayDataGenerator(structField, StringType)

    val sqlExpr = generator.generateSqlExpression
    assert(sqlExpr.contains("TRANSFORM"))
    assert(sqlExpr.contains("ELEMENT_AT"))
    assert(sqlExpr.contains("ARRAY('LOGIN','LOGOUT','VIEW')"))
  }

  test("arrayEmptyProbability should wrap array expression in CASE statement") {
    val metadata = new MetadataBuilder()
      .putString(ARRAY_EMPTY_PROBABILITY, "0.3")
      .putLong(ARRAY_MINIMUM_LENGTH, 1)
      .putLong(ARRAY_MAXIMUM_LENGTH, 5)
      .build()

    val structField = StructField("optional_items", org.apache.spark.sql.types.ArrayType(StringType), nullable = false, metadata)
    val generator = new RandomDataGenerator.RandomArrayDataGenerator(structField, StringType)

    val sqlExpr = generator.generateSqlExpression
    assert(sqlExpr.contains("CASE WHEN"))
    assert(sqlExpr.contains("< 0.3"))
    assert(sqlExpr.contains("THEN ARRAY()"))
  }

  test("arrayEmptyProbability should work with arrayUniqueFrom") {
    val metadata = new MetadataBuilder()
      .putString(ARRAY_UNIQUE_FROM, "'X','Y','Z'")
      .putString(ARRAY_EMPTY_PROBABILITY, "0.2")
      .putLong(ARRAY_MINIMUM_LENGTH, 1)
      .putLong(ARRAY_MAXIMUM_LENGTH, 2)
      .build()

    val structField = StructField("tags", org.apache.spark.sql.types.ArrayType(StringType), nullable = false, metadata)
    val generator = new RandomDataGenerator.RandomArrayDataGenerator(structField, StringType)

    val sqlExpr = generator.generateSqlExpression
    assert(sqlExpr.contains("CASE WHEN"))
    assert(sqlExpr.contains("SLICE"))
    assert(sqlExpr.contains("SHUFFLE"))
    assert(sqlExpr.contains("ARRAY()"))
  }

  test("arrayWeightedOneOf should generate SQL with weighted aggregation") {
    val metadata = new MetadataBuilder()
      .putString(ARRAY_WEIGHTED_ONE_OF, "'HIGH':0.2,'MEDIUM':0.5,'LOW':0.3")
      .putLong(ARRAY_MINIMUM_LENGTH, 1)
      .putLong(ARRAY_MAXIMUM_LENGTH, 3)
      .build()

    val structField = StructField("priorities", org.apache.spark.sql.types.ArrayType(StringType), nullable = false, metadata)
    val generator = new RandomDataGenerator.RandomArrayDataGenerator(structField, StringType)

    val sqlExpr = generator.generateSqlExpression
    assert(sqlExpr.contains("AGGREGATE"))
    assert(sqlExpr.contains("ZIP_WITH"))
    assert(sqlExpr.contains("named_struct"))
  }

  test("arrayWeightedOneOf should handle quoted values with commas and colons") {
    val metadata = new MetadataBuilder()
      .putString(ARRAY_WEIGHTED_ONE_OF, "'10:30':0.5,'hello, world':0.5")
      .putLong(ARRAY_MINIMUM_LENGTH, 1)
      .putLong(ARRAY_MAXIMUM_LENGTH, 2)
      .build()

    val structField = StructField("times", org.apache.spark.sql.types.ArrayType(StringType), nullable = false, metadata)
    val generator = new RandomDataGenerator.RandomArrayDataGenerator(structField, StringType)

    val sqlExpr = generator.generateSqlExpression
    assert(sqlExpr.contains("'10:30'"))
    assert(sqlExpr.contains("'hello, world'"))
  }

  test("array helper methods should be chainable") {
    val field = FieldBuilder()
      .name("tags")
      .arrayUniqueFrom("A", "B", "C", "D")
      .arrayMinLength(2)
      .arrayMaxLength(3)
      .arrayEmptyProbability(0.1)
      .field

    assert(field.options.get(ARRAY_UNIQUE_FROM).isDefined)
    assert(field.options.get(ARRAY_MINIMUM_LENGTH).contains("2"))
    assert(field.options.get(ARRAY_MAXIMUM_LENGTH).contains("3"))
    assert(field.options.get(ARRAY_EMPTY_PROBABILITY).contains("0.1"))
  }

  test("default array generation should still work without helper methods") {
    val metadata = new MetadataBuilder()
      .putLong(ARRAY_MINIMUM_LENGTH, 1)
      .putLong(ARRAY_MAXIMUM_LENGTH, 5)
      .build()

    val structField = StructField("normal_array", org.apache.spark.sql.types.ArrayType(IntegerType), nullable = false, metadata)
    val generator = new RandomDataGenerator.RandomArrayDataGenerator(structField, IntegerType)

    val sqlExpr = generator.generateSqlExpression
    assert(sqlExpr.contains("TRANSFORM"))
    assert(sqlExpr.contains("ARRAY_REPEAT"))
    assert(!sqlExpr.contains("SLICE"))
    assert(!sqlExpr.contains("SHUFFLE"))
  }

  test("arrayFixedSize with arrayUniqueFrom should work correctly") {
    val field = FieldBuilder()
      .name("permissions")
      .arrayUniqueFrom("READ", "WRITE", "DELETE", "ADMIN")
      .arrayFixedSize(2)
      .field

    assert(field.options.get(ARRAY_UNIQUE_FROM).isDefined)
    assert(field.options.get(ARRAY_MINIMUM_LENGTH).contains("2"))
    assert(field.options.get(ARRAY_MAXIMUM_LENGTH).contains("2"))
  }

  test("arrayFixedSize with arrayOneOf should work correctly") {
    val field = FieldBuilder()
      .name("dice_rolls")
      .arrayOneOf(1, 2, 3, 4, 5, 6)
      .arrayFixedSize(10)
      .field

    assert(field.options.get(ARRAY_ONE_OF).isDefined)
    assert(field.options.get(ARRAY_MINIMUM_LENGTH).contains("10"))
    assert(field.options.get(ARRAY_MAXIMUM_LENGTH).contains("10"))
  }

  test("Java varargs should work for arrayUniqueFrom") {
    // Simulate Java varargs call
    val field = FieldBuilder()
      .name("java_tags")
      .arrayUniqueFrom("tag1", "tag2", "tag3")

    assert(field.field.options.get(ARRAY_UNIQUE_FROM).isDefined)
  }

  test("Java varargs should work for arrayOneOf") {
    // Simulate Java varargs call
    val field = FieldBuilder()
      .name("java_events")
      .arrayOneOf("EVENT1", "EVENT2", "EVENT3")

    assert(field.field.options.get(ARRAY_ONE_OF).isDefined)
  }

  test("Java varargs should work for arrayWeightedOneOf") {
    // Simulate Java varargs call
    val field = FieldBuilder()
      .name("java_priorities")
      .arrayWeightedOneOf(("P1", 0.5), ("P2", 0.5))

    assert(field.field.options.get(ARRAY_WEIGHTED_ONE_OF).isDefined)
  }
}
