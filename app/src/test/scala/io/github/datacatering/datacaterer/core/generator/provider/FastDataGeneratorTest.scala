package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants.{MAXIMUM_LENGTH, MINIMUM_LENGTH, REGEX_GENERATOR}
import io.github.datacatering.datacaterer.core.generator.provider.FastDataGenerator.{FastRegexDataGenerator, FastStringDataGenerator}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import net.datafaker.Faker
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField}
import org.scalatest.funsuite.AnyFunSuite

class FastDataGeneratorTest extends AnyFunSuite with SparkSuite {

  test("FastStringDataGenerator should generate strings within specified length range") {
    val metadata = new MetadataBuilder()
      .putString(MINIMUM_LENGTH, "5")
      .putString(MAXIMUM_LENGTH, "10")
      .build()
    val structField = StructField("test_string", StringType, false, metadata)
    val generator = new FastStringDataGenerator(structField, new Faker())

    // Test generation
    for (_ <- 1 to 100) {
      val generated = generator.generate
      assert(generated.length >= 5 && generated.length <= 10, 
        s"Generated string length ${generated.length} should be between 5 and 10: $generated")
      assert(generated.matches("[A-Za-z0-9]*"), 
        s"Generated string should be alphanumeric: $generated")
    }
  }

  test("FastStringDataGenerator should generate SQL without UDF calls") {
    val metadata = new MetadataBuilder()
      .putString(MINIMUM_LENGTH, "3")
      .putString(MAXIMUM_LENGTH, "8")
      .build()
    val structField = StructField("test_string", StringType, false, metadata)
    val generator = new FastStringDataGenerator(structField, new Faker())

    val sqlExpression = generator.generateSqlExpression

    // Validate no UDF calls are present
    assert(!sqlExpression.contains("GENERATE_FAKER_EXPRESSION_UDF"),
      "SQL expression should not contain UDF calls")
    assert(!sqlExpression.contains("GENERATE_REGEX_UDF"),
      "SQL expression should not contain UDF calls")
    assert(!sqlExpression.contains("GENERATE_RANDOM_ALPHANUMERIC_STRING_UDF"),
      "SQL expression should not contain UDF calls")

    // Validate SQL uses MD5-based generation for performance
    assert(sqlExpression.contains("MD5"),
      "SQL expression should use MD5 for fast string generation")
    assert(sqlExpression.contains("SUBSTRING"),
      "SQL expression should use SUBSTRING for length control")
    assert(sqlExpression.contains("CONCAT"),
      "SQL expression should use CONCAT for seed generation")

    println(s"FastStringDataGenerator SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should handle account ID patterns") {
    val metadata = new MetadataBuilder()
      .putString(REGEX_GENERATOR, "ACC[0-9]{8}")
      .build()
    val structField = StructField("account_id", StringType, false, metadata)
    val generator = new FastRegexDataGenerator(structField, new Faker())

    // Test generation (uses faker.regexify for accuracy)
    val generated = generator.generate
    assert(generated.startsWith("ACC"), s"Generated account ID should start with ACC: $generated")
    assert(generated.matches("ACC[0-9]{8}"), s"Generated account ID should match pattern: $generated")
    println(s"Generated account ID: $generated")

    // Test SQL expression (uses parser for pure SQL)
    val sqlExpression = generator.generateSqlExpression
    assert(!sqlExpression.contains("GENERATE_REGEX_UDF"),
      "SQL expression should not contain regex UDF for parseable pattern")
    assert(sqlExpression.contains("CONCAT"),
      "SQL expression should use CONCAT")
    assert(sqlExpression.contains("'ACC'"),
      "SQL expression should include ACC literal")
    assert(sqlExpression.contains("LPAD"),
      "SQL expression should use LPAD for digits")
    println(s"Account ID SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should handle simple digit patterns") {
    val metadata = new MetadataBuilder()
      .putString(REGEX_GENERATOR, "\\d{6}")
      .build()
    val structField = StructField("code", StringType, false, metadata)
    val generator = new FastRegexDataGenerator(structField, new Faker())

    // Test generation
    val generated = generator.generate
    assert(generated.matches("\\d{6}"),
      s"Generated code should match pattern: $generated")
    println(s"Generated code: $generated")

    // Test SQL expression
    val sqlExpression = generator.generateSqlExpression
    assert(!sqlExpression.contains("GENERATE_REGEX_UDF"),
      "SQL expression should not contain regex UDF for parseable pattern")
    assert(sqlExpression.contains("LPAD"),
      "SQL expression should use LPAD for digit formatting")
    println(s"Code SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should handle mixed patterns") {
    val metadata = new MetadataBuilder()
      .putString(REGEX_GENERATOR, "[A-Z]{3}-[0-9]{2}")
      .build()
    val structField = StructField("code", StringType, false, metadata)
    val generator = new FastRegexDataGenerator(structField, new Faker())

    // Test generation
    val generated = generator.generate
    assert(generated.matches("[A-Z]{3}-[0-9]{2}"),
      s"Generated code should match pattern: $generated")
    println(s"Generated mixed code: $generated")

    // Test SQL expression
    val sqlExpression = generator.generateSqlExpression
    assert(!sqlExpression.contains("GENERATE_REGEX_UDF"),
      "SQL expression should not contain regex UDF for parseable pattern")
    assert(sqlExpression.contains("CONCAT"),
      "SQL expression should use CONCAT")
    assert(sqlExpression.contains("'-'"),
      "SQL expression should include dash literal")
    println(s"Mixed code SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should handle alphanumeric patterns with length") {
    val metadata = new MetadataBuilder()
      .putString(REGEX_GENERATOR, "[A-Z0-9]{8}")
      .build()
    val structField = StructField("code", StringType, false, metadata)
    val generator = new FastRegexDataGenerator(structField, new Faker())

    // Test generation
    val generated = generator.generate
    assert(generated.length == 8, s"Generated code should be 8 characters: $generated")
    assert(generated.matches("[A-Z0-9]+"),
      s"Generated code should be alphanumeric uppercase: $generated")
    println(s"Generated code: $generated")

    // Test SQL expression
    val sqlExpression = generator.generateSqlExpression
    assert(!sqlExpression.contains("GENERATE_REGEX_UDF"),
      "SQL expression should not contain regex UDF for parseable pattern")
    assert(sqlExpression.contains("SUBSTRING") || sqlExpression.contains("CONCAT_WS"),
      "SQL expression should use SUBSTRING or CONCAT_WS for character selection")
    println(s"Code SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should handle alternation patterns") {
    val metadata = new MetadataBuilder()
      .putString(REGEX_GENERATOR, "(ACTIVE|INACTIVE|PENDING)")
      .build()
    val structField = StructField("status", StringType, false, metadata)
    val generator = new FastRegexDataGenerator(structField, new Faker())

    // Test generation
    val generated = generator.generate
    assert(List("ACTIVE", "INACTIVE", "PENDING").contains(generated),
      s"Generated status should be one of the options: $generated")
    println(s"Generated status: $generated")

    // Test SQL expression
    val sqlExpression = generator.generateSqlExpression
    assert(!sqlExpression.contains("GENERATE_REGEX_UDF"),
      "SQL expression should not contain regex UDF for parseable pattern")
    assert(sqlExpression.contains("ELEMENT_AT") && sqlExpression.contains("ARRAY"),
      "SQL expression should use ELEMENT_AT and ARRAY for alternation")
    println(s"Status SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should parse quantifiers correctly") {
    val metadata1 = new MetadataBuilder().putString(REGEX_GENERATOR, "[A-Z]{5}").build()
    val generator1 = new FastRegexDataGenerator(StructField("test1", StringType, false, metadata1), new Faker())

    val metadata2 = new MetadataBuilder().putString(REGEX_GENERATOR, "[A-Z]{3,8}").build()
    val generator2 = new FastRegexDataGenerator(StructField("test2", StringType, false, metadata2), new Faker())

    val metadata3 = new MetadataBuilder().putString(REGEX_GENERATOR, "\\d+").build()
    val generator3 = new FastRegexDataGenerator(StructField("test3", StringType, false, metadata3), new Faker())

    // Test exact quantifier
    val gen1 = generator1.generate
    assert(gen1.length == 5, s"Should generate exactly 5 characters: $gen1")

    // Test range quantifier
    val gen2 = generator2.generate
    assert(gen2.length >= 3 && gen2.length <= 8, s"Should generate 3-8 characters: $gen2")

    // Test plus quantifier (bounded to 1-5 in parser)
    val gen3 = generator3.generate
    assert(gen3.length >= 1, s"Should generate at least 1 digit: $gen3")

    println(s"Quantifier pattern tests: $gen1, $gen2, $gen3")
  }

  test("FastRegexDataGenerator should fall back to UDF for unsupported patterns") {
    // Lookahead is not supported by parser
    val metadata = new MetadataBuilder()
      .putString(REGEX_GENERATOR, "(?=complex)pattern")
      .build()
    val structField = StructField("complex", StringType, false, metadata)
    val generator = new FastRegexDataGenerator(structField, new Faker())

    // SQL should fall back to UDF
    val sqlExpression = generator.generateSqlExpression
    assert(sqlExpression.contains("GENERATE_REGEX"),
      s"SQL expression should fall back to UDF for unsupported pattern: $sqlExpression")
    println(s"Fallback SQL: $sqlExpression")
  }

  test("FastDataGenerators should handle edge cases gracefully") {
    // Test minimum length equals maximum length
    val metadata1 = new MetadataBuilder()
      .putString(MINIMUM_LENGTH, "5")
      .putString(MAXIMUM_LENGTH, "5")
      .build()
    val generator1 = new FastStringDataGenerator(StructField("fixed_length", StringType, false, metadata1), new Faker())
    val generated1 = generator1.generate
    assert(generated1.length == 5, s"Fixed length should be exactly 5: $generated1")

    // Test with minimum values
    val metadata2 = new MetadataBuilder()
      .putString(MINIMUM_LENGTH, "1")
      .putString(MAXIMUM_LENGTH, "1")
      .build()
    val generator2 = new FastStringDataGenerator(StructField("single_char", StringType, false, metadata2), new Faker())
    val generated2 = generator2.generate
    assert(generated2.length == 1, s"Single character should be exactly 1: $generated2")

    println(s"Edge case tests: $generated1, $generated2")
  }
} 