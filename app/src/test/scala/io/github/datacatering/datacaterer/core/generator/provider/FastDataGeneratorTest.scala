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
    
    // Validate SQL uses standard functions
    assert(sqlExpression.contains("CONCAT_WS"), 
      "SQL expression should use CONCAT_WS for string construction")
    assert(sqlExpression.contains("TRANSFORM"), 
      "SQL expression should use TRANSFORM for array operations")
    assert(sqlExpression.contains("SEQUENCE"), 
      "SQL expression should use SEQUENCE for repetition")
    
    println(s"FastStringDataGenerator SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should handle common email patterns") {
    val metadata = new MetadataBuilder()
      .putString(REGEX_GENERATOR, "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
      .build()
    val structField = StructField("email", StringType, false, metadata)
    val generator = new FastRegexDataGenerator(structField, new Faker())

    // Test generation
    val generated = generator.generate
    assert(generated.contains("@"), "Generated email should contain @")
    assert(generated.endsWith("example.com"), "Generated email should end with example.com")
    println(s"Generated email: $generated")

    // Test SQL expression
    val sqlExpression = generator.generateSqlExpression
    assert(!sqlExpression.contains("GENERATE_REGEX_UDF"), 
      "SQL expression should not contain regex UDF")
    assert(sqlExpression.contains("CONCAT"), 
      "SQL expression should use CONCAT for email construction")
    println(s"Email SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should handle phone number patterns") {
    val metadata = new MetadataBuilder()
      .putString(REGEX_GENERATOR, "\\d{3}-\\d{3}-\\d{4}")
      .build()
    val structField = StructField("phone", StringType, false, metadata)
    val generator = new FastRegexDataGenerator(structField, new Faker())

    // Test generation
    val generated = generator.generate
    assert(generated.matches("\\d{3}-\\d{3}-\\d{4}"), 
      s"Generated phone should match pattern: $generated")
    println(s"Generated phone: $generated")

    // Test SQL expression
    val sqlExpression = generator.generateSqlExpression
    assert(!sqlExpression.contains("GENERATE_REGEX_UDF"), 
      "SQL expression should not contain regex UDF")
    assert(sqlExpression.contains("LPAD"), 
      "SQL expression should use LPAD for number formatting")
    println(s"Phone SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should handle UUID patterns") {
    val metadata = new MetadataBuilder()
      .putString(REGEX_GENERATOR, "uuid")
      .build()
    val structField = StructField("uuid", StringType, false, metadata)
    val generator = new FastRegexDataGenerator(structField, new Faker())

    // Test generation
    val generated = generator.generate
    assert(generated.contains("-"), "Generated UUID should contain dashes")
    assert(generated.length == 36, s"Generated UUID should be 36 characters: $generated")
    println(s"Generated UUID: $generated")

    // Test SQL expression
    val sqlExpression = generator.generateSqlExpression
    assert(!sqlExpression.contains("GENERATE_REGEX_UDF"), 
      "SQL expression should not contain regex UDF")
    assert(sqlExpression.contains("HEX"), 
      "SQL expression should use HEX for UUID generation")
    println(s"UUID SQL: $sqlExpression")
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
      "SQL expression should not contain regex UDF")
    assert(sqlExpression.contains("SUBSTR"), 
      "SQL expression should use SUBSTR for character selection")
    println(s"Code SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should handle number patterns") {
    val metadata = new MetadataBuilder()
      .putString(REGEX_GENERATOR, "\\d{6}")
      .build()
    val structField = StructField("number", StringType, false, metadata)
    val generator = new FastRegexDataGenerator(structField, new Faker())

    // Test generation
    val generated = generator.generate
    assert(generated.length == 6, s"Generated number should be 6 digits: $generated")
    assert(generated.matches("\\d{6}"), 
      s"Generated number should be all digits: $generated")
    println(s"Generated number: $generated")

    // Test SQL expression
    val sqlExpression = generator.generateSqlExpression
    assert(!sqlExpression.contains("GENERATE_REGEX_UDF"), 
      "SQL expression should not contain regex UDF")
    assert(sqlExpression.contains("LPAD"), 
      "SQL expression should use LPAD for number formatting")
    println(s"Number SQL: $sqlExpression")
  }

  test("FastRegexDataGenerator should extract length from patterns correctly") {
    val metadata1 = new MetadataBuilder().putString(REGEX_GENERATOR, "[A-Z]{5}").build()
    val generator1 = new FastRegexDataGenerator(StructField("test1", StringType, false, metadata1), new Faker())
    
    val metadata2 = new MetadataBuilder().putString(REGEX_GENERATOR, "[A-Z]{3,8}").build()
    val generator2 = new FastRegexDataGenerator(StructField("test2", StringType, false, metadata2), new Faker())
    
    val metadata3 = new MetadataBuilder().putString(REGEX_GENERATOR, "[A-Z]+").build()
    val generator3 = new FastRegexDataGenerator(StructField("test3", StringType, false, metadata3), new Faker())

    // Test length extraction through generation
    val gen1 = generator1.generate
    assert(gen1.length == 5, s"Should generate exactly 5 characters: $gen1")
    
    val gen2 = generator2.generate
    assert(gen2.length >= 3 && gen2.length <= 8, s"Should generate 3-8 characters: $gen2")
    
    val gen3 = generator3.generate
    assert(gen3.length >= 1, s"Should generate at least 1 character: $gen3")
    
    println(s"Length pattern tests: $gen1, $gen2, $gen3")
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