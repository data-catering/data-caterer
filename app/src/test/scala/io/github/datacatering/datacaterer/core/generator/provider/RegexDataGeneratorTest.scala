package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants.REGEX_GENERATOR
import io.github.datacatering.datacaterer.core.exception.InvalidDataGeneratorConfigurationException
import io.github.datacatering.datacaterer.core.generator.provider.RegexDataGenerator.RandomRegexDataGenerator
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RegexDataGeneratorTest extends AnyFunSuite {

  test("Can generate data based on regex") {
    val regex = "ACC100[0-9]{5}"
    val metadata = new MetadataBuilder().putString(REGEX_GENERATOR, regex).build()
    val regexDataGenerator = new RandomRegexDataGenerator(StructField("random_regex", StringType, false, metadata))

    assert(regexDataGenerator.edgeCases.isEmpty)
    (1 to 10).foreach(_ => {
      val data = regexDataGenerator.generate
      assert(data.length == 11)
      assert(data.startsWith("ACC100"))
      assert(data.matches(regex))
    })
  }

  test("Throws exception when no regex is defined") {
    val metadata = new MetadataBuilder().build()
    assertThrows[InvalidDataGeneratorConfigurationException](new RandomRegexDataGenerator(StructField("random_regex", StringType, false, metadata)))
  }
}
