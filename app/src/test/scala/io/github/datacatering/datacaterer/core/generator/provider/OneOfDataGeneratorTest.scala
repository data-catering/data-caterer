package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants.ONE_OF_GENERATOR
import io.github.datacatering.datacaterer.core.generator.provider.OneOfDataGenerator.RandomOneOfDataGenerator
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OneOfDataGeneratorTest extends AnyFunSuite {

  private val oneOfArray = Array("started", "in-progress", "finished", "failed", "restarted", "paused")

  test("Can generate data based on one-of generator") {
    val metadata = new MetadataBuilder()
      .putStringArray(ONE_OF_GENERATOR, oneOfArray)
      .build()
    val oneOfDataGenerator = new RandomOneOfDataGenerator(StructField("random_one_of", StringType, false, metadata))

    (1 to 20).foreach(_ => {
      val data = oneOfDataGenerator.generate
      assert(data.isInstanceOf[String])
      assert(oneOfArray.contains(data))
    })
  }

  test("Will default to use string type when no array type defined") {
    val metadata = new MetadataBuilder()
      .putStringArray(ONE_OF_GENERATOR, oneOfArray)
      .build()
    val oneOfDataGenerator = new RandomOneOfDataGenerator(StructField("random_one_of", StringType, false, metadata))

    (1 to 20).foreach(_ => {
      val data = oneOfDataGenerator.generate
      assert(data.isInstanceOf[String])
      assert(oneOfArray.contains(data))
    })
  }

  test("Will throw an exception if no oneOf is defined in metadata") {
    val metadata = new MetadataBuilder().build()
    assertThrows[AssertionError](new RandomOneOfDataGenerator(StructField("random_one_of", StringType, false, metadata)))
  }
}
