package io.github.datacatering.datacaterer.core.generator.metadata

import io.github.datacatering.datacaterer.api.model.{Field, Generator, Schema}
import net.datafaker.Faker
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CombinationCalculatorTest extends AnyFunSuite {

  ignore("Can calculate number of combinations given a schema with faker expressions and one of data generators") {
    val schema = Schema(Some(List(
      Field("account_id", Some("string"), Some(Generator())),
      Field("name", Some("string"), Some(Generator("random", Map("expression" -> "#{Name.name}")))),
      Field("status", Some("string"), Some(Generator("oneOf", Map("oneOf" -> List("open", "closed"))))),
    )))
    val faker = new Faker()

    val result = CombinationCalculator.totalCombinationsForSchema(schema, faker)

    assert(result.isDefined)
    assert(result.get == BigInt(103908640))
  }

}
