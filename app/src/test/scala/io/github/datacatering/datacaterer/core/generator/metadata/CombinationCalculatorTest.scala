package io.github.datacatering.datacaterer.core.generator.metadata

import io.github.datacatering.datacaterer.api.model.Field
import net.datafaker.Faker
import org.scalatest.funsuite.AnyFunSuite

class CombinationCalculatorTest extends AnyFunSuite {

  ignore("Can calculate number of combinations given a schema with faker expressions and one of data generators") {
    val schema = List(
      Field("account_id", Some("string")),
      Field("name", Some("string"), Map("expression" -> "#{Name.name}")),
      Field("status", Some("string"), Map("oneOf" -> List("open", "closed"))),
    )
    val faker = new Faker()

    val result = CombinationCalculator.totalCombinationsForSchema(schema, faker)

    assertResult(BigInt(103908640))(result)
  }

}
