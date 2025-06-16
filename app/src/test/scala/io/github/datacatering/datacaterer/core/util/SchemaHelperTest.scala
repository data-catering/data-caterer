package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.model.IntegerType
import io.github.datacatering.datacaterer.core.util.PlanImplicits.FieldOps
import org.scalatest.funsuite.AnyFunSuiteLike

class SchemaHelperTest extends AnyFunSuiteLike {

  test("Can convert field to StructField") {
    val field = FieldBuilder().name("age").`type`(IntegerType).min(1).max(100).field

    val result = field.toStructField

    val metadata = result.metadata
    assertResult("1")(metadata.getString("min"))
    assertResult("100")(metadata.getString("max"))
  }

  test("Can convert field with manual options to StructField") {
    val field = FieldBuilder().name("age").`type`(IntegerType)
      .options(Map(
        "min" -> 1,
        "max" -> 100
      )).field

    val result = field.toStructField

    val metadata = result.metadata
    assertResult("1")(metadata.getString("min"))
    assertResult("100")(metadata.getString("max"))
  }

  test("Can convert field with manual oneOf option to StructField") {
    val field = FieldBuilder().name("age").`type`(IntegerType)
      .options(Map(
        "oneOf" -> List(1, 5, 10)
      )).field

    val result = field.toStructField

    val metadata = result.metadata
    assertResult(List("1", "5", "10"))(metadata.getStringArray("oneOf"))
  }
}
