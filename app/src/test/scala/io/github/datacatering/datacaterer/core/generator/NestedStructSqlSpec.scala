package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.{Count, Field, Step}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import net.datafaker.Faker

class NestedStructSqlSpec extends SparkSuite {
  private val dataGeneratorFactory = new DataGeneratorFactory(new Faker() with Serializable, enableFastGeneration = false)

  test("Two-level nested struct SQL referencing top-level scalar works") {
    val fields = List(
      Field("id"),
      Field("tmp_account_id", Some("string"), Map("regex" -> "ACC[0-9]{8}", "omit" -> "true")),
      Field("details", Some("struct"), fields = List(
        Field("account_id", Some("string"), Map("sql" -> "tmp_account_id"))
      ))
    )
    val step = Step("nested_struct", "memory", Count(records = Some(5)), Map.empty, fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "memory", 0, 5)
    assert(df.columns.contains("details"))
    val child = df.selectExpr("details.account_id as account_id").collect().map(_.getString(0))
    assert(child.forall(id => id != null && id.startsWith("ACC")))
  }
}

