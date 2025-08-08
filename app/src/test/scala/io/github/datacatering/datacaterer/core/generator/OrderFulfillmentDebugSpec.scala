package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.{Count, Field, Step}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import net.datafaker.Faker

class OrderFulfillmentDebugSpec extends SparkSuite {
  private val dataGeneratorFactory = new DataGeneratorFactory(new Faker() with Serializable, enableFastGeneration = false)

  test("Debug priority_score vs expedited_shipping") {
    val fields = List(
      Field("order_id", Some("string"), Map("regex" -> "ORD[0-9]{6}")),
      Field("customer_segment", Some("string"), Map("oneOf" -> Array("PREMIUM", "STANDARD", "BASIC"))),
      Field("order_amount", Some("double"), Map("min" -> 100.0, "max" -> 5000.0)),
      Field("base_shipping_days", Some("int"), Map("min" -> 3, "max" -> 10)),
      Field("customer_lifetime_value", Some("double"), Map("sql" -> "CASE WHEN customer_segment = 'PREMIUM' THEN 10000 + RAND() * 40000 WHEN customer_segment = 'STANDARD' THEN 5000 + RAND() * 20000 ELSE 1000 + RAND() * 10000 END")),
      Field("priority_score", Some("double"), Map("sql" -> "PERCENT_RANK() OVER (ORDER BY customer_lifetime_value DESC) * 0.4 + PERCENT_RANK() OVER (ORDER BY order_amount DESC) * 0.6")),
      Field("expedited_shipping", Some("boolean"), Map("sql" -> "priority_score > 0.75"))
    )

    val step = Step("fulfillment_debug", "parquet", Count(records = Some(20)), Map("path" -> "sample/output/parquet/fulfillment_debug"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 20)
    df.select("order_amount", "customer_lifetime_value", "priority_score", "expedited_shipping").show(50, truncate = false)

    val rows = df.collect()
    rows.foreach { r =>
      val ps = r.getAs[Double]("priority_score")
      val ex = r.getAs[Boolean]("expedited_shipping")
      if (ex) assert(ps > 0.75, s"expedited=true but priority_score=$ps")
    }
  }
}

