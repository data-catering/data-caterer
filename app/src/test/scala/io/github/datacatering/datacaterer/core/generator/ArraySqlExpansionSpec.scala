package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.{Count, Field, Step}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import net.datafaker.Faker
import org.apache.spark.sql.Row

class ArraySqlExpansionSpec extends SparkSuite {

  private val dataGeneratorFactory = new DataGeneratorFactory(new Faker() with Serializable, enableFastGeneration = false)

  test("In-array element SQL expansion works for calculated fields") {
    val fields = List(
      Field("orders", Some("array"), fields = List(
        Field("amount", Some("double")),
        Field("tax", Some("double"), Map("sql" -> "orders.amount * 0.1")),
        Field("total", Some("double"), Map("sql" -> "orders.amount + orders.tax"))
      ))
    )

    val step = Step("orders_test", "memory", Count(records = Some(3)), Map.empty, fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "memory", 0, 3)
    val rows = df.collect()
    rows.foreach { row =>
      val arr = row.getAs[Seq[Row]]("orders")
      if (arr != null && arr.nonEmpty) {
        val first = arr.head
        val amount = first.getAs[Double]("amount")
        val tax = first.getAs[Double]("tax")
        val total = first.getAs[Double]("total")
        assert(math.abs(tax - (amount * 0.1)) < 1e-6)
        assert(math.abs(total - (amount + tax)) < 1e-6)
      }
    }
  }
}

