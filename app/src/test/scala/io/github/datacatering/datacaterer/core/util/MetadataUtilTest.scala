package io.github.datacatering.datacaterer.core.util

import org.apache.spark.sql.types.MetadataBuilder
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MetadataUtilTest extends SparkSuite {

  test("Can convert metadata to map") {
    val metadata = new MetadataBuilder()
      .putString("string_key", "value")
      .putLong("long_key", 1L)
      .putDouble("double_key", 0.1)
      .putBoolean("boolean_key", true)
      .putStringArray("array_key", Array("value"))
      .build()

    val result = MetadataUtil.metadataToMap(metadata)

    assert(result.size == 5)
    assert(List("string_key", "long_key", "double_key", "boolean_key", "array_key").forall(result.contains))
  }

}
