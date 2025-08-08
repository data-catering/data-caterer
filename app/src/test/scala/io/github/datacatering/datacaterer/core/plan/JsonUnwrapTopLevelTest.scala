package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.ArrayType
import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.core.util.{ObjectMapperUtil, SparkSuite}
import org.apache.spark.sql.Row

class JsonUnwrapTopLevelTest extends SparkSuite {

  test("Unwrap top-level array outputs a bare JSON array") {
    class TestUnwrapTopLevelArray extends PlanRun {
      val jsonTask = json("unwrap_array_json", "/tmp/json/unwrap-array", Map("saveMode" -> "overwrite", "numPartitions" -> "1"))
        .fields(
          field.name("records").`type`(ArrayType)
            .arrayMinLength(3)
            .arrayMaxLength(3)
            .unwrapTopLevelArray(true)
            .fields(
              field.name("id").regex("ID[0-9]{3}"),
              field.name("name").expression("#{Name.firstName}")
            )
        )
        .count(count.records(1))

      execute(jsonTask)
    }

    PlanProcessor.determineAndExecutePlan(Some(new TestUnwrapTopLevelArray()))

    val written = sparkSession.read.text("/tmp/json/unwrap-array").collect().map(_.getString(0))
    assert(written.nonEmpty, "Expected a single JSON array output line")
    val jsonArrayStr = written.head
    assert(jsonArrayStr.trim.startsWith("["), s"Expected top-level JSON array, got: ${jsonArrayStr.take(100)}...")

    val node = ObjectMapperUtil.jsonObjectMapper.readTree(jsonArrayStr)
    assert(node.isArray, "Parsed JSON should be an array")
    assert(node.size() == 3, s"Expected 3 elements, got ${node.size()}")
    assert(node.get(0).has("id"))
    assert(node.get(0).has("name"))
  }

  test("Default JSON remains object when unwrap not enabled") {
    class TestKeepObject extends PlanRun {
      val jsonTask = json("keep_object_json", "/tmp/json/keep-object", Map("saveMode" -> "overwrite", "numPartitions" -> "1"))
        .fields(
          field.name("records").`type`(ArrayType)
            .arrayMinLength(2)
            .arrayMaxLength(2)
            .fields(
              field.name("id").regex("ID[0-9]{2}")
            )
        )
        .count(count.records(1))

      execute(jsonTask)
    }

    PlanProcessor.determineAndExecutePlan(Some(new TestKeepObject()))
    val df = sparkSession.read.json("/tmp/json/keep-object")
    assert(df.columns.contains("records"))
    val first = df.collect().head
    val arr = first.getAs[Seq[Row]]("records")
    assert(arr.size == 2)
  }

  test("Unwrap is ignored when more than one top-level field exists") {
    class TestMultipleTopLevel extends PlanRun {
      val jsonTask = json("multi_top_json", "/tmp/json/multi-top", Map("saveMode" -> "overwrite", "numPartitions" -> "1"))
        .fields(
          field.name("records").`type`(ArrayType)
            .arrayMinLength(1)
            .arrayMaxLength(1)
            .unwrapTopLevelArray(true)
            .fields(
              field.name("id").regex("ID[0-9]{1}")
            ),
          field.name("extra").static("x")
        )
        .count(count.records(1))

      execute(jsonTask)
    }

    PlanProcessor.determineAndExecutePlan(Some(new TestMultipleTopLevel()))
    val df = sparkSession.read.json("/tmp/json/multi-top")
    assert(df.columns.toSet == Set("records", "extra"))
  }
}



