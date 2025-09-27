package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.{Count, Field, Step}
import io.github.datacatering.datacaterer.core.ui.model.SchemaSampleRequest
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

class SimpleSampleTest extends SparkSuite with Matchers with BeforeAndAfterEach {

  test("FastSampleGenerator generate basic sample data") {
    val step = Step(
      name = "simple_test",
      `type` = "json",
      count = Count(records = Some(2)),
      fields = List(
        Field(
          name = "id",
          `type` = Some("long"),
          options = Map("min" -> 1L, "max" -> 100L)
        ),
        Field(
          name = "name",
          `type` = Some("string"),
          options = Map("static" -> "test_name")
        )
      )
    )

    val request = SchemaSampleRequest(
      fields = step.fields,
      sampleSize = 2,
      fastMode = true
    )

    val result = FastSampleGenerator.generateFromSchema(request)

    if (!result.success) {
      println(s"Generation failed: ${result.error}")
    }
    
    result.success shouldBe true
    result.executionId should not be empty
    result.sampleData shouldBe defined
    result.sampleData.get should have size 2
    result.metadata shouldBe defined

    println(s"Sample data generated: ${result.sampleData.get}")
    
    val metadata = result.metadata.get
    metadata.actualRecords shouldBe 2
    metadata.sampleSize shouldBe 2
    metadata.fastModeEnabled shouldBe true
  }
}