package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.core.ui.model.TaskYamlSampleRequest
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

class TaskYamlCustomUnmarshallerTest extends SparkSuite with Matchers with BeforeAndAfterEach {

  test("FastSampleGenerator generate sample data from YAML content using custom unmarshaller approach") {
    val yamlContent = """name: test_task
steps:
  - name: test_step
    type: file
    options:
      path: /tmp/test
      format: json
    count:
      records: 100
    fields:
      - name: id
        type: long
        options:
          min: 1
          max: 1000
      - name: name
        type: string
        options:
          expression: "#{Name.fullName}"
      - name: age
        type: int
        options:
          min: 18
          max: 80"""

    val request = TaskYamlSampleRequest(
      taskYamlContent = yamlContent,
      stepName = Some("test_step"),
      sampleSize = 5,
      fastMode = true
    )

    val result = FastSampleGenerator.generateFromTaskYaml(request)

    result.success shouldBe true
    result.sampleData shouldBe defined
    result.sampleData.get should have size 5
    result.schema shouldBe defined

    // Verify schema structure
    val schema = result.schema.get
    schema.fields should have size 3
    schema.fields.map(_.name) should contain allOf("id", "name", "age")

    // Verify sample data contains expected fields
    val sampleData = result.sampleData.get
    sampleData.foreach { record =>
      record should contain key "id"
      record should contain key "name" 
      record should contain key "age"
      
      // Verify data types and constraints
      record("id").toString.toLong should (be >= 1L and be <= 1000L)
      record("name") shouldBe a[String]
      record("age").toString.toInt should (be >= 18 and be <= 80)
    }

    // Verify metadata
    val metadata = result.metadata.get
    metadata.sampleSize shouldBe 5
    metadata.actualRecords shouldBe 5
    metadata.fastModeEnabled shouldBe true
    metadata.generatedInMs should be > 0L
  }

  test("FastSampleGenerator handle YAML with default step when no step name specified") {
    val yamlContent = """name: test_task
steps:
  - name: default_step
    type: file
    fields:
      - name: simple_id
        type: long
        options:
          min: 1
          max: 10"""

    val request = TaskYamlSampleRequest(
      taskYamlContent = yamlContent,
      stepName = None,
      sampleSize = 3,
      fastMode = true
    )

    val result = FastSampleGenerator.generateFromTaskYaml(request)

    result.success shouldBe true
    result.sampleData shouldBe defined
    result.sampleData.get should have size 3

    val sampleData = result.sampleData.get
    sampleData.foreach { record =>
      record should contain key "simple_id"
      record("simple_id").toString.toLong should (be >= 1L and be <= 10L)
    }
  }
}