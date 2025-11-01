package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.IntegerType
import io.github.datacatering.datacaterer.core.util.SparkSuite

class YamlMetadataTest extends SparkSuite {

  test("Read metadata from existing task YAML file") {
    class TaskYamlRun extends PlanRun {
      val yamlReferenceTaskPath = getClass.getClassLoader.getResource("sample/task/file/yaml-reference-task.yaml")
      val jsonTask = json("my-existing-yaml-task", "/tmp/json/my-yaml-task", Map("saveMode" -> "overwrite", "numPartitions" -> "1"))
        .fields(metadataSource.yamlTask(yamlReferenceTaskPath.getPath, "my_yaml_task"))
        .fields(
          field.name("year").`type`(IntegerType).min(2024).max(2025)
        )
        .count(count.records(10))

      val config = configuration.enableGeneratePlanAndTasks(true).generatedReportsFolderPath("/tmp/data-caterer/report")

      execute(config, jsonTask)
    }

    PlanProcessor.determineAndExecutePlan(Some(new TaskYamlRun()))

    val written = sparkSession.read.json("/tmp/json/my-yaml-task").collect()
    assert(written.nonEmpty, "Expected JSON data to be written to a file")
    assert(written.length == 10, "Expected 10 records generated")
    assert(written.head.schema.fields.length == 9, "Expected schema should be the 9 fields defined in YAML (user defined value overrides options for year field)")
  }
}



