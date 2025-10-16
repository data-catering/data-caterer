package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.Constants.{YAML_PLAN_FILE, YAML_STEP_NAME, YAML_TASK_FILE, YAML_TASK_NAME}
import org.scalatest.funsuite.AnyFunSuite

class YamlMetadataSourceTest extends AnyFunSuite {

  test("Can create YAML plan metadata source") {
    val metadataSource = MetadataSourceBuilder().yamlPlan("/path/to/plan.yaml")
    
    assert(metadataSource.metadataSource.`type` == "yamlPlan")
    assert(metadataSource.metadataSource.allOptions(YAML_PLAN_FILE) == "/path/to/plan.yaml")
  }

  test("Can create YAML task metadata source") {
    val metadataSource = MetadataSourceBuilder().yamlTask("/path/to/task.yaml")
    
    assert(metadataSource.metadataSource.`type` == "yamlTask")
    assert(metadataSource.metadataSource.allOptions(YAML_TASK_FILE) == "/path/to/task.yaml")
  }

  test("Can create YAML task metadata source with task name filter") {
    val metadataSource = MetadataSourceBuilder().yamlTask("/path/to/task.yaml", "myTask")
    
    assert(metadataSource.metadataSource.`type` == "yamlTask")
    assert(metadataSource.metadataSource.allOptions(YAML_TASK_FILE) == "/path/to/task.yaml")
    assert(metadataSource.metadataSource.allOptions(YAML_TASK_NAME) == "myTask")
  }

  test("Can create YAML task metadata source with task and step name filters") {
    val metadataSource = MetadataSourceBuilder().yamlTask("/path/to/task.yaml", "myTask", "myStep")
    
    assert(metadataSource.metadataSource.`type` == "yamlTask")
    assert(metadataSource.metadataSource.allOptions(YAML_TASK_FILE) == "/path/to/task.yaml")
    assert(metadataSource.metadataSource.allOptions(YAML_TASK_NAME) == "myTask")
    assert(metadataSource.metadataSource.allOptions(YAML_STEP_NAME) == "myStep")
  }

  test("Can use YAML metadata source with file builder") {
    val yamlMetadataSource = MetadataSourceBuilder().yamlTask("/path/to/task.yaml", "simple_json", "file_account")
    
    // Verify the metadata source has the right options
    assert(yamlMetadataSource.metadataSource.allOptions.contains(YAML_TASK_FILE))
    assert(yamlMetadataSource.metadataSource.allOptions(YAML_TASK_FILE) == "/path/to/task.yaml")
    assert(yamlMetadataSource.metadataSource.allOptions(YAML_TASK_NAME) == "simple_json")
    assert(yamlMetadataSource.metadataSource.allOptions(YAML_STEP_NAME) == "file_account")
  }
}