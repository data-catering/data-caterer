package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.Constants.{YAML_PLAN_FILE, YAML_STEP_NAME, YAML_TASK_FILE, YAML_TASK_NAME}
import org.scalatest.funsuite.AnyFunSuite

class YamlBuilderTest extends AnyFunSuite {

  test("Can create YamlBuilder") {
    val yamlBuilder = YamlBuilder()
    
    assert(yamlBuilder.yamlConfig.planFile.isEmpty)
    assert(yamlBuilder.yamlConfig.taskFile.isEmpty)
    assert(yamlBuilder.yamlConfig.taskName.isEmpty)
    assert(yamlBuilder.yamlConfig.stepName.isEmpty)
  }

  test("Can create plan builder from YAML file") {
    val yamlBuilder = YamlBuilder()
    val planBuilder = yamlBuilder.plan("/path/to/plan.yaml")
    
    assert(planBuilder.plan.description.contains("YAML: /path/to/plan.yaml"))
  }

  test("Can create task builder from YAML file") {
    val yamlBuilder = YamlBuilder()
    val taskBuilder = yamlBuilder.task("/path/to/task.yaml")
    
    assert(taskBuilder.task.name.contains("yaml_"))
  }

  test("Can create task builder from YAML file with task name filter") {
    val yamlBuilder = YamlBuilder()
    val taskBuilder = yamlBuilder.task("/path/to/task.yaml", "myTask")
    
    assert(taskBuilder.task.name.contains("yaml_"))
  }

  test("Can create task builder from YAML file with task and step name filters") {
    val yamlBuilder = YamlBuilder()
    val taskBuilder = yamlBuilder.task("/path/to/task.yaml", "myTask", "myStep")
    
    assert(taskBuilder.task.name.contains("yaml_"))
  }

  test("YamlConfig can convert to options map") {
    val yamlConfig = YamlConfig(
      planFile = Some("/path/to/plan.yaml"),
      taskFile = Some("/path/to/task.yaml"),
      taskName = Some("myTask"),
      stepName = Some("myStep")
    )
    
    val options = yamlConfig.toOptionsMap
    
    assert(options(YAML_PLAN_FILE) == "/path/to/plan.yaml")
    assert(options(YAML_TASK_FILE) == "/path/to/task.yaml")
    assert(options(YAML_TASK_NAME) == "myTask")
    assert(options(YAML_STEP_NAME) == "myStep")
  }

  test("YamlConfig handles empty values correctly") {
    val yamlConfig = YamlConfig()
    val options = yamlConfig.toOptionsMap
    
    assert(options.isEmpty)
  }
}