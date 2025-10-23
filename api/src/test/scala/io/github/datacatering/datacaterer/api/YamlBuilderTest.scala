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

  test("Can create task builder from YAML file using taskByFile") {
    val yamlBuilder = YamlBuilder()
    val taskBuilder = yamlBuilder.taskByFile("/path/to/task.yaml")

    assert(taskBuilder.task.steps.nonEmpty)
    assert(taskBuilder.task.steps.head.name == "yaml_placeholder")
    assert(taskBuilder.task.steps.head.options.contains(YAML_TASK_FILE))
    assert(taskBuilder.task.steps.head.options(YAML_TASK_FILE) == "/path/to/task.yaml")
  }

  test("Can create task builder by task name using taskByName") {
    val yamlBuilder = YamlBuilder()
    val taskBuilder = yamlBuilder.taskByName("myTask")

    assert(taskBuilder.task.steps.nonEmpty)
    assert(taskBuilder.task.steps.head.name == "yaml_placeholder")
    assert(taskBuilder.task.steps.head.options.contains(YAML_TASK_NAME))
    assert(taskBuilder.task.steps.head.options(YAML_TASK_NAME) == "myTask")
  }

  test("Can create step builder from YAML file using stepByFile") {
    val yamlBuilder = YamlBuilder()
    val taskBuilder = yamlBuilder.stepByFile("/path/to/task.yaml", "myStep")

    assert(taskBuilder.task.steps.nonEmpty)
    assert(taskBuilder.task.steps.head.name == "yaml_placeholder")
    assert(taskBuilder.task.steps.head.options.contains(YAML_TASK_FILE))
    assert(taskBuilder.task.steps.head.options.contains(YAML_STEP_NAME))
    assert(taskBuilder.task.steps.head.options(YAML_STEP_NAME) == "myStep")
  }

  test("Can create step builder by task name using stepByName") {
    val yamlBuilder = YamlBuilder()
    val taskBuilder = yamlBuilder.stepByName("myTask", "myStep")

    assert(taskBuilder.task.steps.nonEmpty)
    assert(taskBuilder.task.steps.head.name == "yaml_placeholder")
    assert(taskBuilder.task.steps.head.options.contains(YAML_TASK_NAME))
    assert(taskBuilder.task.steps.head.options.contains(YAML_STEP_NAME))
    assert(taskBuilder.task.steps.head.options(YAML_TASK_NAME) == "myTask")
    assert(taskBuilder.task.steps.head.options(YAML_STEP_NAME) == "myStep")
  }

  test("Can create step builder from YAML file with task name using stepByFileAndName") {
    val yamlBuilder = YamlBuilder()
    val taskBuilder = yamlBuilder.stepByFileAndName("/path/to/task.yaml", "myTask", "myStep")

    assert(taskBuilder.task.steps.nonEmpty)
    assert(taskBuilder.task.steps.head.name == "yaml_placeholder")
    assert(taskBuilder.task.steps.head.options.contains(YAML_TASK_FILE))
    assert(taskBuilder.task.steps.head.options.contains(YAML_TASK_NAME))
    assert(taskBuilder.task.steps.head.options.contains(YAML_STEP_NAME))
    assert(taskBuilder.task.steps.head.options(YAML_TASK_FILE) == "/path/to/task.yaml")
    assert(taskBuilder.task.steps.head.options(YAML_TASK_NAME) == "myTask")
    assert(taskBuilder.task.steps.head.options(YAML_STEP_NAME) == "myStep")
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