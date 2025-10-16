package io.github.datacatering.datacaterer.api

import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api.converter.Converters.toScalaMap
import io.github.datacatering.datacaterer.api.model.Constants.{YAML_PLAN_FILE, YAML_STEP_NAME, YAML_TASK_FILE, YAML_TASK_NAME}
import io.github.datacatering.datacaterer.api.model.{Plan, Task}

/**
 * Builds configurations by loading existing YAML plan or task files and allowing override of specific configurations.
 * This enables users to reference existing YAML definitions while still being able to customize specific aspects
 * using the programmatic API.
 *
 * @param yamlConfig Configuration for YAML file loading
 */
case class YamlBuilder(yamlConfig: YamlConfig = YamlConfig()) {
  def this() = this(YamlConfig())

  /**
   * Load from a YAML plan file. This creates a plan builder that references the existing YAML plan
   * and allows overriding specific configurations.
   *
   * @param planFile Path to the YAML plan file
   * @return PlanBuilder with YAML plan as base
   */
  def plan(planFile: String): PlanBuilder = {
    val updatedConfig = this.modify(_.yamlConfig.planFile).setTo(Some(planFile))
    PlanBuilder().fromYaml(updatedConfig.yamlConfig)
  }

  /**
   * Load from a YAML task file. This creates a task builder that references the existing YAML task
   * and allows overriding specific configurations.
   *
   * @param taskFile Path to the YAML task file
   * @return TaskBuilder with YAML task as base
   */
  def task(taskFile: String): TaskBuilder = {
    val updatedConfig = this.modify(_.yamlConfig.taskFile).setTo(Some(taskFile))
    TaskBuilder().fromYaml(updatedConfig.yamlConfig)
  }

  /**
   * Load from a YAML task file with specific task name filter.
   *
   * @param taskFile Path to the YAML task file
   * @param taskName Name of the specific task to use from the YAML file
   * @return TaskBuilder with filtered YAML task as base
   */
  def task(taskFile: String, taskName: String): TaskBuilder = {
    val updatedConfig = this.modify(_.yamlConfig.taskFile).setTo(Some(taskFile))
      .modify(_.yamlConfig.taskName).setTo(Some(taskName))
    TaskBuilder().fromYaml(updatedConfig.yamlConfig)
  }

  /**
   * Load from a YAML task file with specific task and step name filters.
   *
   * @param taskFile Path to the YAML task file
   * @param taskName Name of the specific task to use from the YAML file
   * @param stepName Name of the specific step to use from the task
   * @return TaskBuilder with filtered YAML task as base
   */
  def task(taskFile: String, taskName: String, stepName: String): TaskBuilder = {
    val updatedConfig = this.modify(_.yamlConfig.taskFile).setTo(Some(taskFile))
      .modify(_.yamlConfig.taskName).setTo(Some(taskName))
      .modify(_.yamlConfig.stepName).setTo(Some(stepName))
    TaskBuilder().fromYaml(updatedConfig.yamlConfig)
  }

  /**
   * Java API - Load from a YAML task file with specific task name filter.
   */
  def taskJava(taskFile: String, taskName: String): TaskBuilder = task(taskFile, taskName)

  /**
   * Java API - Load from a YAML task file with specific task and step name filters.
   */
  def taskJava(taskFile: String, taskName: String, stepName: String): TaskBuilder = 
    task(taskFile, taskName, stepName)
}

/**
 * Configuration for YAML file loading
 *
 * @param planFile   Optional path to YAML plan file
 * @param taskFile   Optional path to YAML task file
 * @param taskName   Optional task name filter
 * @param stepName   Optional step name filter
 */
case class YamlConfig(
                       planFile: Option[String] = None,
                       taskFile: Option[String] = None,
                       taskName: Option[String] = None,
                       stepName: Option[String] = None
                     ) {
  def this() = this(None, None, None, None)

  /**
   * Convert to options map for metadata source usage
   */
  def toOptionsMap: Map[String, String] = {
    val baseMap = Map.empty[String, String]
    val withPlan = planFile.fold(baseMap)(pf => baseMap + (YAML_PLAN_FILE -> pf))
    val withTask = taskFile.fold(withPlan)(tf => withPlan + (YAML_TASK_FILE -> tf))
    val withTaskName = taskName.fold(withTask)(tn => withTask + (YAML_TASK_NAME -> tn))
    val withStepName = stepName.fold(withTaskName)(sn => withTaskName + (YAML_STEP_NAME -> sn))
    withStepName
  }
}