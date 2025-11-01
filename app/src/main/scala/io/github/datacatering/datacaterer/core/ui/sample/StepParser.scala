package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest
import io.github.datacatering.datacaterer.core.ui.service.TaskLoaderService
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}

/**
 * Handles parsing and extraction of Step configurations from various sources
 *
 * This class consolidates all step parsing logic that was previously duplicated
 * throughout FastSampleGenerator, including:
 * - Parsing steps from YAML files
 * - Parsing steps from YAML content strings
 * - Finding specific steps within tasks
 * - Navigating step hierarchies in plans
 *
 * Responsibilities:
 * - YAML file parsing and validation
 * - Step lookup and navigation
 * - Error handling with helpful messages
 */
object StepParser {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Parse a step from a YAML task file
   *
   * @param taskPath Path to the task YAML file
   * @param stepName Optional specific step name to extract (defaults to first step)
   * @return Step configuration
   * @throws java.io.FileNotFoundException if task file not found
   * @throws IllegalArgumentException if no steps found or step name not found
   */
  def parseStepFromYaml(taskPath: String, stepName: Option[String])(implicit sparkSession: SparkSession): Step = {
    LOGGER.debug(s"Parsing step from YAML file: $taskPath, step: $stepName")

    if (!Files.exists(Paths.get(taskPath))) {
      throw new java.io.FileNotFoundException(s"Task file not found: $taskPath")
    }

    // Use consolidated YAML parsing from PlanParser
    val convertedTask = PlanParser.parseTaskFile(taskPath)

    if (convertedTask.steps.isEmpty) {
      throw new IllegalArgumentException("No steps found in task file")
    }

    stepName match {
      case Some(name) =>
        convertedTask.steps.find(_.name == name) match {
          case Some(step) => step
          case None =>
            val availableSteps = convertedTask.steps.map(_.name).mkString(", ")
            throw new IllegalArgumentException(
              s"Step '$name' not found in task. Available steps: $availableSteps"
            )
        }
      case None => convertedTask.steps.head
    }
  }

  /**
   * Parse a step from YAML content string
   *
   * @param yamlContent YAML content as string
   * @param stepName Optional specific step name to extract (defaults to first step)
   * @return Step configuration
   * @throws IllegalArgumentException if YAML is empty, has no steps, or step name not found
   */
  def parseStepFromYamlContent(yamlContent: String, stepName: Option[String]): Step = {
    LOGGER.debug(s"Parsing step from YAML content, step: $stepName")

    if (yamlContent.trim.isEmpty) {
      throw new IllegalArgumentException("YAML content is empty")
    }

    // Use consolidated YAML parsing from PlanParser
    val convertedTask = PlanParser.parseTaskFromContent(yamlContent)

    if (convertedTask.steps.isEmpty) {
      throw new IllegalArgumentException("No steps found in task YAML")
    }

    stepName match {
      case Some(name) =>
        convertedTask.steps.find(_.name == name) match {
          case Some(step) => step
          case None =>
            val availableSteps = convertedTask.steps.map(_.name).mkString(", ")
            throw new IllegalArgumentException(
              s"Step '$name' not found in task. Available steps: $availableSteps"
            )
        }
      case None => convertedTask.steps.head
    }
  }

  /**
   * Find steps in a plan by task name
   *
   * This handles both JSON and YAML plans differently:
   * - JSON plans: Steps are directly in planRequest.tasks
   * - YAML plans: Tasks are empty, need to load from task files
   *
   * @param planRequest The plan request to search
   * @param taskName Name of the task to find
   * @param taskDirectory Optional custom task directory
   * @return List of steps in the task
   * @throws IllegalArgumentException if task not found
   */
  def findStepsInPlan(
    planRequest: PlanRunRequest,
    taskName: String,
    taskDirectory: Option[String] = None
  )(implicit sparkSession: SparkSession): List[Step] = {
    // For JSON plans, steps are in planRequest.tasks
    if (planRequest.tasks.nonEmpty) {
      planRequest.tasks.find(_.name == taskName) match {
        case Some(step) => List(step)
        case None =>
          val availableSteps = planRequest.tasks.map(_.name).mkString(", ")
          throw new IllegalArgumentException(s"Step/Task '$taskName' not found in plan. Available: $availableSteps")
      }
    } else {
      // For YAML plans, tasks are empty and need to be loaded from task files
      // First check if taskName is a task name in the plan
      val taskNames = planRequest.plan.tasks.map(_.name)
      if (!taskNames.contains(taskName)) {
        throw new IllegalArgumentException(s"Step/Task '$taskName' not found in plan. Available: ${taskNames.mkString(", ")}")
      }

      // Load the task from YAML files using TaskLoaderService
      TaskLoaderService.findTaskByName(taskName, taskDirectory).steps.headOption match {
        case Some(step) => List(step)
        case None =>
          throw new IllegalArgumentException(s"No steps found in task '$taskName'")
      }
    }
  }

  /**
   * Find a specific step within a task from a plan
   *
   * This navigates the plan structure to find a specific step by name within a task.
   * Handles both JSON and YAML plan formats.
   *
   * @param planRequest The plan request to search
   * @param taskName Name of the task containing the step
   * @param stepName Name of the step to find
   * @param taskDirectory Optional custom task directory
   * @return Step configuration
   * @throws IllegalArgumentException if task or step not found
   */
  def findSpecificStepInTask(
    planRequest: PlanRunRequest,
    taskName: String,
    stepName: String,
    taskDirectory: Option[String] = None
  )(implicit sparkSession: SparkSession): Step = {
    // For JSON plans, steps are in planRequest.tasks
    if (planRequest.tasks.nonEmpty) {
      // In JSON plans, each "task" in planRequest.tasks is actually a Step representing the entire task
      planRequest.tasks.find(_.name == taskName) match {
        case Some(taskStep) =>
          // The task itself is the step we want (no sub-steps in JSON plans)
          taskStep
        case None =>
          val availableTasks = planRequest.tasks.map(_.name).mkString(", ")
          throw new IllegalArgumentException(s"Task '$taskName' not found in plan. Available: $availableTasks")
      }
    } else {
      // For YAML plans, load the task from task files and find the specific step
      val task = TaskLoaderService.findTaskByName(taskName, taskDirectory)
      task.steps.find(_.name == stepName) match {
        case Some(step) => step
        case None =>
          val availableSteps = task.steps.map(_.name).mkString(", ")
          throw new IllegalArgumentException(s"Step '$stepName' not found in task '$taskName'. Available steps: $availableSteps")
      }
    }
  }

  /**
   * Get all steps from a task
   *
   * @param taskName Name of the task
   * @param taskDirectory Optional custom task directory
   * @return List of all steps in the task
   */
  def getAllStepsFromTask(
    taskName: String,
    taskDirectory: Option[String] = None
  )(implicit sparkSession: SparkSession): List[Step] = {
    TaskLoaderService.getAllStepsFromTask(taskName, taskDirectory)
  }
}
