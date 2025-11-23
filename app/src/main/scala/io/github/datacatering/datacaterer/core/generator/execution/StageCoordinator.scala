package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Task, TaskSummary}
import org.apache.log4j.Logger

/**
 * Coordinates multi-stage test execution: setup -> execution -> teardown
 *
 * Stage Types:
 * - setup: Preparatory tasks (data seeding, resource initialization)
 * - execution: Main test workload
 * - teardown: Cleanup tasks (resource deallocation, data deletion)
 *
 * Features:
 * - Tasks are grouped by stage
 * - Stages execute sequentially in order
 * - Tasks within a stage can execute in parallel (existing behavior)
 * - Metrics are collected per stage
 */
class StageCoordinator(tasks: List[(TaskSummary, Task)]) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  // Group tasks by stage (default stage is "execution")
  private val tasksByStage: Map[String, List[(TaskSummary, Task)]] = {
    tasks.groupBy { case (summary, _) =>
      summary.stage.getOrElse("execution")
    }
  }

  // Define stage execution order
  private val stageOrder: List[String] = List("setup", "execution", "teardown")

  // Track which stages have tasks
  val availableStages: List[String] = stageOrder.filter(tasksByStage.contains)
  val hasMultipleStages: Boolean = availableStages.size > 1
  val hasSetupStage: Boolean = tasksByStage.contains("setup")
  val hasExecutionStage: Boolean = tasksByStage.contains("execution")
  val hasTeardownStage: Boolean = tasksByStage.contains("teardown")

  LOGGER.info(s"Stage coordinator initialized: stages=${availableStages.mkString(", ")}, " +
    s"total-tasks=${tasks.size}, multi-stage=$hasMultipleStages")

  if (hasMultipleStages) {
    availableStages.foreach { stage =>
      val stageTasks = tasksByStage(stage)
      LOGGER.info(s"Stage '$stage': ${stageTasks.size} task(s) - ${stageTasks.map(_._1.name).mkString(", ")}")
    }
  }

  /**
   * Get tasks for a specific stage
   */
  def getTasksForStage(stage: String): List[(TaskSummary, Task)] = {
    tasksByStage.getOrElse(stage, List())
  }

  /**
   * Get all tasks in stage execution order
   * Returns a list of (stage, tasks) tuples
   */
  def getTasksInStageOrder: List[(String, List[(TaskSummary, Task)])] = {
    availableStages.map { stage =>
      (stage, tasksByStage(stage))
    }
  }

  /**
   * Check if a specific stage exists
   */
  def hasStage(stage: String): Boolean = {
    tasksByStage.contains(stage)
  }

  /**
   * Get the number of tasks in a stage
   */
  def getStageTaskCount(stage: String): Int = {
    tasksByStage.get(stage).map(_.size).getOrElse(0)
  }

  /**
   * Get summary of stage configuration
   */
  def getStageSummary: String = {
    if (hasMultipleStages) {
      val stageCounts = availableStages.map { stage =>
        s"$stage=${getStageTaskCount(stage)}"
      }.mkString(", ")
      s"Multi-stage execution: $stageCounts"
    } else {
      s"Single-stage execution: ${tasks.size} task(s)"
    }
  }

  /**
   * Validate stage configuration
   * Returns a list of validation errors (empty if valid)
   */
  def validate(): List[String] = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // Check for unknown stages
    val unknownStages = tasksByStage.keys.filterNot(stageOrder.contains)
    if (unknownStages.nonEmpty) {
      errors += s"Unknown stage(s): ${unknownStages.mkString(", ")}. Valid stages are: ${stageOrder.mkString(", ")}"
    }

    // Warn if setup/teardown stages exist without execution stage
    if ((hasSetupStage || hasTeardownStage) && !hasExecutionStage) {
      LOGGER.warn("Setup or teardown stage defined but no execution stage found")
    }

    errors.toList
  }
}

object StageCoordinator {

  /**
   * Create a stage coordinator from tasks
   */
  def apply(tasks: List[(TaskSummary, Task)]): StageCoordinator = {
    new StageCoordinator(tasks)
  }

  /**
   * Check if any task has a stage defined (to determine if stage coordination is needed)
   */
  def isMultiStageExecution(tasks: List[(TaskSummary, Task)]): Boolean = {
    tasks.exists(_._1.stage.isDefined)
  }

  /**
   * Default stages in execution order
   */
  val DEFAULT_STAGE_ORDER: List[String] = List("setup", "execution", "teardown")
}
