package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Task, TaskSummary}
import org.apache.log4j.Logger

import scala.util.Random

/**
 * Selects tasks based on configured weights for mixed workload testing.
 *
 * Example:
 * - Task A: weight=7 -> 70% of operations
 * - Task B: weight=3 -> 30% of operations
 *
 * Use Cases:
 * - Simulate realistic workload distributions (e.g., 70% reads, 30% writes)
 * - Test system behavior under varied load patterns
 * - Replicate production traffic patterns
 *
 * Phase 3 feature.
 */
class WeightedTaskSelector(tasks: List[(TaskSummary, Task)]) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val random = new Random()

  // Calculate task weights and selection ranges
  private case class TaskWeight(
                                 summary: TaskSummary,
                                 task: Task,
                                 weight: Int,
                                 rangeStart: Int,
                                 rangeEnd: Int
                               )

  private val weightedTasks: List[TaskWeight] = {
    // Filter out tasks without weights
    val tasksWithWeights = tasks.filter(_._1.weight.isDefined)

    if (tasksWithWeights.isEmpty) {
      // No weights defined, treat all tasks equally
      List()
    } else {
      val totalWeight = tasksWithWeights.map(_._1.weight.get).sum
      var currentRange = 0

      tasksWithWeights.map { case (summary, task) =>
        val weight = summary.weight.get
        val rangeStart = currentRange
        val rangeEnd = currentRange + weight
        currentRange = rangeEnd

        TaskWeight(summary, task, weight, rangeStart, rangeEnd)
      }
    }
  }

  private val totalWeight: Int = weightedTasks.map(_.weight).sum
  val hasWeights: Boolean = weightedTasks.nonEmpty

  if (hasWeights) {
    LOGGER.info(s"Weighted task execution enabled: total-weight=$totalWeight")
    weightedTasks.foreach { tw =>
      val percentage = (tw.weight.toDouble / totalWeight * 100).toInt
      LOGGER.info(s"Task '${tw.summary.name}': weight=${tw.weight}, percentage=$percentage%, range=${tw.rangeStart}-${tw.rangeEnd}")
    }
  }

  /**
   * Select a task based on weights using weighted random selection
   */
  def selectTask(): (TaskSummary, Task) = {
    if (!hasWeights || weightedTasks.isEmpty) {
      throw new IllegalStateException("No weighted tasks available for selection")
    }

    val randomValue = random.nextInt(totalWeight)

    val selected = weightedTasks.find { tw =>
      randomValue >= tw.rangeStart && randomValue < tw.rangeEnd
    }.getOrElse(weightedTasks.head) // Fallback to first task (shouldn't happen)

    (selected.summary, selected.task)
  }

  /**
   * Select multiple tasks based on weights for a given number of operations
   */
  def selectTasks(count: Int): List[(TaskSummary, Task)] = {
    if (!hasWeights || weightedTasks.isEmpty) {
      throw new IllegalStateException("No weighted tasks available for selection")
    }

    (1 to count).map(_ => selectTask()).toList
  }

  /**
   * Get the expected distribution of tasks based on weights
   */
  def getExpectedDistribution: Map[String, Double] = {
    if (!hasWeights) {
      Map()
    } else {
      weightedTasks.map { tw =>
        tw.summary.name -> (tw.weight.toDouble / totalWeight)
      }.toMap
    }
  }

  /**
   * Get the expected count for each task given a total number of operations
   */
  def getExpectedCounts(totalOperations: Int): Map[String, Int] = {
    getExpectedDistribution.map { case (name, ratio) =>
      name -> (totalOperations * ratio).toInt
    }
  }

  /**
   * Validate weight configuration
   */
  def validate(): List[String] = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // Check for negative weights
    tasks.filter(_._1.weight.exists(_ <= 0)).foreach { case (summary, _) =>
      errors += s"Task '${summary.name}' has invalid weight: ${summary.weight.get}. Weights must be positive."
    }

    // Warn if some tasks have weights and others don't
    val tasksWithWeights = tasks.filter(_._1.weight.isDefined)
    val tasksWithoutWeights = tasks.filter(_._1.weight.isEmpty)

    if (tasksWithWeights.nonEmpty && tasksWithoutWeights.nonEmpty) {
      LOGGER.warn(s"Mixed weight configuration: ${tasksWithWeights.size} tasks have weights, " +
        s"${tasksWithoutWeights.size} tasks don't. Tasks without weights will be executed sequentially first.")
    }

    errors.toList
  }

  /**
   * Get summary of weight configuration
   */
  def getSummary: String = {
    if (hasWeights) {
      val distribution = weightedTasks.map { tw =>
        val pct = (tw.weight.toDouble / totalWeight * 100).toInt
        s"${tw.summary.name}=$pct%"
      }.mkString(", ")
      s"Weighted execution: $distribution"
    } else {
      "No weighted execution (sequential)"
    }
  }
}

object WeightedTaskSelector {

  /**
   * Create a weighted task selector
   */
  def apply(tasks: List[(TaskSummary, Task)]): WeightedTaskSelector = {
    new WeightedTaskSelector(tasks)
  }

  /**
   * Check if any task has weights defined
   */
  def hasWeightedTasks(tasks: List[(TaskSummary, Task)]): Boolean = {
    tasks.exists(_._1.weight.isDefined)
  }

  /**
   * Separate tasks into weighted and non-weighted groups
   */
  def separateTasks(tasks: List[(TaskSummary, Task)]): (List[(TaskSummary, Task)], List[(TaskSummary, Task)]) = {
    val weighted = tasks.filter(_._1.weight.isDefined)
    val nonWeighted = tasks.filter(_._1.weight.isEmpty)
    (weighted, nonWeighted)
  }
}
