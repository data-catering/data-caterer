package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{GenerationConfig, Plan, Task, TaskSummary}
import org.apache.log4j.Logger

/**
 * Factory for creating appropriate execution strategy based on plan configuration
 */
object ExecutionStrategyFactory {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def create(
              plan: Plan,
              executableTasks: List[(TaskSummary, Task)],
              generationConfig: GenerationConfig
            ): ExecutionStrategy = {

    // Check if any step has duration configured
    val hasDuration = executableTasks.flatMap(_._2.steps).exists(_.count.duration.isDefined)
    val hasPattern = executableTasks.flatMap(_._2.steps).exists(_.count.pattern.isDefined)

    // Check if this is a breaking point pattern (Phase 3)
    val isBreakingPoint = hasPattern && executableTasks.flatMap(_._2.steps)
      .exists(step => step.count.pattern.exists(_.`type`.toLowerCase == "breakingpoint"))

    (hasDuration, hasPattern, isBreakingPoint) match {
      case (true, true, true) =>
        // Phase 3: Breaking point execution with auto-stop
        LOGGER.info("Creating breaking point execution strategy (Phase 3)")
        new BreakingPointExecutionStrategy(executableTasks)

      case (true, false, _) =>
        LOGGER.info("Creating duration-based execution strategy")
        new DurationBasedExecutionStrategy(executableTasks)

      case (false, true, _) =>
        // Pattern-based execution - requires duration to be specified with pattern
        throw new IllegalArgumentException("Pattern-based execution requires duration to be specified")

      case (true, true, false) =>
        // Pattern takes precedence when both are specified (non-breaking point patterns)
        LOGGER.info("Creating pattern-based execution strategy")
        new PatternBasedExecutionStrategy(executableTasks)

      case (false, false, _) =>
        // Default: count-based execution (backward compatible)
        LOGGER.debug("Creating count-based execution strategy (backward compatible)")
        new CountBasedExecutionStrategy(plan, executableTasks, generationConfig)
    }
  }
}
