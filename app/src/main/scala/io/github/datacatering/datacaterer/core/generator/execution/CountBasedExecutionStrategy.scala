package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{GenerationConfig, Plan, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.generator.metrics.PerformanceMetrics
import io.github.datacatering.datacaterer.core.util.RecordCountUtil
import org.apache.log4j.Logger

/**
 * Traditional count-based execution strategy (backward compatible)
 * Delegates to existing RecordCountUtil logic
 */
class CountBasedExecutionStrategy(
                                    plan: Plan,
                                    executableTasks: List[(TaskSummary, Task)],
                                    generationConfig: GenerationConfig
                                  ) extends ExecutionStrategy {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val foreignKeys = plan.sinkOptions.map(_.foreignKeys).getOrElse(List())
  private val (numBatches, _) = RecordCountUtil.calculateNumBatches(foreignKeys, executableTasks, generationConfig)

  LOGGER.info(s"Count-based execution strategy initialized: num-batches=$numBatches")

  override def calculateNumBatches: Int = numBatches

  override def shouldContinue(currentBatch: Int): Boolean = {
    currentBatch <= numBatches
  }

  override def getMetrics: Option[PerformanceMetrics] = None // No metrics collection for count-based
}
