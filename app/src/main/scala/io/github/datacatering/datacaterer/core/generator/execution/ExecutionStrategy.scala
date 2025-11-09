package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Plan, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.generator.metrics.PerformanceMetrics

/**
 * Strategy pattern for different execution modes (count-based, duration-based, pattern-based)
 */
trait ExecutionStrategy {

  /**
   * Calculate the number of batches needed for execution
   */
  def calculateNumBatches: Int

  /**
   * Determine if execution should continue for the given batch number
   */
  def shouldContinue(currentBatch: Int): Boolean

  /**
   * Get performance metrics (if applicable)
   */
  def getMetrics: Option[PerformanceMetrics]

  /**
   * Called before batch execution starts
   */
  def onBatchStart(batchNumber: Int): Unit = {}

  /**
   * Called after batch execution completes
   */
  def onBatchEnd(batchNumber: Int, recordsGenerated: Long): Unit = {}

  /**
   * Get the data generation mode for this execution strategy.
   * Determines how data should be generated (per batch, all upfront, or progressively).
   */
  def getGenerationMode: GenerationMode = GenerationMode.Batched

  /**
   * Calculate the number of records to generate per batch.
   * Default implementation divides total records by number of batches.
   * 
   * @param batchNumber Current batch number (1-indexed)
   * @param totalRecords Total records to generate across all batches
   * @return Number of records for this batch
   */
  def getRecordsPerBatch(batchNumber: Int, totalRecords: Long): Long = {
    val numBatches = calculateNumBatches
    if (numBatches == Int.MaxValue) {
      totalRecords
    } else {
      totalRecords / numBatches
    }
  }
}

/**
 * Defines how data should be generated for an execution strategy
 */
sealed trait GenerationMode

object GenerationMode {
  /**
   * Generate data incrementally per batch (default for count-based, pattern-based)
   */
  case object Batched extends GenerationMode
  
  /**
   * Generate all data upfront before writing (used for duration+rate with streaming)
   */
  case object AllUpfront extends GenerationMode
  
  /**
   * Generate data progressively with temp storage (future use case)
   */
  case object Progressive extends GenerationMode
}
