package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.{CardinalityConfig, DataCatererConfiguration, ForeignKey, Plan, Task, TaskSummary, ValidationConfiguration}
import org.apache.log4j.Logger

/**
 * Pre-processor that adjusts task record counts based on foreign key cardinality configurations.
 *
 * When a foreign key relationship defines cardinality (e.g., 1:N ratio), this processor calculates
 * the required number of child records and adjusts the target task's count configuration accordingly.
 * This ensures that data generation produces the correct number of records upfront, eliminating the
 * need for post-generation row duplication.
 *
 * Example:
 * - Source: accounts (30 records)
 * - Target: transactions (initially configured for 30 records)
 * - FK Cardinality: 1:2 ratio (each account should have 2 transactions)
 * - Adjusted: transactions count updated to 60 records
 *
 * Benefits:
 * - All fields get properly generated with unique values
 * - No duplicate rows in generated data
 * - Better test data quality and diversity
 * - Respects unique constraints on non-FK fields
 */
class CardinalityCountAdjustmentProcessor(val dataCatererConfiguration: DataCatererConfiguration)
  extends MutatingPrePlanProcessor {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override def apply(
    plan: Plan,
    tasks: List[Task],
    validations: List[ValidationConfiguration]
  ): (Plan, List[Task], List[ValidationConfiguration]) = {

    LOGGER.info("CardinalityCountAdjustmentProcessor starting...")

    // Extract foreign keys from plan's sink options
    val foreignKeys = plan.sinkOptions.map(_.foreignKeys).getOrElse(List())

    LOGGER.info(s"Found ${foreignKeys.size} foreign key configurations")

    if (foreignKeys.isEmpty) {
      LOGGER.debug("No foreign keys defined, skipping cardinality count adjustment")
      return (plan, tasks, validations)
    }

    // Build maps by data source name for FK lookup
    // Task summaries have dataSourceName which matches FK source/generate names
    val taskSummariesByDataSource = plan.tasks.map(ts => ts.dataSourceName -> ts).toMap

    // Tasks need to be mapped back to summaries to get dataSourceName
    val taskNameToSummary = plan.tasks.map(ts => ts.name -> ts).toMap
    val tasksByDataSource = tasks.flatMap { task =>
      taskNameToSummary.get(task.name).map(summary => summary.dataSourceName -> task)
    }.toMap

    LOGGER.info(s"Task data sources available: ${tasksByDataSource.keys.mkString(", ")}")
    LOGGER.info(s"Task summary data sources: ${taskSummariesByDataSource.keys.mkString(", ")}")

    // Calculate required counts based on cardinality for each FK relationship
    val countAdjustments = foreignKeys.flatMap { fk =>
      LOGGER.info(s"Processing FK: source=${fk.source.dataSource}, targets=${fk.generate.map(_.dataSource).mkString(", ")}, cardinality=${fk.cardinality}")
      if (fk.cardinality.isDefined) {
        calculateCountAdjustments(fk, tasksByDataSource, taskSummariesByDataSource)
      } else {
        LOGGER.info("No cardinality config for this FK, skipping")
        List()
      }
    }

    LOGGER.info(s"Count adjustments calculated: ${countAdjustments.mkString(", ")}")

    // Group adjustments by data source name and take the maximum required count
    val maxCountByDataSource = countAdjustments
      .groupBy(_._1)
      .mapValues(_.map(_._2).max)

    LOGGER.info(s"Max count by data source: ${maxCountByDataSource.mkString(", ")}")

    if (maxCountByDataSource.isEmpty) {
      LOGGER.debug("No cardinality configurations found, skipping count adjustment")
      return (plan, tasks, validations)
    }

    // Apply count adjustments to tasks (need to map from data source name back to task)
    val adjustedTasks = tasks.map { task =>
      // Get data source name for this task
      val dataSourceName = taskNameToSummary.get(task.name).map(_.dataSourceName)

      dataSourceName.flatMap(maxCountByDataSource.get) match {
        case Some(requiredCount) =>
          val originalCount = task.steps.headOption.flatMap(_.count.records).getOrElse(1L)
          // ALWAYS adjust count when cardinality is defined, even if original > required
          // This ensures the correct number of records are generated with proper grouping
          if (requiredCount != originalCount) {
            LOGGER.info(s"Adjusting task count due to cardinality: data-source=${dataSourceName.get}, " +
              s"task=${task.name}, original-count=$originalCount, adjusted-count=$requiredCount")

            // Update the count for all steps in this task
            // Also set up perField configuration to match the cardinality grouping
            val updatedSteps = task.steps.map { step =>
              // Get the FK field names for this step from the foreign key config
              val fkFieldNames = foreignKeys
                .flatMap(_.generate)
                .filter(_.dataSource == dataSourceName.get)
                .filter(_.step == step.name)
                .flatMap(_.fields)
                .distinct

              // Get the cardinality configuration for this step
              val cardinalityConfigOpt = foreignKeys
                .find(fk => fk.generate.exists(g => g.dataSource == dataSourceName.get && g.step == step.name))
                .flatMap(_.cardinality)

              // Set up perField configuration if FK fields exist
              val updatedCount = if (fkFieldNames.nonEmpty && cardinalityConfigOpt.isDefined) {
                val cardinalityConfig = cardinalityConfigOpt.get

                cardinalityConfig match {
                  case config if config.min.isDefined && config.max.isDefined =>
                    // Bounded: set perField with min/max options
                    // For bounded with perField, we need to set records to source count, not requiredCount
                    // because perField will multiply by min/max range
                    val sourceCount = foreignKeys
                      .find(fk => fk.generate.exists(g => g.dataSource == dataSourceName.get && g.step == step.name))
                      .map { fk =>
                        tasksByDataSource.get(fk.source.dataSource)
                          .flatMap(_.steps.headOption)
                          .flatMap(_.count.records)
                          .getOrElse {
                            taskSummariesByDataSource.get(fk.source.dataSource)
                              .flatMap(_.steps)
                              .flatMap(_.headOption)
                              .flatMap(_.count.records)
                              .getOrElse(1L)
                          }
                      }
                      .getOrElse(1L)

                    LOGGER.info(s"Setting perField config for step ${step.name}: fields=${fkFieldNames.mkString(",")}, " +
                      s"records=$sourceCount (not $requiredCount), min=${config.min.get}, max=${config.max.get}, distribution=${config.distribution}")
                    step.count.copy(
                      records = Some(sourceCount),  // Use source count, not requiredCount!
                      perField = Some(io.github.datacatering.datacaterer.api.model.PerFieldCount(
                        fieldNames = fkFieldNames,
                        count = None,  // Use options instead for bounded
                        options = Map(
                          "min" -> config.min.get,
                          "max" -> config.max.get,
                          "distribution" -> config.distribution
                        )
                      ))
                    )

                  case config if config.ratio.isDefined =>
                    // Ratio: set perField with fixed count and distribution
                    val recordsPerParent = config.ratio.get.toInt
                    LOGGER.info(s"Setting perField config for step ${step.name}: fields=${fkFieldNames.mkString(",")}, " +
                      s"count=$recordsPerParent, distribution=${config.distribution}")

                    // If distribution is uniform, use simple count; otherwise use options
                    if (config.distribution == "uniform") {
                      step.count.copy(
                        records = Some(requiredCount),
                        perField = Some(io.github.datacatering.datacaterer.api.model.PerFieldCount(
                          fieldNames = fkFieldNames,
                          count = Some(recordsPerParent.toLong)
                        ))
                      )
                    } else {
                      // For non-uniform distributions, use options with distribution
                      step.count.copy(
                        records = Some(requiredCount),
                        perField = Some(io.github.datacatering.datacaterer.api.model.PerFieldCount(
                          fieldNames = fkFieldNames,
                          count = None,
                          options = Map(
                            "min" -> recordsPerParent,
                            "max" -> recordsPerParent,
                            "distribution" -> config.distribution
                          )
                        ))
                      )
                    }

                  case _ =>
                    // No cardinality or 1:1
                    step.count.copy(records = Some(requiredCount))
                }
              } else {
                step.count.copy(records = Some(requiredCount))
              }

              step.copy(count = updatedCount)
            }
            task.copy(steps = updatedSteps)
          } else {
            task
          }
        case None => task
      }
    }

    // Also update TaskSummaries in the plan (extractMetadata might use these)
    val adjustedTaskSummaries = plan.tasks.map { taskSummary =>
      maxCountByDataSource.get(taskSummary.dataSourceName) match {
        case Some(requiredCount) =>
          // Update count in the TaskSummary's steps if they exist
          taskSummary.steps.map { stepList =>
            val originalCount = stepList.headOption.flatMap(_.count.records).getOrElse(1L)
            // ALWAYS adjust count when cardinality is defined, even if original > required
            if (requiredCount != originalCount) {
              LOGGER.info(s"Adjusting task summary count: data-source=${taskSummary.dataSourceName}, " +
                s"original-count=$originalCount, adjusted-count=$requiredCount")
              val updatedSteps = stepList.map { step =>
                // Get the FK field names for this step from the foreign key config
                val fkFieldNames = foreignKeys
                  .flatMap(_.generate)
                  .filter(_.dataSource == taskSummary.dataSourceName)
                  .filter(_.step == step.name)
                  .flatMap(_.fields)
                  .distinct

                // Get the cardinality configuration for this step
                val cardinalityConfigOpt = foreignKeys
                  .find(fk => fk.generate.exists(g => g.dataSource == taskSummary.dataSourceName && g.step == step.name))
                  .flatMap(_.cardinality)

                // Set up perField configuration if FK fields exist
                val updatedCount = if (fkFieldNames.nonEmpty && cardinalityConfigOpt.isDefined) {
                  val cardinalityConfig = cardinalityConfigOpt.get

                  cardinalityConfig match {
                    case config if config.min.isDefined && config.max.isDefined =>
                      // Bounded: set perField with min/max options
                      // For bounded with perField, we need to set records to source count, not requiredCount
                      val sourceCount = foreignKeys
                        .find(fk => fk.generate.exists(g => g.dataSource == taskSummary.dataSourceName && g.step == step.name))
                        .map { fk =>
                          taskSummariesByDataSource.get(fk.source.dataSource)
                            .flatMap(_.steps)
                            .flatMap(_.headOption)
                            .flatMap(_.count.records)
                            .getOrElse(1L)
                        }
                        .getOrElse(1L)

                      step.count.copy(
                        records = Some(sourceCount),  // Use source count, not requiredCount!
                        perField = Some(io.github.datacatering.datacaterer.api.model.PerFieldCount(
                          fieldNames = fkFieldNames,
                          count = None,
                          options = Map(
                            "min" -> config.min.get,
                            "max" -> config.max.get,
                            "distribution" -> config.distribution
                          )
                        ))
                      )

                    case config if config.ratio.isDefined =>
                      // Ratio: set perField with fixed count and distribution
                      val recordsPerParent = config.ratio.get.toInt

                      if (config.distribution == "uniform") {
                        step.count.copy(
                          records = Some(requiredCount),
                          perField = Some(io.github.datacatering.datacaterer.api.model.PerFieldCount(
                            fieldNames = fkFieldNames,
                            count = Some(recordsPerParent.toLong)
                          ))
                        )
                      } else {
                        step.count.copy(
                          records = Some(requiredCount),
                          perField = Some(io.github.datacatering.datacaterer.api.model.PerFieldCount(
                            fieldNames = fkFieldNames,
                            count = None,
                            options = Map(
                              "min" -> recordsPerParent,
                              "max" -> recordsPerParent,
                              "distribution" -> config.distribution
                            )
                          ))
                        )
                      }

                    case _ =>
                      step.count.copy(records = Some(requiredCount))
                  }
                } else {
                  step.count.copy(records = Some(requiredCount))
                }

                step.copy(count = updatedCount)
              }
              taskSummary.copy(steps = Some(updatedSteps))
            } else {
              taskSummary
            }
          }.getOrElse(taskSummary)
        case None => taskSummary
      }
    }

    val adjustedPlan = plan.copy(tasks = adjustedTaskSummaries)

    (adjustedPlan, adjustedTasks, validations)
  }

  /**
   * Calculate required record counts for target tasks based on cardinality config.
   *
   * @return List of (dataSourceName, requiredCount) tuples
   */
  private def calculateCountAdjustments(
    fk: ForeignKey,
    tasksByDataSource: Map[String, Task],
    taskSummariesByDataSource: Map[String, TaskSummary]
  ): List[(String, Long)] = {

    // Get source task to determine parent count
    val sourceDataSource = fk.source.dataSource
    val sourceTask = tasksByDataSource.get(sourceDataSource)
    val sourceCount = sourceTask
      .flatMap(_.steps.headOption)
      .flatMap(_.count.records)
      .getOrElse {
        // Try to get from task summary if not in task
        taskSummariesByDataSource.get(sourceDataSource)
          .flatMap(_.steps)
          .flatMap(_.headOption)
          .flatMap(_.count.records)
          .getOrElse(1L)
      }

    LOGGER.info(s"Source data source '$sourceDataSource' has $sourceCount records")

    // Calculate required count for each generation link based on cardinality
    fk.generate.flatMap { targetRelation =>
      val targetDataSource = targetRelation.dataSource

      fk.cardinality.map { cardinalityConfig =>
        val requiredCount = calculateRequiredCount(sourceCount, cardinalityConfig)
        LOGGER.info(s"Cardinality analysis: source=$sourceDataSource($sourceCount) -> " +
          s"target=$targetDataSource(required=$requiredCount), " +
          s"config=${cardinalityConfig}")
        (targetDataSource, requiredCount)
      }
    }
  }

  /**
   * Calculate the required number of child records based on cardinality configuration.
   */
  private def calculateRequiredCount(parentCount: Long, cardinality: CardinalityConfig): Long = {
    cardinality match {
      // Bounded cardinality: use max value
      case config if config.min.isDefined && config.max.isDefined =>
        parentCount * config.max.get

      // Ratio-based cardinality: multiply by ratio
      case config if config.ratio.isDefined =>
        val ratio = config.ratio.get
        math.ceil(parentCount * ratio).toLong

      // Default: 1:1
      case _ =>
        parentCount
    }
  }
}
