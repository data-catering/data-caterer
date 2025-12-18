package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.{CardinalityConfig, DataCatererConfiguration, ForeignKey, Plan, Task, ValidationConfiguration}
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
 */
class CardinalityCountAdjustmentProcessor(val dataCatererConfiguration: DataCatererConfiguration)
  extends MutatingPrePlanProcessor {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override def apply(
                      plan: Plan,
                      tasks: List[Task],
                      validations: List[ValidationConfiguration]
                    ): (Plan, List[Task], List[ValidationConfiguration]) = {

    LOGGER.debug("CardinalityCountAdjustmentProcessor starting...")
    // Extract foreign keys from plan's sink options
    val foreignKeys = plan.sinkOptions.map(_.foreignKeys).getOrElse(List())

    if (foreignKeys.isEmpty) {
      LOGGER.debug("No foreign keys defined, skipping cardinality count adjustment")
      return (plan, tasks, validations)
    }

    // Tasks need to be mapped back to summaries to get dataSourceName
    val taskNameToSummary = plan.tasks.map(ts => ts.name -> ts).toMap
    val tasksByDataSource = tasks.flatMap { task =>
      taskNameToSummary.get(task.name).map(summary => summary.dataSourceName -> task)
    }.toMap

    // Validate FK configurations before processing
    validateForeignKeyConfigurations(foreignKeys, tasksByDataSource)

    // Enhance foreign keys with synthetic cardinality from perField counts if needed
    val enhancedForeignKeys = enhanceForeignKeysWithPerFieldCardinality(
      foreignKeys, tasksByDataSource
    )

    if (enhancedForeignKeys.forall(_.generate.forall(_.cardinality.isEmpty))) {
      LOGGER.debug("No cardinality configurations found after enhancement, skipping count adjustment")
      return (plan, tasks, validations)
    }

    LOGGER.debug(s"Processing ${enhancedForeignKeys.size} foreign key(s) with cardinality configurations")

    // Apply count adjustments to tasks based on their FK target configurations
    val adjustedTasks = tasks.map { task =>
      // Get data source name for this task
      val dataSourceNameOpt = taskNameToSummary.get(task.name).map(_.dataSourceName)

      dataSourceNameOpt match {
        case Some(dataSourceName) =>
          // Get all step names in this task to check if any are FK targets
          val taskStepNames = task.steps.map(_.name).toSet
          
          // Check if any step in this task is a target in any FK relationship with cardinality
          // Must match BOTH dataSource AND step name to avoid incorrect matching
          val targetRelationOpt = enhancedForeignKeys
            .flatMap(_.generate)
            .find(target => target.dataSource == dataSourceName && target.cardinality.isDefined && taskStepNames.contains(target.step))

          targetRelationOpt match {
            case Some(targetRelation) =>
              // Find the FK that contains this target
              val fkOpt = enhancedForeignKeys.find(_.generate.contains(targetRelation))

              fkOpt match {
                case Some(fk) =>
                  val sourceCount = getSourceCount(tasksByDataSource.get(fk.source.dataSource))
                  val requiredCount = calculateRequiredCount(sourceCount, targetRelation.cardinality.get)
                  val originalCount = task.steps.headOption.flatMap(_.count.records).getOrElse(1L)

                  if (requiredCount != originalCount) {
                    LOGGER.debug(s"Adjusting task count due to cardinality: data-source=$dataSourceName, " +
                      s"task=${task.name}, original-count=$originalCount, adjusted-count=$requiredCount")

                    // Update only steps that are FK targets with cardinality config
                    // DO NOT modify steps that are not FK targets (like the source step in the same task)
                    val updatedSteps = task.steps.map { step =>
                      // Get the target relation for this step from the foreign key config
                      val targetRelationOpt = enhancedForeignKeys
                        .flatMap(_.generate)
                        .find(target => target.dataSource == dataSourceName && target.step == step.name)

                      // Only process steps that are actual FK targets
                      targetRelationOpt match {
                        case None =>
                          // This step is NOT a FK target - leave it unchanged
                          LOGGER.debug(s"Step ${step.name} is not a FK target, leaving count unchanged: ${step.count.records}")
                          step
                          
                        case Some(targetRel) =>
                          val fkFieldNames = targetRel.fields.distinct
                          val cardinalityConfigOpt = targetRel.cardinality

                          // Get the source FK for this step
                          val fkOpt = enhancedForeignKeys
                            .find(fk => fk.generate.exists(g => g.dataSource == dataSourceName && g.step == step.name))

                          val sourceCount = fkOpt
                            .map { fk =>
                              tasksByDataSource.get(fk.source.dataSource)
                                .flatMap(_.steps.find(_.name == fk.source.step))
                                .flatMap(_.count.records)
                                .getOrElse(1L)
                            }
                            .getOrElse(1L)

                          // Check if step originally had perField config on FK fields (before our processing)
                          val hadOriginalPerField = step.count.perField.exists { pfc =>
                            fkFieldNames.exists(pfc.fieldNames.contains)
                          }

                          // Determine if we should set perField configuration
                          // - If step HAD perField on FK fields: DON'T set it (causes double-grouping with random values)
                          // - If step DIDN'T have perField: SET it (enables proper grouping during generation)
                          val updatedCount = if (fkFieldNames.nonEmpty && cardinalityConfigOpt.isDefined && !hadOriginalPerField) {
                            val cardinalityConfig = cardinalityConfigOpt.get

                            cardinalityConfig match {
                              case config if config.min.isDefined && config.max.isDefined =>
                                // Bounded: set perField with min/max options
                                LOGGER.debug(s"Setting perField config for step ${step.name}: fields=${fkFieldNames.mkString(",")}, " +
                                  s"records=$sourceCount, min=${config.min.get}, max=${config.max.get}, distribution=${config.distribution}")
                                step.count.copy(
                                  records = Some(sourceCount), // Use source count for bounded
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
                                // Ratio: set perField with fixed count
                                val recordsPerParent = config.ratio.get.toInt
                                LOGGER.debug(s"Setting perField config for step ${step.name}: fields=${fkFieldNames.mkString(",")}, " +
                                  s"records=$sourceCount, count=$recordsPerParent, distribution=${config.distribution}")

                                if (config.distribution == "uniform") {
                                  step.count.copy(
                                    records = Some(sourceCount),
                                    perField = Some(io.github.datacatering.datacaterer.api.model.PerFieldCount(
                                      fieldNames = fkFieldNames,
                                      count = Some(recordsPerParent.toLong)
                                    ))
                                  )
                                } else {
                                  step.count.copy(
                                    records = Some(sourceCount),
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
                                step.count.copy(records = Some(sourceCount), perField = None)
                            }
                          } else if (hadOriginalPerField) {
                            // Step had original perField on FK fields - remove it to avoid double-grouping
                            LOGGER.debug(s"Removing original perField config from step ${step.name} to avoid double-grouping (FK fields: ${fkFieldNames.mkString(",")})")
                            step.count.copy(records = Some(requiredCount), perField = None)
                          } else {
                            step.count.copy(records = Some(requiredCount))
                          }

                          step.copy(count = updatedCount)
                      }
                    }
                    task.copy(steps = updatedSteps)
                  } else {
                    task
                  }
                case None => task
              }
            case None => task
          }
        case None => task
      }
    }

    val adjustedPlan = plan.copy(
      sinkOptions = plan.sinkOptions.map(_.copy(foreignKeys = enhancedForeignKeys))
    )

    (adjustedPlan, adjustedTasks, validations)
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

  private def getSourceCount(sourceTask: Option[Task]): Long = {
    sourceTask
      .flatMap(_.steps.headOption)
      .flatMap(_.count.records)
      .getOrElse(1L)
  }

  /**
   * Validate foreign key configurations to prevent conflicts.
   *
   * Validates that:
   * 1. Target cardinality and target step perField on FK fields are not both defined
   * 2. Cardinality config has either ratio OR min/max, not both
   */
  private def validateForeignKeyConfigurations(
                                                foreignKeys: List[ForeignKey],
                                                tasksByDataSource: Map[String, Task]
                                              ): Unit = {
    foreignKeys.foreach { fk =>
      fk.generate.foreach { targetRelation =>
        // Validation 1: Reject if both target cardinality AND step perField on FK fields are defined
        if (targetRelation.cardinality.isDefined) {
          val targetTaskOpt = tasksByDataSource.get(targetRelation.dataSource)
          val targetStepOpt = targetTaskOpt.flatMap(_.steps.find(_.name == targetRelation.step))

          targetStepOpt.flatMap(_.count.perField).foreach { perFieldCount =>
            val perFieldNames = perFieldCount.fieldNames
            val fkFields = targetRelation.fields
            val hasOverlap = fkFields.exists(perFieldNames.contains)

            if (hasOverlap) {
              val errorMsg = s"Invalid FK configuration: target has BOTH cardinality config AND step perField on FK fields. " +
                s"Please use only ONE approach. " +
                s"Target: dataSource=${targetRelation.dataSource}, step=${targetRelation.step}, " +
                s"FK fields=${fkFields.mkString(",")}, perField fields=${perFieldNames.mkString(",")}, " +
                s"cardinality=${targetRelation.cardinality}"
              LOGGER.error(errorMsg)
              throw new IllegalArgumentException(errorMsg)
            }
          }
        }

        // Validation 2: Reject if cardinality has both ratio AND min/max
        targetRelation.cardinality.foreach { cardinalityConfig =>
          val hasRatio = cardinalityConfig.ratio.isDefined
          val hasMinMax = cardinalityConfig.min.isDefined && cardinalityConfig.max.isDefined

          if (hasRatio && hasMinMax) {
            val errorMsg = s"Invalid cardinality configuration: cannot specify BOTH ratio AND min/max. " +
              s"Please use only ONE approach. " +
              s"Target: dataSource=${targetRelation.dataSource}, step=${targetRelation.step}, " +
              s"config=$cardinalityConfig"
            LOGGER.error(errorMsg)
            throw new IllegalArgumentException(errorMsg)
          }
        }
      }
    }
  }

  /**
   * Enhance foreign keys by detecting perField counts in target steps and creating
   * synthetic cardinality configurations when needed.
   *
   * This allows foreign key relationships to work with perField counts even when
   * cardinality is not explicitly configured on each target.
   */
  private def enhanceForeignKeysWithPerFieldCardinality(
                                                         foreignKeys: List[ForeignKey],
                                                         tasksByDataSource: Map[String, Task]
                                                       ): List[ForeignKey] = {

    foreignKeys.map { fk =>
      // Check each target individually for perField counts and add synthetic cardinality
      val enhancedTargets = fk.generate.map { targetRelation =>
        // Skip if target already has cardinality defined
        if (targetRelation.cardinality.isDefined) {
          LOGGER.debug(s"Target already has cardinality defined: target=${targetRelation.dataSource}, skipping perField detection")
          targetRelation
        } else {
          // Get the target task and step
          val targetTaskOpt = tasksByDataSource.get(targetRelation.dataSource)
          val targetStepOpt = targetTaskOpt.flatMap(_.steps.find(_.name == targetRelation.step))

          // Check if step has perField count that includes FK fields
          val perFieldCardinalityOpt = targetStepOpt.flatMap(_.count.perField).flatMap { perFieldCount =>
            val perFieldNames = perFieldCount.fieldNames
            val fkFields = targetRelation.fields
            val hasOverlap = fkFields.exists(perFieldNames.contains)

            if (hasOverlap) {
              LOGGER.debug(s"Found perField count on FK fields in target: dataSource=${targetRelation.dataSource}, " +
                s"step=${targetRelation.step}, perFieldNames=${perFieldNames.mkString(",")}, " +
                s"fkFields=${fkFields.mkString(",")}")

              // Create synthetic cardinality from perField config
              val cardinalityConfig = if (perFieldCount.count.isDefined) {
                // Fixed count per field - use ratio
                val count = perFieldCount.count.get
                LOGGER.debug(s"Creating synthetic cardinality for target from perField count: ratio=$count, distribution=uniform")
                Some(CardinalityConfig(
                  ratio = Some(count.toDouble),
                  distribution = "uniform"
                ))
              } else if (perFieldCount.options.contains("min") && perFieldCount.options.contains("max")) {
                // Bounded with min/max - create bounded cardinality
                val min = perFieldCount.options("min").toString.toInt
                val max = perFieldCount.options("max").toString.toInt
                val distribution = perFieldCount.options.getOrElse("distribution", "uniform").toString
                LOGGER.debug(s"Creating synthetic cardinality for target from perField min/max: min=$min, max=$max, distribution=$distribution")
                Some(CardinalityConfig(
                  min = Some(min),
                  max = Some(max),
                  distribution = distribution
                ))
              } else {
                LOGGER.warn(s"perField config exists but has neither count nor min/max: $perFieldCount")
                None
              }
              cardinalityConfig
            } else {
              None
            }
          }

          perFieldCardinalityOpt match {
            case Some(cardinalityConfig) =>
              LOGGER.debug(s"Enhanced target with synthetic cardinality: target=${targetRelation.dataSource}, config=$cardinalityConfig")
              targetRelation.copy(cardinality = Some(cardinalityConfig))
            case None =>
              targetRelation
          }
        }
      }

      fk.copy(generate = enhancedTargets)
    }
  }

}
