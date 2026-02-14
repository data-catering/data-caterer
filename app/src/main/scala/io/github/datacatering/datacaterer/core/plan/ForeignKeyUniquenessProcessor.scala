package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.Constants.IS_UNIQUE
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, Plan, Step, Task, ValidationConfiguration}
import org.apache.log4j.Logger

/**
 * Pre-processor that ensures source foreign key fields are marked as unique.
 *
 * When foreign keys are defined, the source fields must generate unique values to ensure:
 * 1. Correct number of records are generated with proper FK relationships
 * 2. Cardinality configurations work as expected (e.g., 1:N ratios)
 * 3. No duplicate source keys that would cause unexpected record multiplication
 *
 * Example Problem:
 * - Source: accounts with account_id using regex "[A-E]" (only 5 possible values)
 * - Count: 50 records requested
 * - FK Cardinality: 1:2 ratio (each account should have 2 transactions)
 * - Without unique=true: account_id might generate duplicates (e.g., "A", "A", "B", "A", ...)
 * - Result: FK logic might create more than 100 transactions because it processes each row
 *
 * Solution:
 * - This processor marks all source FK fields as unique=true
 * - Ensures one-to-one mapping between generated source rows and unique FK values
 * - Cardinality logic then works correctly on unique source values
 *
 * Benefits:
 * - Predictable record counts based on cardinality configuration
 * - No unexpected record multiplication due to duplicate source keys
 * - Better test data quality and FK relationship integrity
 */
class ForeignKeyUniquenessProcessor(val dataCatererConfiguration: DataCatererConfiguration)
  extends MutatingPrePlanProcessor {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override def apply(
                      plan: Plan,
                      tasks: List[Task],
                      validations: List[ValidationConfiguration]
                    ): (Plan, List[Task], List[ValidationConfiguration]) = {

    LOGGER.debug("ForeignKeyUniquenessProcessor starting...")

    // Extract foreign keys from plan's sink options
    val foreignKeys = plan.sinkOptions.map(_.foreignKeys).getOrElse(List())

    if (foreignKeys.isEmpty) {
      LOGGER.debug("No foreign keys defined, skipping uniqueness processor")
      return (plan, tasks, validations)
    }

    LOGGER.info(s"Found ${foreignKeys.size} foreign key configurations")

    // Build a map of (dataSource, step, fieldName) -> true for all source FK fields
    val sourceFieldsToMarkUnique = foreignKeys.flatMap { fk =>
      val sourceDataSource = fk.source.dataSource
      val sourceStep = fk.source.step
      val sourceFields = fk.source.fields

      sourceFields.map { fieldName =>
        (sourceDataSource, sourceStep, fieldName)
      }
    }.toSet

    LOGGER.info(s"Source FK fields to mark as unique: ${sourceFieldsToMarkUnique.size} field(s)")
    sourceFieldsToMarkUnique.foreach { case (ds, step, field) =>
      LOGGER.debug(s"  - $ds.$step.$field")
    }

    // Build task name -> data source name mapping
    val taskNameToDataSource = plan.tasks.map(ts => ts.name -> ts.dataSourceName).toMap

    // Update tasks to mark source FK fields as unique
    val updatedTasks = tasks.map { task =>
      val dataSourceName = taskNameToDataSource.get(task.name)

      dataSourceName match {
        case Some(dsName) =>
          // Update steps
          val updatedSteps = task.steps.map { step =>
            updateStepFields(step, dsName, sourceFieldsToMarkUnique)
          }

          if (updatedSteps != task.steps) {
            LOGGER.info(s"Updated task '${task.name}' to mark FK fields as unique")
            task.copy(steps = updatedSteps)
          } else {
            task
          }

        case None =>
          LOGGER.warn(s"Could not find data source name for task '${task.name}'")
          task
      }
    }

    LOGGER.info("ForeignKeyUniquenessProcessor completed")

    (plan, updatedTasks, validations)
  }

  /**
   * Update fields in a step to mark FK source fields as unique.
   */
  private def updateStepFields(
                                step: Step,
                                dataSourceName: String,
                                sourceFieldsToMarkUnique: Set[(String, String, String)]
                              ): Step = {
    if (step.fields.isEmpty) {
      // No fields defined, can't update
      if (sourceFieldsToMarkUnique.exists { case (ds, st, _) => ds == dataSourceName && st == step.name }) {
        LOGGER.warn(s"Step '${step.name}' in data source '$dataSourceName' has FK fields but no fields defined")
      }
      return step
    }

    val updatedFields = step.fields.map { field =>
      val fieldKey = (dataSourceName, step.name, field.name)

      if (sourceFieldsToMarkUnique.contains(fieldKey)) {
        // Check if already marked as unique
        val currentUnique = field.options.get(IS_UNIQUE).exists(_.toString.toLowerCase == "true")

        if (currentUnique) {
          LOGGER.debug(s"Field '$dataSourceName.${step.name}.${field.name}' is already unique, no change needed")
          field
        } else {
          LOGGER.info(s"Marking FK source field as unique: $dataSourceName.${step.name}.${field.name}")
          field.copy(options = field.options + (IS_UNIQUE -> "true"))
        }
      } else {
        // Not a source FK field, leave as-is
        field
      }
    }

    if (updatedFields != step.fields) {
      step.copy(fields = updatedFields)
    } else {
      step
    }
  }
}
