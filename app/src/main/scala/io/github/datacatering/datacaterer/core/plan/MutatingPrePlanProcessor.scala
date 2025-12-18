package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, Plan, Task, ValidationConfiguration}

/**
 * Pre-plan processor that can modify the plan, tasks, or validations before execution.
 *
 * This is an enhanced version of PrePlanProcessor that supports mutations.
 * Use this when you need to adjust counts, modify configurations, or transform
 * the plan/tasks based on analysis of relationships or constraints.
 *
 * Common use cases:
 * - Adjusting task record counts based on foreign key cardinality requirements
 * - Adding computed validations based on data relationships
 * - Resolving configuration conflicts or applying defaults
 *
 * @example
 * {{{
 * class MyProcessor(val dataCatererConfiguration: DataCatererConfiguration)
 *   extends MutatingPrePlanProcessor {
 *
 *   override def apply(plan: Plan, tasks: List[Task], validations: List[ValidationConfiguration]):
 *     (Plan, List[Task], List[ValidationConfiguration]) = {
 *     val modifiedTasks = tasks.map(adjustTaskCount)
 *     (plan, modifiedTasks, validations)
 *   }
 * }
 * }}}
 */
trait MutatingPrePlanProcessor {

  val dataCatererConfiguration: DataCatererConfiguration
  val enabled: Boolean = true

  /**
   * Apply pre-processing logic and return potentially modified plan, tasks, and validations.
   *
   * @param plan The execution plan
   * @param tasks The list of tasks to be executed
   * @param validations The validation configurations
   * @return Tuple of (potentially modified plan, tasks, validations)
   */
  def apply(
    plan: Plan,
    tasks: List[Task],
    validations: List[ValidationConfiguration]
  ): (Plan, List[Task], List[ValidationConfiguration])
}
