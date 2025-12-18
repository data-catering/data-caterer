package io.github.datacatering.datacaterer.core.foreignkey.util

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.ForeignKey
import io.github.datacatering.datacaterer.core.model.ForeignKeyRelationship
import io.github.datacatering.datacaterer.core.util.ForeignKeyRelationHelper.updateForeignKeyName
import org.apache.spark.sql.Dataset

/**
 * Helper utilities for working with foreign key metadata.
 *
 * This object provides utilities for merging foreign keys from different sources
 * (metadata detection vs user-defined) and managing FK relationships during
 * metadata-driven plan/task generation.
 */
object ForeignKeyMetadataHelper {

  /**
   * Merge foreign keys detected from metadata sources with user-defined foreign keys.
   *
   * This method is used during metadata-driven generation to combine:
   * 1. Foreign keys auto-detected from data source metadata (schemas, constraints)
   * 2. Foreign keys explicitly defined by the user in their plan
   *
   * The generated foreign keys take precedence when there are conflicts, as they
   * represent actual constraints from the underlying data source that must be adhered to.
   *
   * @param dataSourceForeignKeys Foreign key relationships detected from each data source
   * @param optPlanRun Optional plan run containing user-defined foreign keys
   * @param stepNameMapping Mapping from original step names to updated step names (used when metadata sources rename steps)
   * @return Merged list of foreign keys combining generated and user-defined relationships
   */
  def getAllForeignKeyRelationships(
    dataSourceForeignKeys: List[Dataset[ForeignKeyRelationship]],
    optPlanRun: Option[PlanRun],
    stepNameMapping: Map[String, String]
  ): List[ForeignKey] = {
    // Collect and group generated foreign keys by source
    val generatedForeignKeys = dataSourceForeignKeys.flatMap(_.collect())
      .groupBy(_.key)
      .map(x => ForeignKey(x._1, x._2.map(_.foreignKey), List()))
      .toList

    // Get user-defined foreign keys and update step names if needed
    val userForeignKeys = optPlanRun.flatMap(planRun => planRun._plan.sinkOptions.map(_.foreignKeys))
      .getOrElse(List())
      .map(userFk => {
        val fkMapped = updateForeignKeyName(stepNameMapping, userFk.source)
        val subFkNamesMapped = userFk.generate.map(subFk => updateForeignKeyName(stepNameMapping, subFk))
        ForeignKey(fkMapped, subFkNamesMapped, List())
      })

    // Merge: generated FK takes precedence for existing sources
    val mergedForeignKeys = generatedForeignKeys.map(genFk => {
      userForeignKeys.find(userFk => userFk.source == genFk.source)
        .map(matchUserFk => {
          // Generated foreign key takes precedence due to constraints from underlying data source
          ForeignKey(matchUserFk.source, matchUserFk.generate ++ genFk.generate, List())
        })
        .getOrElse(genFk)
    })

    // Add user FKs that don't have generated equivalents
    val allForeignKeys = mergedForeignKeys ++ userForeignKeys.filter(userFk => !generatedForeignKeys.exists(_.source == userFk.source))
    allForeignKeys
  }
}
