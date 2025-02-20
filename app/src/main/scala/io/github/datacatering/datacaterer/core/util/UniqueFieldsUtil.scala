package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.{ForeignKeyRelation, Plan, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.util.PlanImplicits.{PerFieldCountOps, StepOps}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class UniqueFieldsUtil(plan: Plan, executableTasks: List[(TaskSummary, Task)])(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  var uniqueFieldsDf: Map[UniqueFields, DataFrame] = getUniqueFields

  def getUniqueFieldsValues(dataSourceStep: String, df: DataFrame, step: Step): DataFrame = {
    LOGGER.debug(s"Only keeping unique values for generated data, data-source-step=$dataSourceStep")
    //get all the unique values that have been generated for each field so far
    val existingFieldValues = uniqueFieldsDf.filter(uniqueDf => uniqueDf._1.getDataSourceName == dataSourceStep)
    var finalDf = df
    if (!finalDf.storageLevel.useMemory) finalDf.cache()

    //drop duplicate records for data via dropDuplicates and then anti join with previously generated values
    existingFieldValues.foreach(previouslyGenerated => {
      val fields = previouslyGenerated._1.fields
      LOGGER.debug(s"Only keeping unique values for generated data for fields, " +
        s"data-source-step=$dataSourceStep, fields=${fields.mkString(",")}")
      //check if it is a nested field, need to bring field to top level before dropping duplicates
      val dfWithUnique = if (fields.exists(c => c.contains("."))) {
        LOGGER.debug("Nested fields exist, required to bring to top level data frame before dropping duplicates")
        val mappedCols = fields.map(c => if (c.contains(".")) c -> s"_dedup_$c" else c -> c).toMap
        val dedupCols = mappedCols.values.filter(c => c.startsWith("_dedup_")).toList
        finalDf.withColumns(mappedCols.filter(_._2.startsWith("_dedup_")).map(c => c._2 -> col(c._1)))
          .dropDuplicates(mappedCols.values.toList)
          .drop(dedupCols: _*)
      } else {
        finalDf.dropDuplicates(fields)
      }
      //if there is a perField count, then need to create unique set of values, then run a left semi join with original dataset
      finalDf = if (step.count.perField.isDefined) {
        getUniqueWithPerFieldCount(step, finalDf, previouslyGenerated, fields, dfWithUnique)
      } else if (previouslyGenerated._2.columns.nonEmpty) {
        dfWithUnique.join(previouslyGenerated._2, fields, "left_anti")
      } else {
        dfWithUnique
      }
    })

    //update the map with the latest values
    existingFieldValues.foreach(col => {
      LOGGER.debug("Updating list of existing generated record value to ensure uniqueness")
      val existingDf = uniqueFieldsDf(col._1)
      val newFieldValuesDf = finalDf.selectExpr(col._1.fields: _*)
      if (!existingDf.storageLevel.useMemory) existingDf.cache()
      if (!newFieldValuesDf.storageLevel.useMemory) newFieldValuesDf.cache()
      val combinedValuesDf = if (existingDf.isEmpty) newFieldValuesDf else newFieldValuesDf.union(existingDf)
      if (!combinedValuesDf.storageLevel.useMemory) combinedValuesDf.cache()
      newFieldValuesDf.unpersist()
      existingDf.unpersist()
      uniqueFieldsDf = uniqueFieldsDf ++ Map(col._1 -> combinedValuesDf)
    })
    finalDf
  }

  private def getUniqueWithPerFieldCount(
                                           step: Step,
                                           finalDf: DataFrame,
                                           previouslyGenerated: (UniqueFields, DataFrame),
                                           fields: List[String],
                                           dfWithUnique: Dataset[Row]
                                         ): DataFrame = {
    LOGGER.debug(s"Per field count is defined, removing any records with number of rows greater than max, " +
      s"data-source-name=${previouslyGenerated._1.dataSource} step=${previouslyGenerated._1.step}")
    val perFieldCount = step.count.perField.get
    val dfUnique = if (previouslyGenerated._2.columns.nonEmpty) {
      dfWithUnique.join(previouslyGenerated._2, fields, "left_anti")
    } else {
      dfWithUnique
    }

    //filter out those who have num records greater than max per field
    val maxPerField = perFieldCount.maxCountPerField
    val rowsAboveMaxPerField = finalDf
      .groupBy(perFieldCount.fieldNames.map(col): _*)
      .count()
      .filter(s"count > $maxPerField")
    val dfWithoutRowsAboveMaxPerField = finalDf.join(rowsAboveMaxPerField, perFieldCount.fieldNames, "left_anti")
    dfWithoutRowsAboveMaxPerField.join(dfUnique, fields, "left_semi")
  }

  private def getUniqueFields: Map[UniqueFields, DataFrame] = {
    def uniqueFieldFromForeignKeyRelation(foreignKeyRelation: ForeignKeyRelation): UniqueFields = {
      UniqueFields(foreignKeyRelation.dataSource, foreignKeyRelation.step, foreignKeyRelation.fields)
    }

    // source foreign keys defined have to be unique fields
    val foreignKeyUniqueFields = plan.sinkOptions.map(sinkOpts => {
      sinkOpts.foreignKeys.flatMap(relationship => {
        val sourceFk = relationship.source
        LOGGER.debug(s"Found foreign keys that require unique values, " +
          s"data-source-name=${sourceFk.dataSource}, step-name=${sourceFk.step}, fields=${sourceFk.fields.mkString(",")}")
        List(uniqueFieldFromForeignKeyRelation(sourceFk))
      })
    }).getOrElse(List())

    // get unique fields defined in tasks
    val taskUniqueFields = executableTasks.flatMap(t => {
      t._2.steps
        .flatMap(step => {
          val primaryKeys = step.gatherPrimaryKeys
          val primaryKeyUf = if (primaryKeys.nonEmpty) List(UniqueFields(t._1.dataSourceName, step.name, primaryKeys)) else List()
          val uniqueKeys = step.gatherUniqueFields
          val uniqueKeyUf = if (uniqueKeys.nonEmpty) uniqueKeys.map(u => UniqueFields(t._1.dataSourceName, step.name, List(u))) else List()
          val allKeys = primaryKeyUf ++ uniqueKeyUf
          LOGGER.debug(s"Found unique fields that require unique values, " +
            s"data-source-name=${t._1.dataSourceName}, step-name=${step.name}, fields=${allKeys.map(_.fields).mkString(",")}")
          allKeys
        })
    })
    (foreignKeyUniqueFields ++ taskUniqueFields).map(uc => (uc, sparkSession.emptyDataFrame)).toMap
  }

}

case class UniqueFields(dataSource: String, step: String, fields: List[String]) {
  def getDataSourceName: String = s"$dataSource.$step"
}
