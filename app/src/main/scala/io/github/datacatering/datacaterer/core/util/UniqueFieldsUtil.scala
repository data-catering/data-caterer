package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.{ForeignKeyRelation, Plan, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.util.PlanImplicits.{PerColumnCountOps, StepOps}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class UniqueFieldsUtil(plan: Plan, executableTasks: List[(TaskSummary, Task)])(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  var uniqueFieldsDf: Map[UniqueFields, DataFrame] = getUniqueFields

  def getUniqueFieldsValues(dataSourceStep: String, df: DataFrame, step: Step): DataFrame = {
    LOGGER.debug(s"Only keeping unique values for generated data, data-source-step=$dataSourceStep")
    //get all the unique values that have been generated for each column so far
    val existingFieldValues = uniqueFieldsDf.filter(uniqueDf => uniqueDf._1.getDataSourceName == dataSourceStep)
    var finalDf = df
    if (!finalDf.storageLevel.useMemory) finalDf.cache()

    //drop duplicate records for data via dropDuplicates and then anti join with previously generated values
    //need to take into account step with perColumn count defined
    existingFieldValues.foreach(previouslyGenerated => {
      val columns = previouslyGenerated._1.columns
      LOGGER.debug(s"Only keeping unique values for generated data for columns, " +
        s"data-source-step=$dataSourceStep, columns=${columns.mkString(",")}")
      val dfWithUnique = finalDf.dropDuplicates(columns)
      //if there is a perColumn count, then need to create unique set of values, then run a left semi join with original dataset
      finalDf = if (step.count.perColumn.isDefined) {
        getUniqueWithPerColumnCount(step, finalDf, previouslyGenerated, columns, dfWithUnique)
      } else if (previouslyGenerated._2.columns.nonEmpty) {
        dfWithUnique.join(previouslyGenerated._2, columns, "left_anti")
      } else {
        dfWithUnique
      }
    })

    //update the map with the latest values
    existingFieldValues.foreach(col => {
      LOGGER.debug("Updating list of existing generated record value to ensure uniqueness")
      val existingDf = uniqueFieldsDf(col._1)
      val newFieldValuesDf = finalDf.selectExpr(col._1.columns: _*)
      if (!existingDf.storageLevel.useMemory) existingDf.cache()
      if (!newFieldValuesDf.storageLevel.useMemory) newFieldValuesDf.cache()
      val combinedValuesDf = if (existingDf.isEmpty) newFieldValuesDf else newFieldValuesDf.union(existingDf)
      if (!combinedValuesDf.storageLevel.useMemory) combinedValuesDf.cache()
      uniqueFieldsDf = uniqueFieldsDf ++ Map(col._1 -> combinedValuesDf)
    })
    finalDf
  }

  private def getUniqueWithPerColumnCount(
                                           step: Step,
                                           finalDf: DataFrame,
                                           previouslyGenerated: (UniqueFields, DataFrame),
                                           columns: List[String],
                                           dfWithUnique: Dataset[Row]
                                         ): DataFrame = {
    LOGGER.debug(s"Per column count is defined, removing any records with number of rows greater than max, " +
      s"data-source-name=${previouslyGenerated._1.dataSource} step=${previouslyGenerated._1.step}")
    val perColumnCount = step.count.perColumn.get
    val dfUnique = if (previouslyGenerated._2.columns.nonEmpty) {
      dfWithUnique.join(previouslyGenerated._2, columns, "left_anti")
    } else {
      dfWithUnique
    }

    //filter out those who have num records greater than max per column
    val maxPerColumn = perColumnCount.maxCountPerColumn
    val rowsAboveMaxPerColumn = finalDf
      .groupBy(perColumnCount.columnNames.map(col): _*)
      .count()
      .filter(s"count > $maxPerColumn")
    val dfWithoutRowsAboveMaxPerColumn = finalDf.join(rowsAboveMaxPerColumn, perColumnCount.columnNames, "left_anti")
    dfWithoutRowsAboveMaxPerColumn.join(dfUnique, columns, "left_semi")
  }

  private def getUniqueFields: Map[UniqueFields, DataFrame] = {
    def uniqueFieldFromForeignKeyRelation(foreignKeyRelation: ForeignKeyRelation): UniqueFields = {
      UniqueFields(foreignKeyRelation.dataSource, foreignKeyRelation.step, foreignKeyRelation.columns)
    }

    // source foreign keys defined have to be unique fields
    val foreignKeyUniqueFields = plan.sinkOptions.map(sinkOpts => {
      sinkOpts.foreignKeys.flatMap(relationship => {
        val sourceFk = ForeignKeyRelationHelper.fromString(relationship._1)
        LOGGER.debug(s"Found foreign keys that require unique values, " +
          s"data-source-name=${sourceFk.dataSource}, step-name=${sourceFk.step}, columns=${sourceFk.columns.mkString(",")}")
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
            s"data-source-name=${t._1.dataSourceName}, step-name=${step.name}, columns=${allKeys.map(_.columns).mkString(",")}")
          allKeys
        })
    })
    (foreignKeyUniqueFields ++ taskUniqueFields).map(uc => (uc, sparkSession.emptyDataFrame)).toMap
  }

}

case class UniqueFields(dataSource: String, step: String, columns: List[String]) {
  def getDataSourceName: String = s"$dataSource.$step"
}
