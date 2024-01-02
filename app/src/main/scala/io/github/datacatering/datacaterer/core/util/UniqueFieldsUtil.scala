package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.{Task, TaskSummary}
import PlanImplicits.StepOps
import org.apache.spark.sql.{DataFrame, SparkSession}

class UniqueFieldsUtil(executableTasks: List[(TaskSummary, Task)])(implicit sparkSession: SparkSession) {

  var uniqueFieldsDf: Map[UniqueFields, DataFrame] = getUniqueFields

  def getUniqueFieldsValues(dataSourceStep: String, df: DataFrame): DataFrame = {
    //get all the unique values that have been generated for each column so far
    val existingFieldValues = uniqueFieldsDf.filter(uniqueDf => uniqueDf._1.getDataSourceName == dataSourceStep)
    var finalDf = df
    if (!finalDf.storageLevel.useMemory) finalDf.cache()

    //drop duplicate records for data via dropDuplicates and then anti join with previously generated values
    existingFieldValues.foreach(existingCol => {
      val columns = existingCol._1.columns
      val dfWithUnique = finalDf.dropDuplicates(columns)
      finalDf = if (existingCol._2.columns.nonEmpty) dfWithUnique.join(existingCol._2, columns, "left_anti") else dfWithUnique
    })

    //update the map with the latest values
    existingFieldValues.foreach(col => {
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

  private def getUniqueFields: Map[UniqueFields, DataFrame] = {
    val uniqueFields = executableTasks.flatMap(t => {
      t._2.steps
        .flatMap(step => {
          val primaryKeys = step.gatherPrimaryKeys
          val primaryKeyUf = if (primaryKeys.nonEmpty) List(UniqueFields(t._1.dataSourceName, step.name, primaryKeys)) else List()
          val uniqueKeys = step.gatherUniqueFields
          val uniqueKeyUf = if (uniqueKeys.nonEmpty) uniqueKeys.map(u => UniqueFields(t._1.dataSourceName, step.name, List(u))) else List()
          primaryKeyUf ++ uniqueKeyUf
        })
    })
    uniqueFields.map(uc => (uc, sparkSession.emptyDataFrame)).toMap
  }

}

case class UniqueFields(dataSource: String, step: String, columns: List[String]) {
  def getDataSourceName: String = s"$dataSource.$step"
}
