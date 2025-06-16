package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.DEFAULT_ENABLE_UNIQUE_CHECK_ONLY_WITHIN_BATCH
import io.github.datacatering.datacaterer.api.model.{ForeignKeyRelation, GenerationConfig, Plan, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.util.PlanImplicits.{PerFieldCountOps, StepOps}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{array, col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.util.sketch.BloomFilter

class UniqueFieldsUtil(
                        plan: Plan,
                        executableTasks: List[(TaskSummary, Task)],
                        enableUniqueCheckOnlyInBatch: Boolean = DEFAULT_ENABLE_UNIQUE_CHECK_ONLY_WITHIN_BATCH,
                        generationConfig: GenerationConfig = GenerationConfig()
                      )(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  var uniqueFieldsDf: Map[UniqueFields, Broadcast[BloomFilter]] = getUniqueFields

  def getUniqueFieldsValues(dataSourceStep: String, df: DataFrame, step: Step): DataFrame = {
    LOGGER.debug(s"Only keeping unique values for generated data, data-source-step=$dataSourceStep")
    //get all the unique values that have been generated for each field so far
    val existingFieldValues = uniqueFieldsDf.filter(uniqueDf => uniqueDf._1.getDataSourceName == dataSourceStep)
    val initialDf = df // Rename df to initialDf to avoid confusion
    if (!initialDf.storageLevel.useMemory) initialDf.cache()

    val finalDf = existingFieldValues.foldLeft(initialDf) { (currentDf, previouslyGenerated) =>
      val fields = previouslyGenerated._1.fields
      LOGGER.debug(s"Only keeping unique values for generated data for fields, " +
        s"data-source-step=$dataSourceStep, fields=${fields.mkString(",")}")

      //check if it is a nested field, need to bring field to top level before dropping duplicates
      val dfWithUnique = if (fields.exists(c => c.contains("."))) {
        LOGGER.debug("Nested fields exist, required to bring to top level data frame before dropping duplicates")
        val mappedCols = fields.map(c => if (c.contains(".")) c -> s"_dedup_$c" else c -> c).toMap
        val dedupCols = mappedCols.values.filter(c => c.startsWith("_dedup_")).toList
        currentDf.withColumns(mappedCols.filter(_._2.startsWith("_dedup_")).map(c => c._2 -> col(c._1)))
          .dropDuplicates(mappedCols.values.toList)
          .drop(dedupCols: _*)
      } else {
        currentDf.dropDuplicates(fields)
      }
      if (!dfWithUnique.storageLevel.useMemory) dfWithUnique.cache()

      val filterGlobalUniqueDf = if (!enableUniqueCheckOnlyInBatch) {
        filterUniqueRecords(dfWithUnique, previouslyGenerated)
      } else {
        dfWithUnique
      }
      if (!filterGlobalUniqueDf.storageLevel.useMemory) filterGlobalUniqueDf.cache()
      dfWithUnique.unpersist()

      val resultDf = if (step.count.perField.isDefined) {
        getUniqueWithPerFieldCount(step, previouslyGenerated, filterGlobalUniqueDf)
      } else {
        filterGlobalUniqueDf
      }
      resultDf.cache()
      resultDf
    }

    finalDf.cache()
    finalDf
  }

  private def getUniqueWithPerFieldCount(
                                          step: Step,
                                          previouslyGenerated: (UniqueFields, Broadcast[BloomFilter]),
                                          dfWithUnique: Dataset[Row]
                                        ): DataFrame = {
    LOGGER.debug(s"Per field count is defined, removing any records with number of rows greater than max, " +
      s"data-source-name=${previouslyGenerated._1.dataSource} step=${previouslyGenerated._1.step}")
    val perFieldCount = step.count.perField.get

    //filter out those who have num records greater than max per field
    val maxPerField = perFieldCount.maxCountPerField
    val rowsAboveMaxPerField = dfWithUnique
      .groupBy(perFieldCount.fieldNames.map(col): _*)
      .count()
      .filter(s"count > $maxPerField")
    dfWithUnique.join(rowsAboveMaxPerField, perFieldCount.fieldNames, "left_anti")
  }

  def filterUniqueRecords(df: Dataset[Row], previouslyGenerated: (UniqueFields, Broadcast[BloomFilter])): DataFrame = {
    val fields = previouslyGenerated._1.fields
    val filter = previouslyGenerated._2.value // Access the broadcast Bloom Filter
    // UDF to check if a composite key (from multiple columns) is unique
    val isUniqueUDF = udf((values: Seq[String]) => {
      val compositeKey = values.mkString("|") // Join multiple column values as a unique key
      !filter.mightContain(compositeKey) // Check in Bloom Filter
    })

    // Create a new column with the concatenated unique key
    val withUniqueKey = df.withColumn("composite_key", array(fields.map(col): _*))

    // Filter out duplicates (but not for array fields)
    val uniqueDF = withUniqueKey.filter(isUniqueUDF(col("composite_key"))).drop("composite_key")
    uniqueDF.cache()

    // Update Bloom Filter with new unique values
    uniqueDF.selectExpr(s"concat_ws('|', ${fields.mkString(", ")}) as unique_key")
      .collect()
      .foreach(row => filter.put(row.getString(0)))
    // Re-broadcast the updated Bloom Filter for future batches
    uniqueFieldsDf += (previouslyGenerated._1 -> sparkSession.sparkContext.broadcast(filter))
    uniqueDF
  }

  private def getUniqueFields: Map[UniqueFields, Broadcast[BloomFilter]] = {
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
    (foreignKeyUniqueFields ++ taskUniqueFields).map(uc => {
      val bloomFilter = BloomFilter.create(generationConfig.uniqueBloomFilterNumItems, generationConfig.uniqueBloomFilterFalsePositiveProbability)
      val bloomFilterBC = sparkSession.sparkContext.broadcast(bloomFilter)
      (uc, bloomFilterBC)
    }).toMap
  }

  def cleanup(): Unit = {
    uniqueFieldsDf.foreach(u => {
      u._2.unpersist()
      u._2.destroy()
    })
  }
}

case class UniqueFields(dataSource: String, step: String, fields: List[String]) {
  def getDataSourceName: String = s"$dataSource.$step"
}
