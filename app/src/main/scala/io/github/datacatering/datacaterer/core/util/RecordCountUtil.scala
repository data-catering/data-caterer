package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.{GenerationConfig, Task}
import io.github.datacatering.datacaterer.core.util.PlanImplicits.{CountOps, PerColumnCountOps}
import org.apache.log4j.Logger

object RecordCountUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def calculateNumBatches(tasks: List[Task], generationConfig: GenerationConfig): (Int, Map[String, StepRecordCount]) = {
    if (tasks.isEmpty) return (0, Map())
    val countPerStep = getCountPerStep(tasks, generationConfig).toMap
    val totalRecordsToGenerate = countPerStep.values.sum
    if (totalRecordsToGenerate <= generationConfig.numRecordsPerBatch) {
      LOGGER.debug(s"Generating all records for all steps in single batch, total-records=$totalRecordsToGenerate, configured-records-per-batch=${generationConfig.numRecordsPerBatch}")
    }

    val numBatches = Math.max(Math.ceil(totalRecordsToGenerate / generationConfig.numRecordsPerBatch.toDouble).toInt, 1)
    LOGGER.info(s"Number of batches for data generation, num-batches=$numBatches, num-records-per-batch=${generationConfig.numRecordsPerBatch}, total-records=$totalRecordsToGenerate")
    val trackRecordsPerStep = stepToRecordCountMap(tasks, generationConfig, numBatches)
    (numBatches, trackRecordsPerStep)
  }

  private def stepToRecordCountMap(tasks: List[Task], generationConfig: GenerationConfig, numBatches: Long): Map[String, StepRecordCount] = {
    tasks.flatMap(task =>
      task.steps
        .map(step => {
          val stepRecords = if (generationConfig.numRecordsPerStep.isDefined) {
            val numRecordsPerStep = generationConfig.numRecordsPerStep.asInstanceOf[Option[String]].get.toLong
            step.count.copy(records = Some(numRecordsPerStep)).numRecords
          } else step.count.numRecords
          val averagePerCol = step.count.perColumn.map(_.averageCountPerColumn).getOrElse(1L)
          (
            s"${task.name}_${step.name}",
            StepRecordCount(0L, (stepRecords / averagePerCol) / numBatches, stepRecords)
          )
        })).toMap
  }

  private def getCountPerStep(tasks: List[Task], generationConfig: GenerationConfig): List[(String, Long)] = {
    //TODO need to take into account the foreign keys defined
    //the main foreign key controls the number of records produced by the children data sources
    val baseStepCounts = tasks.flatMap(task => {
      task.steps.map(step => {
        val stepName = s"${task.name}_${step.name}"
        val stepCount = if (generationConfig.numRecordsPerStep.isDefined) {
          val numRecordsPerStep = generationConfig.numRecordsPerStep.asInstanceOf[Option[String]].get
          LOGGER.debug(s"Step count total is defined in generation config, overriding count total defined in step, " +
            s"task-name=${task.name}, step-name=${step.name}, records-per-step=$numRecordsPerStep")
          step.count.copy(records = Some(numRecordsPerStep.toLong))
        } else {
          step.count
        }
        (stepName, stepCount.numRecords)
      })
    })
    baseStepCounts
  }
}

case class StepRecordCount(currentNumRecords: Long, numRecordsPerBatch: Long, numTotalRecords: Long)
