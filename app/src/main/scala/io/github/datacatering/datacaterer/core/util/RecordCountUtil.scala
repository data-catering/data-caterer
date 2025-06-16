package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.{ForeignKey, GenerationConfig, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.util.PlanImplicits.{CountOps, PerFieldCountOps}
import org.apache.log4j.Logger

object RecordCountUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def calculateNumBatches(foreignKeys: List[ForeignKey], executableTasks: List[(TaskSummary, Task)], generationConfig: GenerationConfig): (Int, Map[String, StepRecordCount]) = {
    if (executableTasks.isEmpty) return (0, Map())
    val tasks = executableTasks.map(_._2)
    val countPerStep = getCountPerStep(foreignKeys, executableTasks, generationConfig).toMap
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
            val numRecordsPerStep = generationConfig.numRecordsPerStep.get
            step.count.copy(records = Some(numRecordsPerStep)).numRecords
          } else step.count.numRecords
          val averagePerCol = step.count.perField.map(_.averageCountPerField).getOrElse(1L)
          (
            s"${task.name}_${step.name}",
            StepRecordCount(0L, (stepRecords / averagePerCol) / numBatches, stepRecords)
          )
        })).toMap
  }

  def getCountPerStep(foreignKeys: List[ForeignKey], executableTasks: List[(TaskSummary, Task)], generationConfig: GenerationConfig): List[(String, Long)] = {
    //TODO need to take into account the foreign keys defined
    //the source foreign key controls the number of records produced by the children data sources
    val allStepsWithDataSource = executableTasks.flatMap(task => task._2.steps.map(step => (task, step)))

    def getCountForStep(task: (TaskSummary, Task), step: Step): (String, Long) = {
      val stepName = s"${task._2.name}_${step.name}"
      //find if step is used in any foreign key generation
      val optGenerationForeignKey = foreignKeys.find(fk => fk.generate.exists(fkr => fkr.dataSource == task._1.dataSourceName && fkr.step == step.name))

      val stepCount = if (generationConfig.numRecordsPerStep.isDefined) {
        val numRecordsPerStep = generationConfig.numRecordsPerStep.get
        LOGGER.debug(s"Step count total is defined in generation config, overriding count total defined in step, " +
          s"data-source-name=${task._1.dataSourceName}, task-name=${task._2.name}, step-name=${step.name}, records-per-step=$numRecordsPerStep")
        step.count.copy(records = Some(numRecordsPerStep))
      } else if (optGenerationForeignKey.isDefined) {
        //then get the source of the foreign key
        val sourceFk = optGenerationForeignKey.get.source
        //then get the count of num records for the source step as it determines the count generation steps
        val optSourceFkStep = allStepsWithDataSource.find(s => s._1._1.dataSourceName == sourceFk.dataSource && s._2.name == sourceFk.step)
        //then pass via recursion
        optSourceFkStep.map(sourceFkStep => {
          val sourceFkRecords = getCountForStep(sourceFkStep._1, sourceFkStep._2)._2
          step.count.copy(records = Some(sourceFkRecords))
        }).getOrElse(step.count)
      } else {
        step.count
      }
      (stepName, stepCount.numRecords)
    }

    val baseStepCounts = executableTasks.flatMap(task => {
      task._2.steps.map(step => {
        getCountForStep(task, step)
      })
    })
    baseStepCounts
  }
}

case class StepRecordCount(currentNumRecords: Long, numRecordsPerBatch: Long, numTotalRecords: Long)
