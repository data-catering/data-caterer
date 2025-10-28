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
    val trackRecordsPerStep = stepToRecordCountMap(tasks, generationConfig, numBatches, countPerStep)
    (numBatches, trackRecordsPerStep)
  }

  private def stepToRecordCountMap(tasks: List[Task], generationConfig: GenerationConfig, numBatches: Long, countPerStep: Map[String, Long]): Map[String, StepRecordCount] = {
    tasks.flatMap(task =>
      task.steps
        .map(step => {
          val stepName = s"${task.name}_${step.name}"
          // Use the pre-calculated count from countPerStep (which accounts for foreign keys)
          val stepRecords = countPerStep.getOrElse(stepName, {
            if (generationConfig.numRecordsPerStep.isDefined) {
              val numRecordsPerStep = generationConfig.numRecordsPerStep.get
              step.count.copy(records = Some(numRecordsPerStep)).numRecords
            } else step.count.numRecords
          })
          val averagePerCol = step.count.perField.map(_.averageCountPerField).getOrElse(1L)
          val adjustedStepRecords = stepRecords / averagePerCol

          // Calculate base records per batch and remainder for proper distribution
          val baseRecordsPerBatch = adjustedStepRecords / numBatches
          val remainder = adjustedStepRecords % numBatches

          // For now, use base + 1 for early batches to handle remainder
          // The actual distribution will be handled in BatchDataProcessor
          val recordsPerBatch = if (remainder > 0) baseRecordsPerBatch + 1 else baseRecordsPerBatch

          LOGGER.debug(s"Step record distribution: step=${step.name}, total-records=$adjustedStepRecords, " +
            s"base-per-batch=$baseRecordsPerBatch, remainder=$remainder, records-per-batch=$recordsPerBatch")

          (
            stepName,
            StepRecordCount(0L, recordsPerBatch, stepRecords, baseRecordsPerBatch, remainder, averagePerCol)
          )
        })).toMap
  }

  def getCountPerStep(foreignKeys: List[ForeignKey], executableTasks: List[(TaskSummary, Task)], generationConfig: GenerationConfig): List[(String, Long)] = {
    //TODO need to take into account the foreign keys defined
    //the source foreign key controls the number of records produced by the children data sources
    val allStepsWithDataSource = executableTasks.flatMap(task => task._2.steps.map(step => (task, step)))

    def getCountForStep(task: (TaskSummary, Task), step: Step): (String, Long) = {
      val stepName = s"${task._2.name}_${step.name}"

      // Check if this step is in reference mode
      val isReferenceMode = step.options.get(io.github.datacatering.datacaterer.api.model.Constants.ENABLE_REFERENCE_MODE)
        .map(_.toBoolean)
        .getOrElse(io.github.datacatering.datacaterer.api.model.Constants.DEFAULT_ENABLE_REFERENCE_MODE)

      //find if step is used in any foreign key generation
      val optGenerationForeignKey = foreignKeys.find(fk => fk.generate.exists(fkr => fkr.dataSource == task._1.dataSourceName && fkr.step == step.name))

      val stepCount = if (generationConfig.numRecordsPerStep.isDefined) {
        val numRecordsPerStep = generationConfig.numRecordsPerStep.get
        LOGGER.debug(s"Step count total is defined in generation config, overriding count total defined in step, " +
          s"data-source-name=${task._1.dataSourceName}, task-name=${task._2.name}, step-name=${step.name}, records-per-step=$numRecordsPerStep")
        step.count.copy(records = Some(numRecordsPerStep))
      } else if (isReferenceMode) {
        // Reference mode: DO NOT override based on FK source, keep the step's configured count
        // The target step should maintain its explicitly set count even when it's in a FK relationship
        // The actual count for reference mode sources will be determined when data is read
        LOGGER.debug(s"Step has reference mode enabled, using step's configured count (not deriving from FK source), " +
          s"data-source-name=${task._1.dataSourceName}, task-name=${task._2.name}, step-name=${step.name}, step-count=${step.count.numRecords}")
        step.count
      } else if (optGenerationForeignKey.isDefined) {
        //then get the source of the foreign key
        val sourceFk = optGenerationForeignKey.get.source
        //then get the count of num records for the source step as it determines the count generation steps
        val optSourceFkStep = allStepsWithDataSource.find(s => s._1._1.dataSourceName == sourceFk.dataSource && s._2.name == sourceFk.step)
        //then pass via recursion
        optSourceFkStep.map(sourceFkStep => {
          // Check if the source step is in reference mode
          val isSourceReferenceMode = sourceFkStep._2.options.get(io.github.datacatering.datacaterer.api.model.Constants.ENABLE_REFERENCE_MODE)
            .map(_.toBoolean)
            .getOrElse(io.github.datacatering.datacaterer.api.model.Constants.DEFAULT_ENABLE_REFERENCE_MODE)

          if (isSourceReferenceMode) {
            // Source is in reference mode: DO NOT override target's count based on source's default count
            // Instead, keep the target's explicitly configured count
            LOGGER.debug(s"FK source step has reference mode enabled, NOT overriding target count, " +
              s"source-data-source=${sourceFk.dataSource}, source-step=${sourceFk.step}, " +
              s"target-data-source=${task._1.dataSourceName}, target-step=${step.name}, target-count=${step.count.numRecords}")
            step.count
          } else {
            // Normal FK generation: derive count from source
            val sourceFkRecords = getCountForStep(sourceFkStep._1, sourceFkStep._2)._2
            LOGGER.debug(s"Deriving target count from FK source, " +
              s"source-data-source=${sourceFk.dataSource}, source-step=${sourceFk.step}, source-count=$sourceFkRecords, " +
              s"target-data-source=${task._1.dataSourceName}, target-step=${step.name}")
            step.count.copy(records = Some(sourceFkRecords))
          }
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

case class StepRecordCount(
  currentNumRecords: Long, 
  numRecordsPerBatch: Long, 
  numTotalRecords: Long,
  baseRecordsPerBatch: Long = 0L,
  remainder: Long = 0L,
  averagePerCol: Long = 1L
)
