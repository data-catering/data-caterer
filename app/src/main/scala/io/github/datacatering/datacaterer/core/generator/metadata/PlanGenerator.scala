package io.github.datacatering.datacaterer.core.generator.metadata

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{FoldersConfig, ForeignKey, Plan, SinkOptions, Task, TaskSummary, ValidationConfiguration}
import io.github.datacatering.datacaterer.core.util.FileUtil.writeStringToFile
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object PlanGenerator {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.yamlObjectMapper

  def writeToFiles(
                    optPlanRun: Option[PlanRun],
                    tasks: List[(String, Task)],
                    foreignKeys: List[ForeignKey],
                    validationConfig: List[ValidationConfiguration],
                    foldersConfig: FoldersConfig
                  )(implicit sparkSession: SparkSession): (Plan, List[Task], List[ValidationConfiguration]) = {
    val baseFolderPath = foldersConfig.generatedPlanAndTaskFolderPath
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fileSystem.setWriteChecksum(false)
    val plan = writePlanToFile(optPlanRun, tasks, foreignKeys, s"$baseFolderPath/plan", fileSystem)
    writeTasksToFiles(tasks, s"$baseFolderPath/task", fileSystem)
    writeValidationsToFiles(validationConfig, s"$baseFolderPath/validation", fileSystem)
    fileSystem.close()
    (plan, tasks.map(_._2), validationConfig)
  }

  private def writePlanToFile(
                               optPlanRun: Option[PlanRun],
                               tasks: List[(String, Task)],
                               foreignKeys: List[ForeignKey],
                               planFolder: String,
                               fileSystem: FileSystem
                             ): Plan = {
    val currentTime = new DateTime().toString(ISODateTimeFormat.basicDateTimeNoMillis())
    val taskSummary = tasks.map(t => TaskSummary(t._2.name, t._1))
    val plan = optPlanRun.map(p => {
      val sinkOpts = p._plan.sinkOptions.map(s => SinkOptions(s.seed, s.locale, foreignKeys)).getOrElse(SinkOptions(None, None, foreignKeys))
      p._plan.copy(tasks = taskSummary, sinkOptions = Some(sinkOpts))
    }).getOrElse(Plan(s"plan_$currentTime", "Generated plan", taskSummary, Some(SinkOptions(None, None, foreignKeys))))
    val planFilePath = s"$planFolder/${plan.name}.yaml"
    LOGGER.info(s"Writing plan to file, plan=${plan.name}, num-tasks=${plan.tasks.size}, file-path=$planFilePath")
    val fileContent = OBJECT_MAPPER.writeValueAsString(plan)
    writeStringToFile(fileSystem, planFilePath, fileContent)
    plan
  }

  private def writeTasksToFiles(tasks: List[(String, Task)], taskFolder: String, fileSystem: FileSystem)(implicit sparkSession: SparkSession): Unit = {
    tasks.map(_._2).foreach(task => {
      val taskFilePath = s"$taskFolder/${task.name}_task.yaml"
      LOGGER.info(s"Writing task to file, task=${task.name}, num-steps=${task.steps.size}, file-path=$taskFilePath")
      val fileContent = OBJECT_MAPPER.writeValueAsString(task)
      writeStringToFile(fileSystem, taskFilePath, fileContent)
    })
  }

  private def writeValidationsToFiles(
                                       validationConfiguration: List[ValidationConfiguration],
                                       folder: String,
                                       fileSystem: FileSystem
                                     ): Unit = {
    validationConfiguration.zipWithIndex.foreach(vc => {
      val taskFilePath = s"$folder/validations_${vc._2}.yaml"
      val numValidations = vc._1.dataSources.flatMap(_._2.head.validations).size
      LOGGER.info(s"Writing validations to file, num-validations=$numValidations, file-path=$taskFilePath")
      val fileContent = OBJECT_MAPPER.writeValueAsString(validationConfiguration)
      writeStringToFile(fileSystem, taskFilePath, fileContent)
    })
  }
}
