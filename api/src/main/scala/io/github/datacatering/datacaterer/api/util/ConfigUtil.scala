package io.github.datacatering.datacaterer.api.util

import io.github.datacatering.datacaterer.api.model.{PlanResults, PlanRunSummary, Step, Task}

object ConfigUtil {

  private val baseCleanseOptionKeys = Set("password", "token", "secret", "private")
  private val additionalCleanseOptionKeys = Set("user", "url", "uri", "server", "endpoint", "ssl")
  private val allCleanseKeys = baseCleanseOptionKeys ++ additionalCleanseOptionKeys

  def cleanseOptions(config: Map[String, String]): Map[String, String] = {
    config.filter(o =>
      !(baseCleanseOptionKeys.contains(o._1.toLowerCase) || o._2.toLowerCase.contains("password"))
    )
  }

  private def cleanseAdditionalOptions(config: Map[String, String]): Map[String, String] = {
    config.filter(o =>
      !(allCleanseKeys.contains(o._1.toLowerCase) || o._2.toLowerCase.contains("password"))
    )
  }

  def cleanseOptions(planRunSummary: PlanRunSummary): PlanRunSummary = {
    val cleanTasksOptions = cleanTasks(planRunSummary.tasks)
    val cleanValidations = planRunSummary.validations.map(validConfig => {
      val cleanDataSources = validConfig.dataSources.map(dsV => {
        dsV._1 -> dsV._2.map(dataSourceValid => {
          val cleanOpts = cleanseAdditionalOptions(dataSourceValid.options)
          dataSourceValid.copy(options = cleanOpts)
        })
      })
      validConfig.copy(dataSources = cleanDataSources)
    })

    planRunSummary.copy(tasks = cleanTasksOptions, validations = cleanValidations)
  }

  def cleanOptions(planResults: PlanResults): PlanResults = {
    val cleanGenerationRes = planResults.generationResult.map(dataSourceRes => {
      val cleanSinkResOpts = cleanseAdditionalOptions(dataSourceRes.sinkResult.options)
      val cleanSinkResult = dataSourceRes.sinkResult.copy(options = cleanSinkResOpts)
      dataSourceRes.copy(
        sinkResult = cleanSinkResult,
        step = cleanStep(dataSourceRes.step),
        task = cleanTask(dataSourceRes.task)
      )
    })
    val cleanValidationRes = planResults.validationResult.map(validationConfig => {
      val cleanValidations = validationConfig.dataSourceValidationResults.map(dataSourceValidation => {
        val cleanDataSourceOpts = cleanseAdditionalOptions(dataSourceValidation.options)
        dataSourceValidation.copy(options = cleanDataSourceOpts)
      })
      validationConfig.copy(dataSourceValidationResults = cleanValidations)
    })

    planResults.copy(generationResult = cleanGenerationRes, validationResult = cleanValidationRes)
  }

  private def cleanTasks(tasks: List[Task]) = {
    tasks.map(cleanTask)
  }

  private def cleanTask(task: Task) = {
    val cleanSteps = task.steps.map(cleanStep)
    task.copy(steps = cleanSteps)
  }

  private def cleanStep(step: Step) = {
    val cleanOpts = cleanseAdditionalOptions(step.options)
    step.copy(options = cleanOpts)
  }


}
