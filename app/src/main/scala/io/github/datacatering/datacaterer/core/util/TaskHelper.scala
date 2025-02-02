package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.connection.{ConnectionTaskBuilder, NoopBuilder}
import io.github.datacatering.datacaterer.api.model.Constants.{FOREIGN_KEY_DELIMITER, FOREIGN_KEY_DELIMITER_REGEX, SPECIFIC_DATA_SOURCE_OPTIONS}
import io.github.datacatering.datacaterer.api.model.{Count, Step, Task}
import io.github.datacatering.datacaterer.api.{PlanRun, StepBuilder}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.DataSourceDetail
import org.apache.log4j.Logger

import scala.language.implicitConversions


object TaskHelper {
  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Generates a task from the metadata of a data source. If a plan run is provided, it will attempt to match the data source
   * with the steps in the plan run to see if there are any user defined options that need to be used.
   *
   * @param optPlanRun  the optional plan run
   * @param name        the name of the data source
   * @param stepType    the type of the step
   * @param structTypes the list of struct types
   * @return a tuple containing the task and a map of the step names
   */
  def fromMetadata(optPlanRun: Option[PlanRun], name: String, stepType: String, structTypes: List[DataSourceDetail]): (Task, Map[String, String]) = {
    val baseSteps = optPlanRun.map(planRun =>
      planRun._connectionTaskBuilders
        .filter(_.connectionConfigWithTaskBuilder.dataSourceName == name)
        .flatMap(_.step.map(_.step))
        .filter(step => step.fields.nonEmpty || step.options.nonEmpty)
    ).getOrElse(Seq())
    val hasMultipleSubDataSources = if (structTypes.size > 1) true else false
    val stepsWithAdditionalMetadata = structTypes.map(structType => enrichWithUserDefinedOptions(name, stepType, structType, optPlanRun, hasMultipleSubDataSources))
    val mappedStepNames = stepsWithAdditionalMetadata.map(_._2).filter(_.isDefined).map(_.get).toMap
    val mappedStepNamesWithoutDataSource = mappedStepNames.map(m => m._1.split(FOREIGN_KEY_DELIMITER_REGEX).last -> m._2.split(FOREIGN_KEY_DELIMITER_REGEX).last)
    val stepsWithoutAdditionalMetadata = baseSteps
      .filter(step => !stepsWithAdditionalMetadata.exists(s => s._1.name == mappedStepNamesWithoutDataSource(step.name)))
      .map(s => s -> None)
      .toList
    val allSteps = stepsWithoutAdditionalMetadata ++ stepsWithAdditionalMetadata
    (Task(name, allSteps.map(_._1)), mappedStepNames)
  }

  def enrichWithUserDefinedOptions(
                                    name: String,
                                    stepType: String,
                                    generatedDetails: DataSourceDetail,
                                    optPlanRun: Option[PlanRun],
                                    hasMultipleSubDataSources: Boolean
                                  ): (Step, Option[(String, String)]) = {
    val stepName = generatedDetails.dataSourceMetadata.toStepName(generatedDetails.sparkOptions)

    //check if there is any user defined step attributes that need to be used
    val optUserConf = if (optPlanRun.isDefined) {
      val planRun = optPlanRun.get
      val matchingDataSourceConfig = planRun._connectionTaskBuilders.filter(_.connectionConfigWithTaskBuilder.dataSourceName == name)
      if (matchingDataSourceConfig.size == 1) {
        stepWithOptNameMapping(matchingDataSourceConfig, stepName)
      } else if (matchingDataSourceConfig.size > 1) {
        //multiple matches, so have to match against step options as well if defined
        val matchingStepOptions = matchingDataSourceConfig.filter(dsConf => dsConf.step.isDefined && dsConf.step.get.step.options == generatedDetails.sparkOptions)
        if (matchingStepOptions.nonEmpty) {
          if (matchingStepOptions.size > 1) {
            LOGGER.warn(s"Multiple definitions of same sub data source found. Will default to taking first definition, " +
              s"data-source-name=$name, step-name=$stepName")
          }
          stepWithOptNameMapping(matchingStepOptions, stepName)
        } else {
          LOGGER.warn(s"No matching step options, defaulting to first matching data source config, data-source-name=$name, step-name=$stepName")
          stepWithOptNameMapping(matchingDataSourceConfig, stepName)
        }
      } else {
        getUserDefinedFromPlanSteps(name, generatedDetails, stepName, planRun)
      }
    } else {
      None
    }

    val count = optUserConf.flatMap(_._1.step.map(_.step.count)).getOrElse(Count())
    //there might be some schemas inside the user schema that are gathered from metadata sources like Marquez or OpenMetadata
    val optUserSchema = optUserConf.flatMap(_._1.step.map(_.step.fields))
    val generatedSchema = SchemaHelper.fromStructType(generatedDetails.structType)
    //if multiple sub data sources exist, merged schema should not create new fields from user schema
    val mergedSchema = optUserSchema.map(userSchema => SchemaHelper.mergeSchemaInfo(generatedSchema, userSchema, hasMultipleSubDataSources))
      .getOrElse(generatedSchema)
    val optUserOptions = optUserConf.flatMap(_._1.step.map(_.step.options)).getOrElse(Map())
    val mergedOptions = generatedDetails.sparkOptions ++ optUserOptions
    (Step(stepName, stepType, count, mergedOptions, mergedSchema), optUserConf.flatMap(_._2))
  }

  /**
   * If a plan run is provided, it will attempt to match the data source with the steps in the plan run to see if there
   * are any user defined options that need to be used.
   *
   * @param name             data source name
   * @param generatedDetails details auto generated from the data source or metadata
   * @param stepName         name of the step
   * @param planRun          plan run
   * @return a connection task builder with the user defined options
   */
  private def getUserDefinedFromPlanSteps(
                                           name: String,
                                           generatedDetails: DataSourceDetail,
                                           stepName: String,
                                           planRun: PlanRun
                                         ): Option[(ConnectionTaskBuilder[_], Option[(String, String)])] = {
    val planSteps = planRun._tasks.flatMap(task => task.steps)
    val dataSourceToTaskMapping = planRun._plan.tasks.map(t => t.name -> t.dataSourceName).toMap
    val taskToStepMapping = planRun._tasks.flatMap(t => t.steps.map(step => t.name -> step)).toMap
    val stepToDataSourceMapping = taskToStepMapping.map(t => t._2.name -> dataSourceToTaskMapping(t._1))

    val generateDetailsDataSourceOptions = generatedDetails.dataSourceMetadata.connectionConfig.filter(o => SPECIFIC_DATA_SOURCE_OPTIONS.contains(o._1))
    val matchingStepOptions = planSteps.filter(step => {
      val stepDataSourceOptions = step.options.filter(o => SPECIFIC_DATA_SOURCE_OPTIONS.contains(o._1))
      stepDataSourceOptions == generateDetailsDataSourceOptions
    })
    if (matchingStepOptions.nonEmpty) {
      if (matchingStepOptions.size > 1) {
        LOGGER.warn(s"Multiple definitions of same sub data source found. Will default to taking first definition, data-source-name=$name, step-name=$stepName")
      }
      val dataSourceName = stepToDataSourceMapping(matchingStepOptions.head.name)
      val connectionTaskBuilder = NoopBuilder().name(dataSourceName).step(StepBuilder(matchingStepOptions.head))
      stepWithOptNameMapping(Seq(connectionTaskBuilder), stepName)
    } else {
      LOGGER.warn(s"No matching step found for data source, data-source-name=$name, step-name=$stepName")
      None
    }
  }

  private def stepWithOptNameMapping(matchingStepOptions: Seq[ConnectionTaskBuilder[_]], stepName: String): Some[(ConnectionTaskBuilder[_], Option[(String, String)])] = {
    val matchStep = matchingStepOptions.head
    val optStepNameMapping = matchStep.step.map(s => {
      val baseStepName = s"${matchStep.connectionConfigWithTaskBuilder.dataSourceName}$FOREIGN_KEY_DELIMITER"
      (s"$baseStepName${s.step.name}", s"$baseStepName$stepName")
    })
    Some(matchStep, optStepNameMapping)
  }
}
