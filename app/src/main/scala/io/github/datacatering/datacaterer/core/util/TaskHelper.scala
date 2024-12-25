package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{FOREIGN_KEY_DELIMITER, FOREIGN_KEY_DELIMITER_REGEX}
import io.github.datacatering.datacaterer.api.model.{Count, Step, Task}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.DataSourceDetail
import org.apache.log4j.Logger

import scala.language.implicitConversions


object TaskHelper {
  private val LOGGER = Logger.getLogger(getClass.getName)

  def fromMetadata(optPlanRun: Option[PlanRun], name: String, stepType: String, structTypes: List[DataSourceDetail]): (Task, Map[String, String]) = {
    val baseSteps = optPlanRun.map(planRun =>
      planRun._connectionTaskBuilders
        .filter(_.connectionConfigWithTaskBuilder.dataSourceName == name)
        .flatMap(_.step.map(_.step))
    ).getOrElse(Seq())
    val hasMultipleSubDataSources = if (structTypes.size > 1) true else false
    val stepsWithAdditionalMetadata = structTypes.map(structType => enrichWithUserDefinedOptions(name, stepType, structType, optPlanRun, hasMultipleSubDataSources))
    val mappedStepNames = stepsWithAdditionalMetadata.map(_._2).filter(_.isDefined).map(_.get).toMap
    val mappedStepNamesWithoutDataSource = mappedStepNames.map(m => m._1.split(FOREIGN_KEY_DELIMITER_REGEX).last -> m._2.split(FOREIGN_KEY_DELIMITER_REGEX).last)
    val stepsWithoutAdditionalMetadata = baseSteps.filter(step => !stepsWithAdditionalMetadata.exists(s => s._1.name == mappedStepNamesWithoutDataSource(step.name))).map(s => s -> None).toList
    val allSteps = stepsWithoutAdditionalMetadata ++ stepsWithAdditionalMetadata
    (Task(name, allSteps.map(_._1)), mappedStepNames)
  }

  private def enrichWithUserDefinedOptions(
                                            name: String,
                                            stepType: String,
                                            generatedDetails: DataSourceDetail,
                                            optPlanRun: Option[PlanRun],
                                            hasMultipleSubDataSources: Boolean
                                          ): (Step, Option[(String, String)]) = {
    val stepName = generatedDetails.dataSourceMetadata.toStepName(generatedDetails.sparkOptions)

    def stepWithOptNameMapping(matchingStepOptions: Seq[ConnectionTaskBuilder[_]]): Some[(ConnectionTaskBuilder[_], Option[(String, String)])] = {
      val matchStep = matchingStepOptions.head
      val optStepNameMapping = matchStep.step.map(s => {
        val baseStepName = s"${matchStep.connectionConfigWithTaskBuilder.dataSourceName}$FOREIGN_KEY_DELIMITER"
        (s"$baseStepName${s.step.name}", s"$baseStepName$stepName")
      })
      Some(matchStep, optStepNameMapping)
    }

    //check if there is any user defined step attributes that need to be used
    val optUserConf = if (optPlanRun.isDefined) {
      val matchingDataSourceConfig = optPlanRun.get._connectionTaskBuilders.filter(_.connectionConfigWithTaskBuilder.dataSourceName == name)
      if (matchingDataSourceConfig.size == 1) {
        stepWithOptNameMapping(matchingDataSourceConfig)
      } else if (matchingDataSourceConfig.size > 1) {
        //multiple matches, so have to match against step options as well if defined
        val matchingStepOptions = matchingDataSourceConfig.filter(dsConf => dsConf.step.isDefined && dsConf.step.get.step.options == generatedDetails.sparkOptions)
        if (matchingStepOptions.size == 1) {
          stepWithOptNameMapping(matchingStepOptions)
        } else {
          LOGGER.warn(s"Multiple definitions of same sub data source found. Will default to taking first definition, data-source-name=$name, step-name=$stepName")
          stepWithOptNameMapping(matchingStepOptions)
        }
      } else {
        None
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
}
