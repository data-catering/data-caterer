package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.connection.{ConnectionTaskBuilder, NoopBuilder}
import io.github.datacatering.datacaterer.api.model.Constants.{FOREIGN_KEY_DELIMITER, FOREIGN_KEY_DELIMITER_REGEX, SPECIFIC_DATA_SOURCE_OPTIONS}
import io.github.datacatering.datacaterer.api.model.{Count, Step, Task}
import io.github.datacatering.datacaterer.api.{PlanRun, StepBuilder}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.DataSourceDetail
import org.apache.log4j.Logger

import scala.language.implicitConversions

/**
 * Helper utility for creating tasks from metadata and enriching them with user-defined options.
 * 
 * This object provides functionality to:
 * - Generate tasks from data source metadata
 * - Match user-defined configurations with auto-generated metadata
 * - Merge user schemas and options with generated schemas
 * - Handle step name mappings for foreign key relationships
 */
object TaskHelper {
  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Generates a task from the metadata of a data source.
   * If a plan run is provided, it will attempt to match the data source with user-defined configurations.
   *
   * @param optPlanRun  Optional plan run containing user configurations
   * @param name        Name of the data source
   * @param stepType    Type of the step (e.g., json, csv, parquet)
   * @param structTypes List of data source details containing metadata
   * @return Tuple containing the generated task and map of step name mappings
   */
  def fromMetadata(
    optPlanRun: Option[PlanRun],
    name: String,
    stepType: String,
    structTypes: List[DataSourceDetail]
  ): (Task, Map[String, String]) = {

    val baseSteps = extractBaseStepsFromPlanRun(optPlanRun, name)
    val hasMultipleSubDataSources = structTypes.size > 1
    
    // Enrich each data source detail with user-defined options
    val enrichedSteps = structTypes.map { structType =>
      enrichWithUserDefinedOptions(name, stepType, structType, optPlanRun, hasMultipleSubDataSources)
    }
    
    val stepNameMappings = extractStepNameMappings(enrichedSteps)
    val stepsWithoutMetadata = findStepsWithoutMetadata(baseSteps, enrichedSteps, stepNameMappings)
    
    val allSteps = (stepsWithoutMetadata ++ enrichedSteps).map(_._1)
    (Task(name, allSteps), stepNameMappings)
  }

  /**
   * Enriches a step with user-defined options by matching against configurations in the plan run.
   *
   * @param name                      Name of the data source
   * @param stepType                  Type of the step
   * @param generatedDetails          Auto-generated data source details
   * @param optPlanRun               Optional plan run containing user configurations
   * @param hasMultipleSubDataSources Whether this data source has multiple sub-sources
   * @return Tuple containing the enriched step and optional name mapping
   */
  def enrichWithUserDefinedOptions(
    name: String,
    stepType: String,
    generatedDetails: DataSourceDetail,
    optPlanRun: Option[PlanRun],
    hasMultipleSubDataSources: Boolean
  ): (Step, Option[(String, String)]) = {
    
    val stepName = generatedDetails.dataSourceMetadata.toStepName(generatedDetails.sparkOptions)
    val userConfig = findUserConfiguration(name, stepName, generatedDetails, optPlanRun)
    
    val mergedFields = mergeUserAndGeneratedSchemas(generatedDetails, userConfig, hasMultipleSubDataSources)
    val mergedOptions = mergeUserAndGeneratedOptions(generatedDetails, userConfig, hasMultipleSubDataSources)
    val stepNameMapping = extractNameMapping(generatedDetails, userConfig)
    val userCount = extractUserCount(userConfig)
    
    val finalStep = Step(
      stepName,
      stepType,
      userCount,
      mergedOptions,
      mergedFields
    )
    
    (finalStep, stepNameMapping)
  }

  // ============================================================================
  // Private helper methods - Each with a single, clear responsibility
  // ============================================================================

  /**
   * Extracts base steps from the plan run for a given data source name.
   */
  private def extractBaseStepsFromPlanRun(optPlanRun: Option[PlanRun], name: String): Seq[Step] = {
    optPlanRun.map { planRun =>
      val javaScalaApiSteps = planRun._connectionTaskBuilders
        .filter(_.connectionConfigWithTaskBuilder.dataSourceName == name)
        .flatMap(_.step.map(_.step))
        .filter(step => step.fields.nonEmpty || step.options.nonEmpty)
      
      // FIXED: Filter YAML steps by data source name to prevent cross-contamination
      // Create a mapping from task names to data source names to filter YAML steps correctly
      val taskToDataSourceMapping = planRun._plan.tasks.map(t => t.name -> t.dataSourceName).toMap
      
      val yamlSteps = planRun._tasks
        .filter(task => taskToDataSourceMapping.get(task.name).contains(name))
        .flatMap(_.steps)
        .filter(step => step.fields.nonEmpty || step.options.nonEmpty)

      javaScalaApiSteps ++ yamlSteps
    }.getOrElse(Seq.empty)
  }

  /**
   * Extracts step name mappings from enriched steps.
   */
  private def extractStepNameMappings(enrichedSteps: List[(Step, Option[(String, String)])]): Map[String, String] = {
    enrichedSteps
      .map(_._2)
      .filter(_.isDefined)
      .map(_.get)
      .toMap
  }

  /**
   * Finds steps from base steps that don't have corresponding metadata.
   */
  private def findStepsWithoutMetadata(
    baseSteps: Seq[Step],
    enrichedSteps: List[(Step, Option[(String, String)])],
    stepNameMappings: Map[String, String]
  ): List[(Step, Option[(String, String)])] = {
    
    val mappedStepNamesWithoutDataSource = stepNameMappings.map { case (k, v) =>
      k.split(FOREIGN_KEY_DELIMITER_REGEX).last -> v.split(FOREIGN_KEY_DELIMITER_REGEX).last
    }

    baseSteps
      .filter { step =>
        !enrichedSteps.exists(s => s._1.name == mappedStepNamesWithoutDataSource.getOrElse(step.name, step.name))
      }
      .map(step => step -> None)
      .toList
  }

  /**
   * Finds user configuration that matches the generated data source details.
   */
  private def findUserConfiguration(
    name: String,
    stepName: String,
    generatedDetails: DataSourceDetail,
    optPlanRun: Option[PlanRun]
  ): Option[(ConnectionTaskBuilder[_], Option[(String, String)])] = {
    
    val result = optPlanRun.flatMap { planRun =>
      val javaScalaResult = findJavaScalaApiConfig(name, stepName, generatedDetails, planRun)
      
      if (javaScalaResult.isDefined) {
        javaScalaResult
      } else {
        val yamlResult = findYamlConfig(name, stepName, generatedDetails, planRun)
        yamlResult
      }
    }
    
    result
  }

  /**
   * Finds matching configuration from Java/Scala API connection task builders.
   */
  private def findJavaScalaApiConfig(
    name: String,
    stepName: String,
    generatedDetails: DataSourceDetail,
    planRun: PlanRun
  ): Option[(ConnectionTaskBuilder[_], Option[(String, String)])] = {
    
    val matchingConfigs = planRun._connectionTaskBuilders
      .filter(_.connectionConfigWithTaskBuilder.dataSourceName == name)

    matchingConfigs.size match {
      case 0 => None
      case 1 => Some(createConnectionTaskBuilderWithMapping(matchingConfigs.head, stepName))
      case _ => handleMultipleMatches(matchingConfigs, generatedDetails, stepName, name)
    }
  }

  /**
   * Handles the case where multiple configuration matches are found.
   */
  private def handleMultipleMatches(
    matchingConfigs: Seq[ConnectionTaskBuilder[_]],
    generatedDetails: DataSourceDetail,
    stepName: String,
    name: String
  ): Option[(ConnectionTaskBuilder[_], Option[(String, String)])] = {
    
    val matchingStepOptions = matchingConfigs.filter { config =>
      config.step.isDefined && config.step.get.step.options == generatedDetails.sparkOptions
    }

    if (matchingStepOptions.nonEmpty) {
      if (matchingStepOptions.size > 1) {
        LOGGER.warn(s"Multiple definitions of same sub data source found. Using first definition, " +
          s"data-source-name=$name, step-name=$stepName")
      }
      Some(createConnectionTaskBuilderWithMapping(matchingStepOptions.head, stepName))
    } else {
      LOGGER.warn(s"No matching step options, defaulting to first matching data source config, " +
        s"data-source-name=$name, step-name=$stepName")
      Some(createConnectionTaskBuilderWithMapping(matchingConfigs.head, stepName))
    }
  }

  /**
   * Finds matching configuration from YAML plan steps.
   */
  private def findYamlConfig(
    name: String,
    stepName: String,
    generatedDetails: DataSourceDetail,
    planRun: PlanRun
  ): Option[(ConnectionTaskBuilder[_], Option[(String, String)])] = {
    
    val planSteps = planRun._tasks.flatMap(_.steps)
    val dataSourceMappings = createDataSourceMappings(planRun)
    
    val matchingSteps = findMatchingSteps(planSteps, generatedDetails)
    
    if (matchingSteps.nonEmpty) {
      if (matchingSteps.size > 1) {
        LOGGER.warn(s"Multiple definitions of same sub data source found. Using first definition, " +
          s"data-source-name=$name, step-name=$stepName")
      }
      
      val matchingStep = matchingSteps.head
      val dataSourceName = dataSourceMappings.getOrElse(matchingStep.name, name)
      val connectionTaskBuilder = NoopBuilder().name(dataSourceName).step(StepBuilder(matchingStep))
      
      Some(createConnectionTaskBuilderWithMapping(connectionTaskBuilder, stepName))
    } else {
      LOGGER.warn(s"No matching step found for data source, data-source-name=$name, step-name=$stepName")
      None
    }
  }

  /**
   * Creates mappings between steps and data sources from the plan run.
   */
  private def createDataSourceMappings(planRun: PlanRun): Map[String, String] = {
    val dataSourceToTaskMapping = planRun._plan.tasks.map(t => t.name -> t.dataSourceName).toMap
    val taskToStepMapping = planRun._tasks.flatMap(t => t.steps.map(step => t.name -> step)).toMap
    taskToStepMapping.map { case (taskName, step) =>
      step.name -> dataSourceToTaskMapping(taskName)
    }
  }

  /**
   * Finds steps that match the generated details based on data source options.
   */
  private def findMatchingSteps(planSteps: Seq[Step], generatedDetails: DataSourceDetail): Seq[Step] = {
    val generatedOptions = generatedDetails.dataSourceMetadata.connectionConfig
      .filter { case (key, _) => SPECIFIC_DATA_SOURCE_OPTIONS.contains(key) }

    planSteps.filter { step =>
      val stepOptions = step.options.filter { case (key, _) => SPECIFIC_DATA_SOURCE_OPTIONS.contains(key) }
      generatedOptions.forall { case (key, value) =>
        stepOptions.contains(key) && stepOptions(key) == value
      }
    }
  }

  /**
   * Creates a connection task builder with step name mapping.
   */
  private def createConnectionTaskBuilderWithMapping(
    connectionTaskBuilder: ConnectionTaskBuilder[_],
    stepName: String
  ): (ConnectionTaskBuilder[_], Option[(String, String)]) = {
    
    val nameMapping = connectionTaskBuilder.step.map { stepBuilder =>
      val baseStepName = s"${connectionTaskBuilder.connectionConfigWithTaskBuilder.dataSourceName}$FOREIGN_KEY_DELIMITER"
      (s"$baseStepName${stepBuilder.step.name}", s"$baseStepName$stepName")
    }
    
    (connectionTaskBuilder, nameMapping)
  }

  /**
   * Extracts user count from configuration.
   */
  private def extractUserCount(userConfig: Option[(ConnectionTaskBuilder[_], Option[(String, String)])]): Count = {
    userConfig
      .flatMap(_._1.step.map(_.step.count))
      .getOrElse(Count())
  }

  /**
   * Merges user-defined schema with generated schema.
   */
  private def mergeUserAndGeneratedSchemas(
    generatedDetails: DataSourceDetail,  
    userConfig: Option[(ConnectionTaskBuilder[_], Option[(String, String)])],
    hasMultipleSubDataSources: Boolean
  ): List[io.github.datacatering.datacaterer.api.model.Field] = {
    
    val generatedSchema = SchemaHelper.fromStructType(generatedDetails.structType)
    val userSchema = userConfig.flatMap(_._1.step.map(_.step.fields))
    
    // Debug logging to see what's being passed to merging
    val result = userSchema.map(userFields => {
      SchemaHelper.mergeSchemaInfo(generatedSchema, userFields, hasMultipleSubDataSources)
    }).getOrElse(generatedSchema)
    
    result
  }

  /**
   * Merges user-defined options with generated options.
   */
  private def mergeUserAndGeneratedOptions(
    generatedDetails: DataSourceDetail,
    userConfig: Option[(ConnectionTaskBuilder[_], Option[(String, String)])],
    hasMultipleSubDataSources: Boolean
  ): Map[String, String] = {
    
    val userOptions = userConfig
      .flatMap(_._1.step.map(_.step.options))
      .getOrElse(Map.empty)
    
    generatedDetails.sparkOptions ++ userOptions
  }

  /**
   * Extracts name mapping from user configuration.
   */
  private def extractNameMapping(
    generatedDetails: DataSourceDetail,
    userConfig: Option[(ConnectionTaskBuilder[_], Option[(String, String)])]
  ): Option[(String, String)] = {
    userConfig.flatMap(_._2)
  }
}
