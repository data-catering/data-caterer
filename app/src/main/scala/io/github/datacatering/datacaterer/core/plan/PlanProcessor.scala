package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.{DATA_CATERER_INTERFACE_JAVA, DATA_CATERER_INTERFACE_SCALA, DATA_CATERER_INTERFACE_YAML, PLAN_CLASS, PLAN_STAGE_EXTRACT_METADATA, PLAN_STAGE_PARSE_PLAN}
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, Plan, Task, ValidationConfiguration}
import io.github.datacatering.datacaterer.core.activity.{PlanRunPostPlanProcessor, PlanRunPrePlanProcessor}
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.exception.PlanRunClassNotFoundException
import io.github.datacatering.datacaterer.core.generator.DataGeneratorProcessor
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.DataSourceMetadataFactory
import io.github.datacatering.datacaterer.core.model.Constants.METADATA_CONNECTION_OPTIONS
import io.github.datacatering.datacaterer.core.model.PlanRunResults
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.util.SparkProvider
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object PlanProcessor {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def determineAndExecutePlan(
                               optPlanRun: Option[PlanRun] = None,
                               interface: String = DATA_CATERER_INTERFACE_SCALA,
                             ): PlanRunResults = {
    val optPlanClass = getPlanClass
    optPlanClass.map(Class.forName)
      .map(cls => {
        cls.getDeclaredConstructor().newInstance()
        val tryScalaPlan = Try(cls.getDeclaredConstructor().newInstance().asInstanceOf[PlanRun])
        val tryJavaPlan = Try(cls.getDeclaredConstructor().newInstance().asInstanceOf[io.github.datacatering.datacaterer.javaapi.api.PlanRun])
        (tryScalaPlan, tryJavaPlan) match {
          case (Success(value), _) => (value, DATA_CATERER_INTERFACE_SCALA)
          case (_, Success(value)) => (value.getPlan, DATA_CATERER_INTERFACE_JAVA)
          case _ => throw PlanRunClassNotFoundException(optPlanClass.get)
        }
      })
      .map(p => executePlan(p._1, p._2))
      .getOrElse(
        optPlanRun.map(p => executePlan(p, interface))
          .getOrElse(executePlan(interface))
      )
  }

  def determineAndExecutePlanJava(planRun: io.github.datacatering.datacaterer.javaapi.api.PlanRun): PlanRunResults =
    determineAndExecutePlan(Some(planRun.getPlan))

  def executeFromYamlFiles(planFilePath: String, taskFolderPath: String): PlanRunResults = {
    val dataCatererConfiguration = ConfigParser.toDataCatererConfiguration.copy(
      foldersConfig = ConfigParser.toDataCatererConfiguration.foldersConfig.copy(
        planFilePath = planFilePath,
        taskFolderPath = taskFolderPath
      )
    )
    executePlanWithConfig(dataCatererConfiguration, None, DATA_CATERER_INTERFACE_YAML)
  }

  private def executePlan(planRun: PlanRun, interface: String): PlanRunResults = {
    val dataCatererConfiguration = planRun._configuration
    executePlanWithConfig(dataCatererConfiguration, Some(planRun), interface)
  }

  private def executePlan(interface: String): PlanRunResults = {
    val dataCatererConfiguration = ConfigParser.toDataCatererConfiguration
    executePlanWithConfig(dataCatererConfiguration, None, interface)
  }

  def executePlanWithConfig(
                                     dataCatererConfiguration: DataCatererConfiguration,
                                     optPlan: Option[PlanRun],
                                     interface: String
                                   ): PlanRunResults = {
    val connectionConf = optPlan.map(_._connectionTaskBuilders.flatMap(_.connectionConfigWithTaskBuilder.options).toMap).getOrElse(Map())
    implicit val sparkSession: SparkSession = new SparkProvider(dataCatererConfiguration.master, dataCatererConfiguration.runtimeConfig ++ connectionConf).getSparkSession
    Class.forName("org.postgresql.Driver")

    val (planRun, resolvedInterface) = parsePlan(dataCatererConfiguration, optPlan, interface)
    try {
      // Step 1: Extract metadata if configured (this may generate new plan/tasks)
      val optPlanWithTasks = extractMetadata(dataCatererConfiguration, planRun)

      // Step 2: Determine which plan/tasks to use (metadata-generated or original)
      val (basePlan, baseTasks, baseValidations) = optPlanWithTasks match {
        case Some((genPlan, genTasks, genValidation)) if genTasks.nonEmpty =>
          LOGGER.info(s"Using metadata-generated tasks: num-tasks=${genTasks.size}")
          (genPlan, genTasks, genValidation)
        case Some(_) =>
          LOGGER.info(s"Metadata extraction returned empty tasks, using plan's tasks instead: num-tasks=${planRun._tasks.size}")
          (planRun._plan, planRun._tasks, planRun._validations)
        case None =>
          LOGGER.debug(s"No metadata extraction performed, using plan's tasks: num-tasks=${planRun._tasks.size}")
          (planRun._plan, planRun._tasks, planRun._validations)
      }

      // Step 3: Apply pre-processors to modify plan/tasks (e.g., cardinality count adjustments)
      val (finalPlan, finalTasks, finalValidations) = applyMutatingPrePlanProcessors(
        basePlan, baseTasks, baseValidations, dataCatererConfiguration, resolvedInterface
      )

      LOGGER.info(s"After pre-processors: num-tasks=${finalTasks.size}")

      // Step 4: Generate data with the final modified plan/tasks
      val dataGeneratorProcessor = new DataGeneratorProcessor(dataCatererConfiguration)
      dataGeneratorProcessor.generateData(finalPlan, finalTasks, Some(finalValidations))
    } catch {
      case ex: Exception => throw ex
    }
  }

  private def extractMetadata(dataCatererConfiguration: DataCatererConfiguration, planRun: PlanRun)(implicit sparkSession: SparkSession): Option[(Plan, List[Task], List[ValidationConfiguration])] = {
    try {
      val dataSourceMetadataFactory = new DataSourceMetadataFactory(dataCatererConfiguration)
      dataSourceMetadataFactory.extractAllDataSourceMetadata(planRun)
    } catch {
      case exception: Exception =>
        handleException(exception, PLAN_STAGE_EXTRACT_METADATA)
        throw exception
    }
  }

  def parsePlanWithFiltering(dataCatererConfiguration: DataCatererConfiguration, optPlan: Option[PlanRun], interface: String, taskFilter: Option[String] = None, stepFilter: Option[String] = None)(implicit sparkSession: SparkSession): (PlanRun, String) = {
    parsePlanInternal(dataCatererConfiguration, optPlan, interface, taskFilter, stepFilter)
  }

  private def parsePlan(dataCatererConfiguration: DataCatererConfiguration, optPlan: Option[PlanRun], interface: String)(implicit sparkSession: SparkSession): (PlanRun, String) = {
    parsePlanInternal(dataCatererConfiguration, optPlan, interface)
  }

  private def parsePlanInternal(dataCatererConfiguration: DataCatererConfiguration, optPlan: Option[PlanRun], interface: String, taskFilter: Option[String] = None, stepFilter: Option[String] = None)(implicit sparkSession: SparkSession): (PlanRun, String) = {
    try {
      if (optPlan.isDefined) {
        // Check if we need to load from YAML by looking at plan task summaries vs actual tasks
        val existingTaskNames = optPlan.get._tasks.map(_.name).toSet
        val planTaskNames = optPlan.get._plan.tasks.map(_.name).toSet
        val missingTaskNames = planTaskNames.diff(existingTaskNames)

        if (missingTaskNames.nonEmpty && interface != DATA_CATERER_INTERFACE_YAML) {
          // Tasks are missing - this means the plan is from a YAML file, not the UI
          // Load the entire plan and all tasks from the YAML file
          LOGGER.debug(s"Attempting to load plan from YAML file, plan-name=${optPlan.get._plan.name}")
          val yamlConfig = ConfigParser.toDataCatererConfiguration
          
          // Find the correct YAML plan file by name in the configured plan directory
          val requestedPlanName = optPlan.get._plan.name
          val yamlPlanFilePath = findYamlPlanFile(yamlConfig.foldersConfig.planFilePath, requestedPlanName)
          
          // Load the plan from the specific YAML file if found, otherwise use default
          val planConfigForParsing = yamlPlanFilePath.map { planPath =>
            yamlConfig.copy(
              foldersConfig = yamlConfig.foldersConfig.copy(planFilePath = planPath)
            )
          }.getOrElse(yamlConfig)
          
          // Load everything from YAML - use only YAML configuration, no UI data
          val (parsedPlan, enabledTasks, validations) = PlanParser.getPlanTasksFromYaml(planConfigForParsing, false)
          
          val yamlPlanRun = new YamlPlanRun(parsedPlan, enabledTasks, validations, planConfigForParsing, taskFilter, stepFilter)
          (yamlPlanRun, DATA_CATERER_INTERFACE_YAML)
        } else {
          // All tasks are provided by UI, this is a pure UI plan
          (optPlan.get, interface)
        }
      } else {
        val (parsedPlan, enabledTasks, validations) = PlanParser.getPlanTasksFromYaml(dataCatererConfiguration)
        (new YamlPlanRun(parsedPlan, enabledTasks, validations, dataCatererConfiguration, taskFilter, stepFilter), DATA_CATERER_INTERFACE_YAML)
      }
    } catch {
      case parsePlanException: Exception =>
        handleException(parsePlanException, PLAN_STAGE_PARSE_PLAN)
        throw parsePlanException
    }
  }
  
  private def findYamlPlanFile(configuredPlanPath: String, planName: String)(implicit sparkSession: SparkSession): Option[String] = {
    import java.io.File
    
    val logger = Logger.getLogger(getClass.getName)
    
    // Get the parent directory from the configured plan file path
    val planFile = new File(configuredPlanPath)
    val planDir = if (planFile.isDirectory) planFile else new File(planFile.getParent)
    
    if (planDir.exists() && planDir.isDirectory) {
      // Look for a YAML file matching the plan name
      val matchingFiles = planDir.listFiles()
        .filter(f => f.isFile && f.getName.endsWith(".yaml"))
        .filter(f => {
          // Try to parse the plan and match by name
          Try {
            val parsed = PlanParser.parsePlan(f.getAbsolutePath)
            parsed.name.equalsIgnoreCase(planName)
          } match {
            case Success(matches) => matches
            case Failure(ex) =>
              logger.warn(s"Failed to parse YAML plan file: ${f.getAbsolutePath}", ex)
              false
          }
        })
      
      matchingFiles.headOption.map(_.getAbsolutePath)
    } else {
      None
    }
  }

  private def handleException(
                                exception: Exception,
                                stage: String = PLAN_STAGE_PARSE_PLAN,
                                optConfig: Option[DataCatererConfiguration] = None,
                                optPlanRun: Option[PlanRun] = None
                             ): Unit = {
    val planRunPostPlanProcessor = optConfig.map(config => new PlanRunPostPlanProcessor(config))
      .getOrElse(new PlanRunPostPlanProcessor(DataCatererConfiguration()))
    val plan = optPlanRun.map(_._plan).getOrElse(Plan())
    planRunPostPlanProcessor.notifyPlanResult(plan, List(), List(), stage, Some(exception))
  }

  private def getPlanClass: Option[String] = {
    val envPlanClass = System.getenv(PLAN_CLASS)
    val propPlanClass = System.getProperty(PLAN_CLASS)
    (envPlanClass, propPlanClass) match {
      case (env, _) if env != null && env.nonEmpty => Some(env)
      case (_, prop) if prop != null && prop.nonEmpty => Some(prop)
      case _ => None
    }
  }

  /**
   * Apply mutating pre-plan processors that can modify plan/tasks/validations.
   * These run after metadata extraction and before data generation.
   *
   * @return Tuple of (modified plan, modified tasks, modified validations)
   */
  private def applyMutatingPrePlanProcessors(
                                              plan: Plan,
                                              tasks: List[Task],
                                              validations: List[ValidationConfiguration],
                                              dataCatererConfiguration: DataCatererConfiguration,
                                              interface: String
                                            ): (Plan, List[Task], List[ValidationConfiguration]) = {
    try {
      // Read-only processors (for logging, monitoring, etc.)
      val readOnlyProcessors = List(new PlanRunPrePlanProcessor(dataCatererConfiguration))

      // Mutating processors (can modify plan/tasks/validations)
      // Order matters: uniqueness should be applied BEFORE cardinality adjustment
      val mutatingProcessors = List(
        new ForeignKeyUniquenessProcessor(dataCatererConfiguration),
        new CardinalityCountAdjustmentProcessor(dataCatererConfiguration)
      )

      val planWithInterface = plan.copy(runInterface = Some(interface))

      // Apply read-only processors first (don't mutate)
      readOnlyProcessors.foreach(processor => {
        if (processor.enabled) {
          processor.apply(planWithInterface, tasks, validations)
        }
      })

      // Apply mutating processors in sequence
      var currentPlan = planWithInterface
      var currentTasks = tasks
      var currentValidations = validations

      mutatingProcessors.foreach(processor => {
        if (processor.enabled) {
          val (updatedPlan, updatedTasks, updatedValidations) =
            processor.apply(currentPlan, currentTasks, currentValidations)
          currentPlan = updatedPlan
          currentTasks = updatedTasks
          currentValidations = updatedValidations
        }
      })

      (currentPlan, currentTasks, currentValidations)

    } catch {
      case preProcessorException: Exception =>
        LOGGER.error(s"Error in pre-plan processors: ${preProcessorException.getMessage}", preProcessorException)
        throw preProcessorException
    }
  }
}

class YamlPlanRun(
                   yamlPlan: Plan,
                   yamlTasks: List[Task],
                   validations: Option[List[ValidationConfiguration]],
                   dataCatererConfiguration: DataCatererConfiguration,
                   taskFilter: Option[String] = None,
                   stepFilter: Option[String] = None
                 ) extends PlanRun {
  _plan = yamlPlan
  _validations = validations.getOrElse(List())

  //get any metadata configuration from tasks for data sources and add to configuration
  private val tasksWithMetadataOptions = yamlTasks.filter(t => t.steps.nonEmpty)
    .map(t => {
      val dataSourceName = yamlPlan.tasks.find(ts => ts.name.equalsIgnoreCase(t.name)).get.dataSourceName
      dataSourceName -> t.steps.flatMap(s => s.options.filter(o => METADATA_CONNECTION_OPTIONS.contains(o._1))).toMap
    }).toMap
  private val updatedConnectionConfig = dataCatererConfiguration.connectionConfigByName
    .map(c => c._1 -> (tasksWithMetadataOptions.getOrElse(c._1, Map()) ++ c._2))

  // Apply task and step filtering if specified
  private val filteredYamlTasks = taskFilter match {
    case Some(taskName) =>
      val matchingTasks = yamlTasks.filter(_.name == taskName)
      if (matchingTasks.isEmpty) {
        throw new IllegalArgumentException(s"Task '$taskName' not found in YAML tasks. Available tasks: ${yamlTasks.map(_.name).mkString(", ")}")
      }
      matchingTasks
    case None => yamlTasks
  }

  // Apply step filtering if specified
  private val tasksWithFilteredSteps = stepFilter match {
    case Some(stepName) if taskFilter.isDefined =>
      filteredYamlTasks.map(task => {
        val matchingSteps = task.steps.filter(_.name == stepName)
        if (matchingSteps.isEmpty) {
          throw new IllegalArgumentException(s"Step '$stepName' not found in task '${task.name}'. Available steps: ${task.steps.map(_.name).mkString(", ")}")
        }
        task.copy(steps = matchingSteps)
      })
    case Some(_) =>
      throw new IllegalArgumentException("Step filter requires task filter to be specified")
    case None => filteredYamlTasks
  }

  // Merge connection configuration into task step options
  // This ensures that step.options contains all necessary connection details like 'format'
  _tasks = tasksWithFilteredSteps.map(task => {
    val dataSourceName = yamlPlan.tasks.find(ts => ts.name.equalsIgnoreCase(task.name)).get.dataSourceName
    val connectionConfig = updatedConnectionConfig.getOrElse(dataSourceName, Map())

    // Merge connection config into each step's options (connection config as base, step options override)
    val stepsWithConnectionConfig = task.steps.map(step => {
      step.copy(options = connectionConfig ++ step.options)
    })

    task.copy(steps = stepsWithConnectionConfig)
  })

  _configuration = dataCatererConfiguration.copy(connectionConfigByName = updatedConnectionConfig)
}
