package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, DataSourceValidation, Plan, Task, UpstreamDataSourceValidation, ValidationConfiguration, YamlUpstreamDataSourceValidation, YamlValidationConfiguration}
import io.github.datacatering.datacaterer.api.{ConnectionConfigWithTaskBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.exception.DataValidationMissingUpstreamDataSourceException
import io.github.datacatering.datacaterer.core.parser.unified.{UnifiedConfigConverter, UnifiedConfigDetector, UnifiedYamlParser}
import io.github.datacatering.datacaterer.core.util.FileUtil.{getFileContentFromFileSystem, isCloudStoragePath}
import io.github.datacatering.datacaterer.core.util.{FileUtil, ObjectMapperUtil}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.{Failure, Success, Try}

object PlanParser {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.yamlObjectMapper

  // ==================== Main Entry Points ====================

  /**
   * Enhanced version that allows custom folder paths for testing.
   * Detects unified YAML format and handles accordingly.
   */
  def getPlanTasksFromYaml(
                            dataCatererConfiguration: DataCatererConfiguration,
                            enabledOnly: Boolean = true,
                            planName: Option[String] = None,
                            customTaskFolderPath: Option[String] = None,
                            customValidationFolderPath: Option[String] = None
                          )(implicit sparkSession: SparkSession): (Plan, List[Task], Option[List[ValidationConfiguration]]) = {
    val planFilePath = dataCatererConfiguration.foldersConfig.planFilePath

    // Check for unified format first
    if (UnifiedConfigDetector.isUnifiedFormat(planFilePath)) {
      LOGGER.info(s"Detected unified YAML format, parsing: $planFilePath")
      return parseUnifiedConfig(planFilePath, enabledOnly)
    }

    val effectiveTaskFolderPath = customTaskFolderPath.getOrElse(dataCatererConfiguration.foldersConfig.taskFolderPath)
    val effectiveValidationFolderPath = customValidationFolderPath.getOrElse(dataCatererConfiguration.foldersConfig.validationFolderPath)

    getPlanTasksFromYamlWithPaths(
      dataCatererConfiguration,
      enabledOnly,
      planName,
      effectiveTaskFolderPath,
      effectiveValidationFolderPath
    )
  }

  /**
   * Parse a unified YAML configuration file
   */
  def parseUnifiedConfig(
                          filePath: String,
                          enabledOnly: Boolean = true
                        )(implicit sparkSession: SparkSession): (Plan, List[Task], Option[List[ValidationConfiguration]]) = {
    val unifiedConfig = UnifiedYamlParser.parse(filePath)
    val (plan, tasks, validations, _) = UnifiedConfigConverter.convert(unifiedConfig)

    val filteredTasks = if (enabledOnly) {
      val enabledTaskNames = plan.tasks.filter(_.enabled).map(_.name).toSet
      tasks.filter(t => enabledTaskNames.contains(t.name))
    } else {
      tasks
    }

    LOGGER.info(s"Parsed unified config: plan=${plan.name}, tasks=${filteredTasks.size}, validations=${validations.map(_.size).getOrElse(0)}")
    (plan, filteredTasks, validations)
  }

  private def getPlanTasksFromYamlWithPaths(
                                             dataCatererConfiguration: DataCatererConfiguration,
                                             enabledOnly: Boolean,
                                             planName: Option[String],
                                             taskFolderPath: String,
                                             validationFolderPath: String
                                           )(implicit sparkSession: SparkSession): (Plan, List[Task], Option[List[ValidationConfiguration]]) = {
    val parsedPlan = planName match {
      case Some(name) =>
        findYamlPlanFile(dataCatererConfiguration.foldersConfig.planFilePath, name) match {
          case Some(planPath) => parsePlan(planPath)
          case None =>
            LOGGER.warn(s"YAML plan file not found for plan name: $name, using default plan file: ${dataCatererConfiguration.foldersConfig.planFilePath}")
            parsePlan(dataCatererConfiguration.foldersConfig.planFilePath)
        }
      case None => parsePlan(dataCatererConfiguration.foldersConfig.planFilePath)
    }

    LOGGER.debug(s"Parsed plan from YAML: name=${parsedPlan.name}, num-tasks=${parsedPlan.tasks.size}, task-names=${parsedPlan.tasks.map(_.name).mkString(", ")}")
    val enabledPlannedTasks = if (enabledOnly) parsedPlan.tasks.filter(_.enabled) else parsedPlan.tasks
    val enabledTaskMap = enabledPlannedTasks.map(t => (t.name, t)).toMap
    val planWithEnabledTasks = parsedPlan.copy(tasks = enabledPlannedTasks)

    val tasks = parseTasksFromFolder(taskFolderPath)
    LOGGER.debug(s"Parsed tasks from folder: task-folder=$taskFolderPath, num-tasks=${tasks.length}, task-names=${tasks.map(_.name).mkString(", ")}")
    val enabledTasks = tasks.filter(t => enabledTaskMap.contains(t.name)).toList
    LOGGER.debug(s"Filtered enabled tasks: num-enabled-tasks=${enabledTasks.size}, enabled-task-names=${enabledTasks.map(_.name).mkString(", ")}")

    val validations = if (dataCatererConfiguration.flagsConfig.enableValidation) {
      Some(parseValidations(validationFolderPath, dataCatererConfiguration.connectionConfigByName))
    } else None
    (planWithEnabledTasks, enabledTasks, validations)
  }

  // ==================== Plan Parsing ====================

  /**
   * Parse a plan file without requiring SparkSession (for simpler use cases)
   */
  def parsePlanFile(planFilePath: String): Plan = {
    if (isCloudStoragePath(planFilePath)) {
      throw new IllegalArgumentException("Cloud storage paths require SparkSession - use parsePlan() method instead")
    }
    val planFile = new File(planFilePath)
    if (!planFile.exists()) {
      throw new java.io.FileNotFoundException(s"Plan file not found: $planFilePath")
    }
    val parsedPlan = OBJECT_MAPPER.readValue(planFile, classOf[Plan])
    LOGGER.debug(s"Found plan file and parsed successfully, plan-file-path=$planFilePath, plan-name=${parsedPlan.name}, plan-description=${parsedPlan.description}")
    parsedPlan
  }

  def parsePlan(planFilePath: String)(implicit sparkSession: SparkSession): Plan = {
    val parsedPlan = if (isCloudStoragePath(planFilePath)) {
      val fileContent = getFileContentFromFileSystem(FileSystem.get(sparkSession.sparkContext.hadoopConfiguration), planFilePath)
      OBJECT_MAPPER.readValue(fileContent, classOf[Plan])
    } else {
      val planFile = FileUtil.getFile(planFilePath)
      OBJECT_MAPPER.readValue(planFile, classOf[Plan])
    }
    LOGGER.debug(s"Found plan file and parsed successfully, plan-file-path=$planFilePath, plan-name=${parsedPlan.name}, plan-description=${parsedPlan.description}")
    parsedPlan
  }

  // ==================== Task Parsing ====================

  /**
   * Parse all tasks from a folder, with configurable folder path for testing
   */
  def parseTasksFromFolder(taskFolderPath: String)(implicit sparkSession: SparkSession): Array[Task] = {
    if (isCloudStoragePath(taskFolderPath)) {
      YamlFileParser.parseFiles[Task](taskFolderPath)
        .map(TaskConversionRegistry.applyTaskConversions(_))
    } else {
      val yamlFiles = findYamlFiles(taskFolderPath)
      yamlFiles.map(file => parseTaskFile(file)).toArray
    }
  }

  /**
   * Parse a single YAML task file with full field conversion (default behavior)
   */
  def parseTaskFile(taskFile: File): Task = {
    val rawTask = OBJECT_MAPPER.readValue(taskFile, classOf[Task])
    TaskConversionRegistry.applyTaskConversions(rawTask)
  }

  /**
   * Parse a single YAML task file from path with full field conversion (default behavior)
   */
  def parseTaskFile(taskFilePath: String)(implicit sparkSession: SparkSession): Task = {
    val taskFile = FileUtil.getFile(taskFilePath)
    parseTaskFile(taskFile)
  }

  /**
   * Parse a single YAML task file with optional field conversion
   */
  private def parseTaskFileInternal(taskFile: File, withFieldConversion: Boolean): Task = {
    val rawTask = OBJECT_MAPPER.readValue(taskFile, classOf[Task])
    TaskConversionRegistry.applyTaskConversions(rawTask, includeFieldConversions = withFieldConversion)
  }

  /**
   * Parse a single YAML task file from path with optional field conversion
   */
  private def parseTaskFileInternal(taskFilePath: String, withFieldConversion: Boolean): Task = {
    val taskFile = new File(taskFilePath)
    if (!taskFile.exists()) {
      throw new java.io.FileNotFoundException(s"Task file not found: $taskFilePath")
    }
    parseTaskFileInternal(taskFile, withFieldConversion)
  }

  /**
   * Parse YAML content as a Task with optional field conversion
   */
  def parseTaskFromContent(yamlContent: String, withFieldConversion: Boolean = true): Task = {
    val rawTask = OBJECT_MAPPER.readValue(yamlContent, classOf[Task])
    TaskConversionRegistry.applyTaskConversions(rawTask, includeFieldConversions = withFieldConversion)
  }

  /**
   * Parse a single YAML task file without field conversion (for backward compatibility)
   */
  def parseTaskFileSimple(taskFilePath: String): Task = {
    parseTaskFileInternal(taskFilePath, withFieldConversion = false)
  }

  /**
   * Parse all tasks from folder (for backward compatibility)
   */
  def parseTasks(taskFolderPath: String)(implicit sparkSession: SparkSession): Array[Task] = {
    parseTasksFromFolder(taskFolderPath)
  }

  /**
   * Find a task by name with custom folder path
   */
  def findTaskByName(taskName: String, taskFolderPath: String)(implicit sparkSession: SparkSession): Option[Task] = {
    val allTasks = parseTasksFromFolder(taskFolderPath)
    allTasks.find(_.name == taskName)
  }

  // ==================== Validation Parsing ====================

  def parseValidations(
                        validationFolderPath: String,
                        connectionConfigsByName: Map[String, Map[String, String]]
                      )(implicit sparkSession: SparkSession): List[ValidationConfiguration] = {
    try {
      val yamlConfig = YamlFileParser.parseFiles[YamlValidationConfiguration](validationFolderPath).toList
      yamlConfig.map(y => {
        val dataSourceValidations = y.dataSources.map(d => {
          val mappedValidations = d._2.map(dataSourceValidations => {
            val parsedValidations = dataSourceValidations.validations.map {
              case yamlUpstream: YamlUpstreamDataSourceValidation =>
                getYamlUpstreamValidationAsValidationWithConnection(connectionConfigsByName, yamlUpstream)
              case v => ValidationBuilder(v)
            }
            DataSourceValidation(dataSourceValidations.options, dataSourceValidations.waitCondition, parsedValidations)
          })
          d._1 -> mappedValidations
        })
        ValidationConfiguration(y.name, y.description, dataSourceValidations)
      })
    } catch {
      case e: Exception =>
        LOGGER.error(s"Error parsing validations from $validationFolderPath, continuing with empty validations", e)
        List()
    }
  }

  def getYamlUpstreamValidationAsValidationWithConnection(
                                                           connectionConfigsByName: Map[String, Map[String, String]],
                                                           yamlUpstream: YamlUpstreamDataSourceValidation
                                                         ): ValidationBuilder = {
    val upstreamConnectionConfig = connectionConfigsByName.get(yamlUpstream.upstreamDataSource)
    upstreamConnectionConfig match {
      case Some(value) =>
        val connectionConfigWithTaskBuilder = ConnectionConfigWithTaskBuilder(yamlUpstream.upstreamDataSource, value).noop()
        val baseValidation = UpstreamDataSourceValidation(
          yamlUpstream.validation.map(v => ValidationBuilder(v)),
          connectionConfigWithTaskBuilder,
          yamlUpstream.upstreamReadOptions,
          yamlUpstream.joinFields,
          yamlUpstream.joinType
        )
        ValidationBuilder(baseValidation)
      case None =>
        throw DataValidationMissingUpstreamDataSourceException(yamlUpstream.upstreamDataSource)
    }
  }

  // ==================== File Utilities ====================

  /**
   * Find a directory using multiple resolution strategies
   */
  def findDirectory(dirPath: String): Option[File] = {
    val directFile = new File(dirPath)
    val classFile = scala.util.Try(new File(getClass.getResource(s"/$dirPath").getPath))
    val classLoaderFile = scala.util.Try(new File(getClass.getClassLoader.getResource(dirPath).getPath))

    if (directFile.isDirectory) {
      Some(directFile)
    } else if (classFile.isSuccess && classFile.get.isDirectory) {
      Some(classFile.get)
    } else if (classLoaderFile.isSuccess && classLoaderFile.get.isDirectory) {
      Some(classLoaderFile.get)
    } else {
      None
    }
  }

  /**
   * Find all YAML files in a directory (including subdirectories)
   */
  private def findYamlFiles(folderPath: String, recursive: Boolean = true): List[File] = {
    val directory = findDirectory(folderPath).getOrElse(FileUtil.getDirectory(folderPath))
    if (!directory.isDirectory) {
      LOGGER.warn(s"Folder is not a directory, unable to list files, path=${directory.getPath}")
      List()
    } else {
      val current = directory.listFiles().filter(_.getName.endsWith(".yaml")).toList
      if (recursive) {
        current ++ directory.listFiles
          .filter(_.isDirectory)
          .flatMap(dir => findYamlFiles(dir.getAbsolutePath, recursive))
      } else {
        current
      }
    }
  }

  def findYamlPlanFile(configuredPlanPath: String, planName: String)(implicit sparkSession: SparkSession): Option[String] = {
    val planFile = findDirectory(configuredPlanPath).getOrElse(new File(configuredPlanPath))
    val planDirPath = if (planFile.isDirectory) planFile.getAbsolutePath else planFile.getParent

    // Use existing findYamlFiles method instead of manual file filtering
    val yamlFiles = findYamlFiles(planDirPath, recursive = false)

    yamlFiles.find(file => {
      Try {
        val parsed = parsePlan(file.getAbsolutePath)
        parsed.name.equalsIgnoreCase(planName)
      } match {
        case Success(matches) => matches
        case Failure(ex) =>
          LOGGER.warn(s"Failed to parse YAML plan file: ${file.getAbsolutePath}", ex)
          false
      }
    }).map(_.getAbsolutePath)
  }
}
