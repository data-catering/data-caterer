package io.github.datacatering.datacaterer.core.ui.service

import io.github.datacatering.datacaterer.api.model.Plan
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
import io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.util.{Failure, Success, Try}

/**
 * Centralized service for loading plans from various sources (JSON files, YAML files)
 *
 * This service consolidates plan loading logic that was previously duplicated across
 * PlanRepository and FastSampleGenerator, providing a single source of truth for:
 * - Loading JSON plans from the plan save folder
 * - Loading YAML plans from configured directories
 * - Converting YAML plans to PlanRunRequest format
 * - Finding plans by name across multiple sources
 */
object PlanLoaderService {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val planSaveFolder = s"$INSTALL_DIRECTORY/plan"

  /**
   * Load a plan by name, searching JSON first, then YAML sources
   *
   * @param planName Name of the plan to load
   * @param planDirectory Optional custom plan directory (defaults to INSTALL_DIRECTORY/plan)
   * @return PlanRunRequest if found
   * @throws java.io.FileNotFoundException if plan not found in any source
   */
  def loadPlan(
    planName: String,
    planDirectory: Option[String] = None
  )(implicit sparkSession: SparkSession): PlanRunRequest = {
    val effectivePlanFolder = planDirectory.getOrElse(planSaveFolder)

    // Try JSON first
    val tryJson = loadJsonPlan(planName, effectivePlanFolder)
    tryJson match {
      case Success(plan) =>
        LOGGER.debug(s"Loaded JSON plan, plan-name=$planName")
        plan
      case Failure(_) =>
        // Try YAML
        val tryYaml = loadYamlPlanByName(planName)
        tryYaml match {
          case Success(plan) =>
            LOGGER.debug(s"Loaded YAML plan, plan-name=$planName")
            plan
          case Failure(_) =>
            throw new java.io.FileNotFoundException(
              s"Plan not found: $planName (searched in JSON and YAML sources)"
            )
        }
    }
  }

  /**
   * Load a JSON plan from the plan save folder
   */
  def loadJsonPlan(planName: String, planFolder: String = planSaveFolder): Try[PlanRunRequest] = {
    Try {
      LOGGER.debug(s"Loading JSON plan, plan-name=$planName, folder=$planFolder")
      val planFile = Paths.get(s"$planFolder/$planName.json")
      if (!Files.exists(planFile)) {
        throw new java.io.FileNotFoundException(s"JSON plan file not found: ${planFile.toAbsolutePath}")
      }
      val fileContent = Files.readString(planFile)
      ObjectMapperUtil.jsonObjectMapper.readValue(fileContent, classOf[PlanRunRequest])
    }
  }

  /**
   * Load a YAML plan by plan name (searches all YAML plan files for matching name)
   */
  def loadYamlPlanByName(planName: String)(implicit sparkSession: SparkSession): Try[PlanRunRequest] = {
    Try {
      // First try finding a specific YAML plan file by name using enhanced resolution
      val planFilePath = ConfigParser.foldersConfig.planFilePath
      val yamlPlanFilePath = PlanParser.findYamlPlanFile(planFilePath, planName)

      yamlPlanFilePath match {
        case Some(planPath) =>
          LOGGER.debug(s"Found YAML plan file by name, plan-name=$planName, path=$planPath")
          val parsedPlan = PlanParser.parsePlan(planPath)
          convertYamlPlanToPlanRunRequest(parsedPlan, planName)
        case None =>
          // Fallback to searching all YAML plans
          LOGGER.debug(s"YAML plan file not found by name search, scanning all YAML plans, plan-name=$planName")
          val allYamlPlans = getAllYamlPlansAsPlanRunRequests
          allYamlPlans.find(_.plan.name == planName) match {
            case Some(plan) => plan
            case None => throw new java.io.FileNotFoundException(s"YAML plan not found with name: $planName")
          }
      }
    }
  }

  /**
   * Load a YAML plan by file name
   */
  def loadYamlPlanByFileName(fileName: String)(implicit sparkSession: SparkSession): Try[PlanRunRequest] = {
    Try {
      val yamlPlanFiles = getYamlPlanFiles
      val planFile = yamlPlanFiles.find(_.getName == fileName) match {
        case Some(file) => file
        case None => throw new java.io.FileNotFoundException(s"YAML plan file not found: $fileName")
      }

      val parsedPlan = PlanParser.parsePlan(planFile.getAbsolutePath)
      convertYamlPlanToPlanRunRequest(parsedPlan, fileName.replaceAll("\\.yaml$", ""))
    }
  }

  /**
   * Get all YAML plan files and convert them to PlanRunRequest format
   */
  def getAllYamlPlansAsPlanRunRequests(implicit sparkSession: SparkSession): List[PlanRunRequest] = {
    val yamlPlanFiles = getYamlPlanFiles
    yamlPlanFiles.flatMap(planFile => {
      val tryParse = Try {
        val parsedPlan = PlanParser.parsePlan(planFile.getAbsolutePath)
        convertYamlPlanToPlanRunRequest(parsedPlan, planFile.getName.replaceAll("\\.yaml$", ""))
      }
      tryParse match {
        case Failure(exception) =>
          LOGGER.error(s"Failed to parse YAML plan file, file=${planFile.getAbsolutePath}, exception=${exception.getMessage}")
          None
        case Success(value) => Some(value)
      }
    })
  }

  /**
   * Get all YAML plan files from configured folders
   * Searches both the plan save folder and the configured plan file directory
   */
  def getYamlPlanFiles: List[File] = {
    // Check the plan save folder (where UI stores plans)
    val planFolder = new File(planSaveFolder)
    val planFolderYamlFiles = if (planFolder.exists() && planFolder.isDirectory) {
      LOGGER.debug(s"Scanning for YAML plan files in plan save folder: ${planFolder.getAbsolutePath}")
      val files = planFolder.listFiles()
      if (files != null) {
        files.filter(f => f.isFile && f.getName.endsWith(".yaml")).toList
      } else {
        List()
      }
    } else {
      List()
    }

    // Also check the directory from the configured planFilePath
    val planFilePath = ConfigParser.foldersConfig.planFilePath
    val planDirPath = new File(planFilePath).getParent

    val configuredYamlFiles = if (planDirPath != null) {
      PlanParser.findDirectory(planDirPath) match {
        case Some(planDir) if planDir.exists() && planDir.isDirectory =>
          LOGGER.debug(s"Scanning for YAML plan files in configured path: ${planDir.getAbsolutePath}")
          val files = planDir.listFiles()
          if (files != null) {
            files.filter(f => f.isFile && f.getName.endsWith(".yaml")).toList
          } else {
            LOGGER.warn(s"Could not list files in directory: ${planDir.getAbsolutePath}")
            List()
          }
        case Some(planDir) =>
          LOGGER.warn(s"Plan directory does not exist or is not a directory: ${planDir.getAbsolutePath}")
          List()
        case None =>
          LOGGER.warn(s"Could not find plan directory: $planDirPath")
          List()
      }
    } else {
      LOGGER.warn(s"Could not determine parent directory from planFilePath: $planFilePath")
      List()
    }

    // Combine and deduplicate based on file name
    val allFiles = planFolderYamlFiles ++ configuredYamlFiles
    allFiles.groupBy(_.getName).values.map(_.head).toList
  }

  /**
   * Convert a YAML Plan to PlanRunRequest format for API consistency
   */
  private def convertYamlPlanToPlanRunRequest(plan: Plan, planName: String): PlanRunRequest = {
    PlanRunRequest(
      id = UUID.randomUUID().toString,
      plan = plan,
      tasks = List(), // Tasks will be loaded from YAML task files when executed
      validation = List(),
      configuration = None
    )
  }

  /**
   * Check if a plan exists in the JSON plan storage
   */
  def planExistsInJson(planName: String): Boolean = {
    loadJsonPlan(planName, planSaveFolder).isSuccess
  }

  /**
   * Get all plans (both JSON and YAML)
   */
  def getAllPlans(implicit sparkSession: SparkSession): List[PlanRunRequest] = {
    // Get JSON plans
    val planFolder = Path.of(planSaveFolder)
    if (!planFolder.toFile.exists()) planFolder.toFile.mkdirs()

    val jsonPlans = if (planFolder.toFile.exists() && planFolder.toFile.isDirectory) {
      val files = planFolder.toFile.listFiles()
      if (files != null) {
        files
          .filter(file => file.toString.endsWith(".json"))
          .flatMap(file => {
            val tryParse = loadJsonPlan(file.getName.replaceAll("\\.json$", ""), planSaveFolder)
            tryParse match {
              case Failure(exception) =>
                LOGGER.error(s"Failed to parse JSON plan file, file-name=${file.getName}, exception=${exception.getMessage}")
                None
              case Success(value) => Some(value)
            }
          })
          .toList
      } else {
        List()
      }
    } else {
      List()
    }

    // Get YAML plans
    val yamlPlans = getAllYamlPlansAsPlanRunRequests

    jsonPlans ++ yamlPlans
  }
}
