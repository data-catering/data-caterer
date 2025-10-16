package io.github.datacatering.datacaterer.core.generator.metadata.datasource.yaml

import io.github.datacatering.datacaterer.api.model.Constants.{METADATA_IDENTIFIER, YAML_PLAN_FILE, YAML_STEP_NAME, YAML_TASK_FILE, YAML_TASK_NAME}
import io.github.datacatering.datacaterer.api.model.{Plan, Task}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.util.{FileUtil, ObjectMapperUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

case class YamlDataSourceMetadata(
                                   name: String,
                                   format: String,
                                   connectionConfig: Map[String, String]
                                 ) extends DataSourceMetadata {

  private val LOGGER = Logger.getLogger(getClass.getName)
  
  override val hasSourceData: Boolean = false

  override def toStepName(options: Map[String, String]): String = {
    options(METADATA_IDENTIFIER)
  }

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    val yamlPlanFile = connectionConfig.get(YAML_PLAN_FILE)
    val yamlTaskFile = connectionConfig.get(YAML_TASK_FILE)
    val yamlTaskName = connectionConfig.get(YAML_TASK_NAME)
    val yamlStepName = connectionConfig.get(YAML_STEP_NAME)

    try {
      (yamlPlanFile, yamlTaskFile) match {
        case (Some(planFile), None) =>
          LOGGER.info(s"Loading metadata from YAML plan file: $planFile")
          getMetadataFromPlan(planFile)
        case (None, Some(taskFile)) =>
          LOGGER.info(s"Loading metadata from YAML task file: $taskFile, taskName=$yamlTaskName, stepName=$yamlStepName")
          getMetadataFromTask(taskFile, yamlTaskName, yamlStepName)
        case (Some(planFile), Some(taskFile)) =>
          LOGGER.warn(s"Both plan and task files specified, using task file: $taskFile")
          getMetadataFromTask(taskFile, yamlTaskName, yamlStepName)
        case (None, None) =>
          LOGGER.error("No YAML plan or task file specified in connection config")
          Array.empty[SubDataSourceMetadata]
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to load YAML metadata from files, plan=$yamlPlanFile, task=$yamlTaskFile", ex)
        Array.empty[SubDataSourceMetadata]
    }
  }

  private def getMetadataFromPlan(planFile: String)(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    try {
      val plan = PlanParser.parsePlan(planFile)
      LOGGER.debug(s"Parsed plan: ${plan.name} with ${plan.tasks.size} tasks")
      
      // Plan contains TaskSummary objects, not full Task objects with steps
      // We return metadata placeholders that will be resolved when tasks are loaded
      plan.tasks.map(taskSummary => {
        val identifier = s"${taskSummary.dataSourceName}.${taskSummary.name}"
        val readOptions = Map(METADATA_IDENTIFIER -> identifier)
        
        // TaskSummary doesn't have steps, so no field metadata available
        SubDataSourceMetadata(readOptions, None)
      }).toArray
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to parse plan file: $planFile", ex)
        Array.empty[SubDataSourceMetadata]
    }
  }

  private def getMetadataFromTask(
                                   taskFile: String,
                                   taskNameFilter: Option[String],
                                   stepNameFilter: Option[String]
                                 )(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    try {
      // Parse single task file directly using ObjectMapper like PlanParser does
      val taskObj = FileUtil.getFile(taskFile)
      val task = ObjectMapperUtil.yamlObjectMapper.readValue(taskObj, classOf[Task])
      LOGGER.debug(s"Parsed task from file: $taskFile, task-name=${task.name}")
      
      // Check if task name matches filter (if provided)
      val taskMatches = taskNameFilter.forall(_ == task.name)
      if (!taskMatches) {
        LOGGER.debug(s"Task name '${task.name}' does not match filter '${taskNameFilter.getOrElse("")}', skipping")
        return Array.empty[SubDataSourceMetadata]
      }
      
      // Filter steps by step name if provided
      val filteredSteps = stepNameFilter match {
        case Some(stepName) => task.steps.filter(_.name == stepName)
        case None => task.steps
      }
      
      filteredSteps.map(step => {
        val identifier = s"${task.name}.${step.name}"
        val readOptions = Map(METADATA_IDENTIFIER -> identifier)
        
        // Convert step fields to field metadata
        val fieldMetadata = convertStepToFieldMetadata(step.fields)
        
        SubDataSourceMetadata(readOptions, Some(fieldMetadata))
      }).toArray
      
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to parse task file: $taskFile", ex)
        Array.empty[SubDataSourceMetadata]
    }
  }

  private[yaml] def convertStepToFieldMetadata(fields: List[io.github.datacatering.datacaterer.api.model.Field])
                                       (implicit sparkSession: SparkSession): Dataset[FieldMetadata] = {
    import sparkSession.implicits._
    
    try {
      if (fields.nonEmpty) {
        LOGGER.debug(s"Converting ${fields.size} fields to field metadata")
        
        // Convert Fields directly to FieldMetadata using helper method
        val fieldMetadataList = convertFieldsToFieldMetadata(fields)
        
        sparkSession.createDataset(fieldMetadataList)
      } else {
        LOGGER.debug("No fields found, returning empty field metadata dataset")
        sparkSession.emptyDataset[FieldMetadata]
      }
    } catch {
      case ex: Exception =>
        LOGGER.error("Failed to convert fields to field metadata", ex)
        sparkSession.emptyDataset[FieldMetadata]
    }
  }

  /**
   * Convert Field objects to FieldMetadata objects, handling nested fields recursively.
   * This method can be reused for nested field conversion as well.
   */
  private def convertFieldsToFieldMetadata(fields: List[io.github.datacatering.datacaterer.api.model.Field]): List[FieldMetadata] = {
    fields.map(convertFieldToFieldMetadata)
  }

  /**
   * Convert a single Field to FieldMetadata, handling nested fields recursively.
   * Package-private for testing.
   */
  private[yaml] def convertFieldToFieldMetadata(field: io.github.datacatering.datacaterer.api.model.Field): FieldMetadata = {
    FieldMetadata(
      field = field.name,
      dataSourceReadOptions = Map(METADATA_IDENTIFIER -> s"yaml.${field.name}"),
      metadata = buildFieldMetadata(field),
      nestedFields = if (field.fields.nonEmpty) convertFieldsToFieldMetadata(field.fields) else List.empty
    )
  }

  /**
   * Build metadata map for a field, including type, nullable, options, and static values.
   */
  private def buildFieldMetadata(field: io.github.datacatering.datacaterer.api.model.Field): Map[String, String] = {
    val baseMetadata = Map(
      "type" -> field.`type`.getOrElse("string"),
      "nullable" -> field.nullable.toString
    ) ++ field.options.map { case (k, v) => k -> v.toString }
    
    // Include static value if present
    field.static match {
      case Some(staticValue) => baseMetadata + ("static" -> staticValue)
      case None => baseMetadata
    }
  }
}