package io.github.datacatering.datacaterer.core.generator.metadata.datasource.yaml

import io.github.datacatering.datacaterer.api.model.Constants.{METADATA_IDENTIFIER, YAML_PLAN_FILE, YAML_STEP_NAME, YAML_TASK_FILE, YAML_TASK_NAME}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import io.github.datacatering.datacaterer.core.parser.PlanParser
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

  /**
   * Override to create a more specific cache key for YAML metadata.
   * Includes YAML file path and filters for precise caching.
   */
  override protected def createCacheKey(): String = {
    val yamlPlanFile = connectionConfig.get(YAML_PLAN_FILE).getOrElse("")
    val yamlTaskFile = connectionConfig.get(YAML_TASK_FILE).getOrElse("")
    val yamlTaskName = connectionConfig.get(YAML_TASK_NAME).getOrElse("")
    val yamlStepName = connectionConfig.get(YAML_STEP_NAME).getOrElse("")

    s"${getClass.getSimpleName}|plan:$yamlPlanFile|task:$yamlTaskFile|taskName:$yamlTaskName|stepName:$yamlStepName"
  }

  /**
   * Loads YAML metadata from plan or task files.
   * This method is called by the base trait's caching mechanism.
   */
  override protected def loadSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    val yamlPlanFile = connectionConfig.get(YAML_PLAN_FILE)
    val yamlTaskFile = connectionConfig.get(YAML_TASK_FILE)
    val yamlTaskName = connectionConfig.get(YAML_TASK_NAME)
    val yamlStepName = connectionConfig.get(YAML_STEP_NAME)

    LOGGER.info(s"Processing YAML metadata source - planFile=$yamlPlanFile, taskFile=$yamlTaskFile, taskName=$yamlTaskName, stepName=$yamlStepName")

    try {
      val result = (yamlPlanFile, yamlTaskFile) match {
        case (Some(planFile), None) =>
          LOGGER.info(s"Loading metadata from YAML plan file: $planFile")
          getMetadataFromPlan(planFile)
        case (None, Some(taskFile)) =>
          LOGGER.info(s"Loading metadata from YAML task file: $taskFile, taskName=$yamlTaskName, stepName=$yamlStepName")
          getMetadataFromTask(taskFile, yamlTaskName, yamlStepName)
        case (Some(planFile), Some(taskFile)) =>
          LOGGER.warn(s"Both plan and task files specified, using task file: $taskFile (ignoring plan file: $planFile)")
          getMetadataFromTask(taskFile, yamlTaskName, yamlStepName)
        case (None, None) =>
          LOGGER.error(s"YAML metadata source configuration error: No YAML plan or task file specified in connection config. Please provide either '${YAML_PLAN_FILE}' or '${YAML_TASK_FILE}' in your metadata source configuration.")
          Array.empty[SubDataSourceMetadata]
      }
      
      if (result.nonEmpty) {
        LOGGER.info(s"Successfully loaded ${result.length} sub-data sources from YAML metadata. Sub-data sources: ${result.map(_.readOptions.getOrElse(METADATA_IDENTIFIER, "unknown")).mkString(", ")}")
      } else {
        LOGGER.warn(s"No sub-data sources found in YAML metadata. This could mean: 1) File is empty, 2) Task/step filters don't match, 3) Parsing failed. Check file content and filters.")
      }
      
      result
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to load YAML metadata from files. Configuration: plan=$yamlPlanFile, task=$yamlTaskFile, taskName=$yamlTaskName, stepName=$yamlStepName. Error: ${ex.getMessage}", ex)
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
        val baseReadOptions = Map(METADATA_IDENTIFIER -> identifier)
        
        // Include YAML context in readOptions for better traceability and debugging
        val readOptionsWithYamlContext = baseReadOptions ++ Map(YAML_PLAN_FILE -> planFile)
        
        // TaskSummary doesn't have steps, so no field metadata available
        SubDataSourceMetadata(readOptionsWithYamlContext, None)
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
      // Use consolidated task parsing from PlanParser
      val task = PlanParser.parseTaskFileSimple(taskFile)
      LOGGER.info(s"Successfully parsed YAML task file: $taskFile. Found task: '${task.name}' with ${task.steps.length} steps: [${task.steps.map(_.name).mkString(", ")}]")
      
      // Check if task name matches filter (if provided)
      val taskMatches = taskNameFilter.forall(_ == task.name)
      if (!taskMatches) {
        LOGGER.error(s"YAML metadata task filter mismatch: Task name '${task.name}' does not match required filter '${taskNameFilter.getOrElse("<none>")}'. Please check your yamlTask configuration or update the task name in the YAML file. Available task: '${task.name}'")
        return Array.empty[SubDataSourceMetadata]
      }
      
      // Filter steps by step name if provided
      val filteredSteps = stepNameFilter match {
        case Some(stepName) => 
          val matchingSteps = task.steps.filter(_.name == stepName)
          if (matchingSteps.isEmpty) {
            LOGGER.error(s"YAML metadata step filter mismatch: Step name '$stepName' not found in task '${task.name}'. Available steps: [${task.steps.map(_.name).mkString(", ")}]. Please check your yamlTask configuration.")
          } else {
            LOGGER.info(s"Step filter applied: Using step '$stepName' from task '${task.name}'")
          }
          matchingSteps
        case None => 
          LOGGER.info(s"No step filter specified, using all ${task.steps.length} steps from task '${task.name}'")
          task.steps
      }
      
      if (filteredSteps.isEmpty) {
        LOGGER.warn(s"No steps found after filtering in task '${task.name}'. Check your step name filter: $stepNameFilter")
        return Array.empty[SubDataSourceMetadata]
      }
      
      filteredSteps.map(step => {
        val identifier = s"${task.name}.${step.name}"
        val baseReadOptions = Map(METADATA_IDENTIFIER -> identifier)

        // Include YAML context in readOptions for better traceability and debugging
        val yamlContextOptions =
          taskNameFilter.map(YAML_TASK_NAME -> _).toMap ++
          stepNameFilter.map(YAML_STEP_NAME -> _).toMap ++
          Map(YAML_TASK_FILE -> taskFile)

        // Combine base options, YAML context, and step options
        // Priority: base > YAML context > step options (step options have lowest priority)
        val readOptionsWithYamlContext = step.options ++ yamlContextOptions ++ baseReadOptions

        LOGGER.info(s"Processing step '${step.name}' with ${step.fields.length} fields. Metadata identifier: '$identifier'")
        if (step.options.nonEmpty) {
          LOGGER.debug(s"Step '${step.name}' has ${step.options.size} options that will be included in readOptions: ${step.options.keys.mkString(", ")}")
        }

        // Convert step fields to field metadata with the correct identifier and all read options
        val fieldMetadata = convertStepToFieldMetadata(step.fields, identifier, readOptionsWithYamlContext)

        SubDataSourceMetadata(readOptionsWithYamlContext, Some(fieldMetadata))
      }).toArray
      
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to parse YAML task file: $taskFile. Common causes: 1) File not found, 2) Invalid YAML syntax, 3) File doesn't match expected Task structure. Error: ${ex.getMessage}", ex)
        Array.empty[SubDataSourceMetadata]
    }
  }

  private[yaml] def convertStepToFieldMetadata(
      fields: List[io.github.datacatering.datacaterer.api.model.Field],
      identifier: String,
      dataSourceReadOptions: Map[String, String] = Map()
  )(implicit sparkSession: SparkSession): Dataset[FieldMetadata] = {

    try {
      if (fields.nonEmpty) {
        LOGGER.debug(s"Converting ${fields.size} fields to field metadata with identifier '$identifier'. Fields: [${fields.map(f => s"${f.name}:${f.`type`.getOrElse("unknown")}").mkString(", ")}]")

        // Combine identifier with additional read options
        val readOptionsWithIdentifier = dataSourceReadOptions + (METADATA_IDENTIFIER -> identifier)

        // Convert Fields directly to FieldMetadata using helper method with correct identifier and read options
        val fieldMetadataList = convertFieldsToFieldMetadata(fields, readOptionsWithIdentifier)
        LOGGER.debug(s"Successfully converted ${fieldMetadataList.size} field metadata entries for identifier '$identifier'")

        // Log any complex types for debugging
        val complexFields = fields.filter(f => f.`type`.exists(t => t == "struct" || t == "array"))
        if (complexFields.nonEmpty) {
          LOGGER.debug(s"Found ${complexFields.size} complex type fields: [${complexFields.map(f => s"${f.name}:${f.`type`.getOrElse("unknown")}(${f.fields.size} nested)").mkString(", ")}]")
        }

        sparkSession.createDataset(fieldMetadataList)
      } else {
        LOGGER.warn(s"No fields found in YAML metadata for identifier '$identifier'. This may indicate an empty step or parsing issue.")
        sparkSession.emptyDataset[FieldMetadata]
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to convert fields to field metadata for identifier '$identifier'. This could be due to: 1) Invalid field type definitions, 2) Complex nested structure issues, 3) Missing required field properties. Error: ${ex.getMessage}", ex)
        sparkSession.emptyDataset[FieldMetadata]
    }
  }

  /**
   * Convert Field objects to FieldMetadata objects, handling nested fields recursively.
   * This method can be reused for nested field conversion as well.
   */
  private def convertFieldsToFieldMetadata(
      fields: List[io.github.datacatering.datacaterer.api.model.Field],
      dataSourceReadOptions: Map[String, String]
  ): List[FieldMetadata] = {
    fields.map(convertFieldToFieldMetadata(_, dataSourceReadOptions))
  }

  /**
   * Convert a single Field to FieldMetadata, handling nested fields recursively.
   * Package-private for testing.
   */
  private[yaml] def convertFieldToFieldMetadata(
      field: io.github.datacatering.datacaterer.api.model.Field,
      dataSourceReadOptions: Map[String, String]
  ): FieldMetadata = {
    FieldMetadata(
      field = field.name,
      dataSourceReadOptions = dataSourceReadOptions,
      metadata = buildFieldMetadata(field),
      nestedFields = if (field.fields.nonEmpty) convertFieldsToFieldMetadata(field.fields, dataSourceReadOptions) else List.empty
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