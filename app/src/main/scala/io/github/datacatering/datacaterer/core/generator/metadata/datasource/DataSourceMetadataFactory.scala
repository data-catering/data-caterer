package io.github.datacatering.datacaterer.core.generator.metadata.datasource

import io.github.datacatering.datacaterer.api.model.Constants.METADATA_SOURCE_TYPE
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, DataSourceValidation, Field, Plan, Task, ValidationConfiguration}
import io.github.datacatering.datacaterer.api.{PlanRun, ValidationBuilder}
import io.github.datacatering.datacaterer.core.generator.metadata.PlanGenerator.writeToFiles
import io.github.datacatering.datacaterer.core.model.{ForeignKeyRelationship, ValidationConfigurationHelper}
import io.github.datacatering.datacaterer.core.util.MetadataUtil.getMetadataFromConnectionConfig
import io.github.datacatering.datacaterer.core.util.{ConfigUtil, ForeignKeyUtil, MetadataUtil, SchemaHelper, TaskHelper}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}

class DataSourceMetadataFactory(dataCatererConfiguration: DataCatererConfiguration)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val metadataConfig = dataCatererConfiguration.metadataConfig
  private val flagsConfig = dataCatererConfiguration.flagsConfig

  /**
   * Extracts all data source metadata as defined in the connection configurations in the application config.
   *
   * If the plan is defined, it will only retrieve metadata for the data sources being used in the plan.
   * It will then generate tasks and validation configurations based on the extracted metadata.
   *
   * @param planRun the current plan run
   * @return an optional tuple containing the plan, list of tasks, and list of validation configurations
   */
  def extractAllDataSourceMetadata(planRun: PlanRun): Option[(Plan, List[Task], List[ValidationConfiguration])] = {
    if (flagsConfig.enableGeneratePlanAndTasks || flagsConfig.enableGenerateValidations) {
      LOGGER.info("Attempting to extract all data source metadata as defined in connection configurations in application config")
      val connectionMetadata = getMetadataFromConnections(planRun)
      val updatedPlanRun = getExternalMetadataFromPlanRun(planRun)

      val metadataPerConnection = connectionMetadata.map(x => (x, x.getForeignKeys, x.getMetadataForDataSource(flagsConfig, metadataConfig)))
      val generatedTasksFromMetadata = metadataPerConnection.map(m => (m._1.name, TaskHelper.fromMetadata(Some(updatedPlanRun), m._1.name, m._1.format, m._3)))
      val stepMapping = generatedTasksFromMetadata.flatMap(_._2._2)
      val generatedTasks = generatedTasksFromMetadata.map(x => (x._1, x._2._1))

      val allForeignKeys = ForeignKeyUtil.getAllForeignKeyRelationships(metadataPerConnection.map(_._2), Some(updatedPlanRun), stepMapping.toMap)
      val validationConfig = getValidationConfiguration(metadataPerConnection, Some(updatedPlanRun))
      connectionMetadata.foreach(_.close())

      Some(writeToFiles(Some(updatedPlanRun), generatedTasks, allForeignKeys, validationConfig, dataCatererConfiguration.foldersConfig))
    } else {
      LOGGER.debug("Generate plan, tasks or validations not enabled, not attempting to get data source metadata")
      None
    }
  }

  /**
   * Generate data source metadata per connection defined in the plan.
   *
   * @param planRun the plan defined by the user
   * @return a list of `DataSourceMetadata` per connection defined in the plan
   */
  private def getMetadataFromConnections(planRun: PlanRun): List[DataSourceMetadata] = {
    //if plan is defined, only get those data sources being used
    val planDataSources = (planRun._connectionTaskBuilders.map(c => c.connectionConfigWithTaskBuilder.dataSourceName) ++
      planRun._plan.tasks.map(t => t.dataSourceName) ++
      planRun.plan.tasks.map(t => t.dataSourceName)).distinct
    LOGGER.debug(s"Only retrieving metadata for those data sources defined in plan, data-source-names=${planDataSources.mkString(",")}")

    val connectionsFromConfig = planRun._configuration.connectionConfigByName
      .map(c => c._1 -> (c._2 ++ dataCatererConfiguration.connectionConfigByName.getOrElse(c._1, Map())))
    val planConnections = connectionsFromConfig.filter(c => planDataSources.contains(c._1))
    planConnections.map(getMetadataFromConnectionConfig).filter(_.isDefined).map(_.get).toList
  }

  private def getValidationConfiguration(
                                          metadataPerConnection: List[(DataSourceMetadata, Dataset[ForeignKeyRelationship], List[DataSourceDetail])],
                                          optPlanRun: Option[PlanRun]
                                        ): List[ValidationConfiguration] = {
    val dataSourceValidations = metadataPerConnection.flatMap(m => {
      m._3.map(_.toDataSourceValidation)
    }).toMap
    val generatedValidationConfig = ValidationConfiguration(dataSources = dataSourceValidations)
    if (optPlanRun.isDefined && optPlanRun.get._validations.nonEmpty) {
      if (dataSourceValidations.nonEmpty) {
        LOGGER.debug("Manual validations found, combining with any generated validations")
        val userValidationConfig = optPlanRun.get._validations
        List(ValidationConfigurationHelper.merge(userValidationConfig, generatedValidationConfig))
      } else {
        LOGGER.debug("No generated validations found, using manual validations")
        optPlanRun.get._validations
      }
    } else {
      List(generatedValidationConfig)
    }
  }

  private def getExternalMetadataFromPlanRun(planRun: PlanRun): PlanRun = {
    val updatedTaskBuilders = planRun._connectionTaskBuilders.map(connectionTaskBuilder => {
      val stepWithUpdatedSchema = connectionTaskBuilder.step.map(stepBuilder => {
        val stepSchemaFromMetadataSource = getMetadataSourceSchema(stepBuilder.step.fields)
        //remove the metadata source config from generator options after
        stepBuilder.step.copy(fields = stepSchemaFromMetadataSource)
      })
      connectionTaskBuilder.apply(connectionTaskBuilder.connectionConfigWithTaskBuilder, connectionTaskBuilder.task.map(_.task), stepWithUpdatedSchema)
    })
    planRun._connectionTaskBuilders = updatedTaskBuilders
    planRun
  }

  private def getMetadataSourceSchema(userFields: List[Field]): List[Field] = {
    val updatedFields = userFields.map(f => {
      if (f.fields.nonEmpty) {
        val metadataForFieldSchema = getMetadataSourceSchema(f.fields)
        f.copy(fields = metadataForFieldSchema)
      } else {
        val isFieldDefinedFromMetadataSource = f.options.contains(METADATA_SOURCE_TYPE)
        if (isFieldDefinedFromMetadataSource) {
          LOGGER.debug(s"Found field definition is based on metadata source, field-name=${f.name}")
        }
        val metadataSource = MetadataUtil.getMetadataFromConnectionConfig("schema_metadata" -> f.options.map(e => e._1 -> e._2.toString))
        val fieldMetadata = metadataSource.map(_.getMetadataForDataSource(flagsConfig, metadataConfig)).getOrElse(List())
        val baseDataSourceDetail = if (fieldMetadata.size > 1) {
          LOGGER.warn(s"Multiple schemas found for field level schema, will only use the first schema found from metadata source, " +
            s"metadata-source-type=${f.options(METADATA_SOURCE_TYPE)}, num-schemas=${fieldMetadata.size}, field-name=${f.name}")
          Some(fieldMetadata.head)
        } else if (fieldMetadata.size == 1) {
          Some(fieldMetadata.head)
        } else {
          None
        }
        val updatedGeneratorOptions = ConfigUtil.cleanseOptions(f.options.map(o => o._1 -> o._2.toString))
        baseDataSourceDetail.map(d => {
          val metadataSourceSchema = SchemaHelper.fromStructType(d.structType)
          f.copy(fields = metadataSourceSchema, options = updatedGeneratorOptions)
        }).getOrElse(f)
      }
    })
    updatedFields
  }
}

case class DataSourceDetail(
                             dataSourceMetadata: DataSourceMetadata,
                             sparkOptions: Map[String, String],
                             structType: StructType,
                             validations: List[ValidationBuilder] = List()
                           ) {
  def toDataSourceValidation: (String, List[DataSourceValidation]) =
    (dataSourceMetadata.name, List(DataSourceValidation(sparkOptions, validations = validations)))
}
