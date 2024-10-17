package io.github.datacatering.datacaterer.core.generator.delete

import io.github.datacatering.datacaterer.api.model.Constants.{CASSANDRA, CSV, DELTA, FOREIGN_KEY_DELIMITER, FORMAT, JDBC, JSON, ORC, PARQUET, PATH}
import io.github.datacatering.datacaterer.api.model.{ForeignKeyRelation, Plan, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.model.Constants.RECORD_TRACKING_VALIDATION_FORMAT
import io.github.datacatering.datacaterer.core.util.ConfigUtil.cleanseOptions
import io.github.datacatering.datacaterer.core.util.MetadataUtil.getSubDataSourcePath
import io.github.datacatering.datacaterer.core.util.PlanImplicits.SinkOptionsOps
import io.github.datacatering.datacaterer.core.util.{ForeignKeyRelationHelper, ForeignKeyUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.reflect.io.Directory
import scala.util.{Failure, Success, Try}

/*
 * Ability to delete generated records.
 * Also, have the ability to delete records that are created as a result of the generated records (i.e. a service/job
 * reads in the generated records and creates additional records in another data source or the same data source, with
 * potential transformations).
 */
class DeleteRecordProcessor(connectionConfigsByName: Map[String, Map[String, String]], recordTrackingFolderPath: String)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /*
   * Ability to delete generated records from the corresponding data source.
   * Also, has the ability to delete records that are associated with generated records (i.e. records consumed by
   * a service/job then inserted into another data source).
   *
   * Need to delete according to reverse order of insertion.
   *
   * If deleting from non-tracked data source, then should get tracked records from source foreign key.
   */
  def deleteGeneratedRecords(plan: Plan, stepsByName: Map[String, Step], summaryWithTask: List[(TaskSummary, Task)]): Unit = {
    if (plan.sinkOptions.isDefined && plan.sinkOptions.get.foreignKeys.nonEmpty) {
      val sinkOpts = plan.sinkOptions.get
      val allForeignKeys = sinkOpts.getAllForeignKeyRelations
      val foreignKeysWithoutColNames = sinkOpts.foreignKeysWithoutColumnNames
      val foreignKeyDeleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeysWithoutColNames)

      foreignKeyDeleteOrder.foreach(foreignKeyName => {
        val fullForeignKey = allForeignKeys.find(f => s"${f._1.dataSource}$FOREIGN_KEY_DELIMITER${f._1.step}".equalsIgnoreCase(foreignKeyName))

        val optSourceForeignKey = foreignKeysWithoutColNames.find(fkLinks => fkLinks._2.contains(foreignKeyName)).map(_._1)
        val (foreignKeyRelation, step) = foreignKeySourceDetails(stepsByName, foreignKeyName)
        LOGGER.debug(s"Using following connection details for deleting data with foreign keys defined, " +
          s"data-source-name=${foreignKeyRelation.dataSource}, data-source-details=${cleanseOptions(step.options)}")

        deleteRecords(foreignKeyRelation.dataSource, plan, step, stepsByName, optSourceForeignKey, fullForeignKey)
      })
      val nonForeignKeySources = summaryWithTask.map(summary => {
        val nonFkSteps = summary._2.steps
          .filter(step => !foreignKeyDeleteOrder.contains(summary._1.dataSourceName + FOREIGN_KEY_DELIMITER + step.name))
        summary._1 -> summary._2.copy(steps = nonFkSteps)
      })
      deleteRecordsByTask(plan, nonForeignKeySources)
    } else {
      deleteRecordsByTask(plan, summaryWithTask)
    }
  }

  private def foreignKeySourceDetails(stepsByName: Map[String, Step], foreignKeyName: String): (ForeignKeyRelation, Step) = {
    val foreignKeyRelation = ForeignKeyRelationHelper.fromString(foreignKeyName)
    val connectionConfig = connectionConfigsByName(foreignKeyRelation.dataSource)
    val step = stepsByName(foreignKeyRelation.step)
    val stepWithConnectionConfig = step.copy(options = step.options ++ connectionConfig)
    (foreignKeyRelation, stepWithConnectionConfig)
  }

  private def deleteRecordsByTask(plan: Plan, summaryWithTask: List[(TaskSummary, Task)]): Unit = {
    summaryWithTask.foreach(task => {
      task._2.steps.foreach(step => {
        val connectionConfig = connectionConfigsByName(task._1.dataSourceName)
        val stepWithConnectionConfig = step.copy(options = step.options ++ connectionConfig)
        LOGGER.debug(s"Using following connection details for deleting data with no foreign keys defined, " +
          s"data-source-name=${task._1.dataSourceName}, data-source-details=${cleanseOptions(stepWithConnectionConfig.options)}")
        deleteRecords(task._1.dataSourceName, plan, stepWithConnectionConfig)
      })
    })
  }

  private def deleteRecords(dataSourceName: String, plan: Plan, step: Step, stepsByName: Map[String, Step] = Map(),
                    optSourceForeignKey: Option[String] = None, optFullForeignKey: Option[(ForeignKeyRelation, String)] = None): Unit = {
    val format = step.options(FORMAT)
    val subDataSourcePath = getSubDataSourcePath(dataSourceName, plan.name, step, recordTrackingFolderPath)
    val optDeleteRecordService = getDeleteRecordService(format)

    optDeleteRecordService.foreach(deleteRecordService => {
      LOGGER.warn(s"Attempting to delete generated records, all generated records for this data source will be deleted, " +
        s"data-source-name=$dataSourceName, format=$format, details=${cleanseOptions(step.options)}")
      val tryTrackedRecords = Try(getTrackedRecords(subDataSourcePath))

      tryTrackedRecords match {
        case Failure(exception) =>
          // try to get tracked records from source foreign key if defined
          optSourceForeignKey.map(sourceForeignKey => {
            deleteRecordsUsingForeignKeySource(dataSourceName, plan, step, stepsByName, optFullForeignKey, deleteRecordService, sourceForeignKey)
          }).getOrElse(
            LOGGER.error(s"Failed to get tracked records for data source, unable to delete data, will continue to try delete other data sources, " +
              s"data-source-name=$dataSourceName, format=$format, tracked-records-path=$subDataSourcePath, " +
              s"details=${cleanseOptions(step.options)}, exception=${exception.getMessage}")
          )
        case Success(trackedRecords) =>
          deleteRecordsAndTrackingFile(dataSourceName, step, subDataSourcePath, deleteRecordService, trackedRecords)
      }
    })
  }

  private def deleteRecordsUsingForeignKeySource(dataSourceName: String, plan: Plan, step: Step, stepsByName: Map[String, Step],
                                                 optFullForeignKey: Option[(ForeignKeyRelation, String)],
                                                 deleteRecordService: DeleteRecordService, sourceForeignKey: String): Unit = {
    val sourceFkDetails = foreignKeySourceDetails(stepsByName, sourceForeignKey)
    val sourceFkSubDataSourcePath = getSubDataSourcePath(sourceFkDetails._1.dataSource, plan.name, sourceFkDetails._2, recordTrackingFolderPath)
    val trySourceFkTrackedRecords = Try(getTrackedRecords(sourceFkSubDataSourcePath))

    trySourceFkTrackedRecords match {
      case Failure(exception) =>
        LOGGER.error("Failed to get tracked records of source foreign key data source, unable to delete data, " +
          s"data-source-name=$dataSourceName, source-foreign-key=$sourceForeignKey, " +
          s"tracked-records-path=$sourceFkSubDataSourcePath, details=${step.options}, exception=${exception.getMessage}")
      case Success(df) =>
        // need to apply SQL expressions if it is a 'delete' foreign key relationship
        val mappedDf = optFullForeignKey.filter(_._2.equalsIgnoreCase("delete"))
          .map(fk => {
            val sqlExpr = fk._1.columns
            df.selectExpr(sqlExpr: _*)
          })
          .getOrElse(df)
        deleteRecordsAndTrackingFile(dataSourceName, step, sourceFkSubDataSourcePath, deleteRecordService, mappedDf, true)
    }
  }

  private def deleteRecordsAndTrackingFile(dataSourceName: String, step: Step, subDataSourcePath: String,
                                           deleteRecordService: DeleteRecordService, trackedRecords: DataFrame, isDeleteForeignKey: Boolean = false): Unit = {
    val tryDeleteRecords = Try(deleteRecordService.deleteRecords(dataSourceName, trackedRecords, step.options))
    tryDeleteRecords match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to delete records from data source, not deleting tracking file, data-source-name=$dataSourceName, details=${cleanseOptions(step.options)}", exception)
      case Success(_) =>
        LOGGER.info(s"Successfully deleted records from data source, data-source-name=$dataSourceName, details=${cleanseOptions(step.options)}")
        // don't delete tracked records file when deleting data from foreign key source that is marked as a delete foreign key
        if (!isDeleteForeignKey) {
          deleteTrackedRecordsFile(subDataSourcePath)
        }
    }
  }

  private def getTrackedRecords(dataSourcePath: String): DataFrame = {
    sparkSession.read.format(RECORD_TRACKING_VALIDATION_FORMAT)
      .option(PATH, dataSourcePath)
      .load()
  }

  private def deleteTrackedRecordsFile(dataSourcePath: String): Unit = {
    new Directory(new File(dataSourcePath)).deleteRecursively()
  }

  private def getDeleteRecordService(format: String): Option[DeleteRecordService] = {
    format.toLowerCase match {
      case JDBC => Some(new JdbcDeleteRecordService)
      case CASSANDRA => Some(new CassandraDeleteRecordService)
      case PARQUET | JSON | CSV | ORC | DELTA => Some(new LocalFileDeleteRecordService)
      case x =>
        LOGGER.warn(s"Unsupported data source for deleting records, format=$x")
        None
    }
  }

}
