package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Constants.{DELTA, DELTA_LAKE_SPARK_CONF, DRIVER, FORMAT, ICEBERG, ICEBERG_SPARK_CONF, JDBC, OMIT, PARTITIONS, PARTITION_BY, PATH, POSTGRES_DRIVER, SAVE_MODE, SPARK_ICEBERG_CATALOG_TYPE, SPARK_ICEBERG_CATALOG_WAREHOUSE, TABLE}
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, MetadataConfig, Step}
import io.github.datacatering.datacaterer.core.model.Constants.{FAILED, FINISHED, STARTED}
import io.github.datacatering.datacaterer.core.model.SinkResult
import io.github.datacatering.datacaterer.core.util.ConfigUtil
import io.github.datacatering.datacaterer.core.util.MetadataUtil.getFieldMetadata
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{CreateTableWriter, DataFrame, DataFrameWriter, DataFrameWriterV2, Dataset, Row, SaveMode, SparkSession}

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

class SinkFactory(val flagsConfig: FlagsConfig, val metadataConfig: MetadataConfig)(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private var HAS_LOGGED_COUNT_DISABLE_WARNING = false

  def pushToSink(df: DataFrame, dataSourceName: String, step: Step, startTime: LocalDateTime): SinkResult = {
    val dfWithoutOmitFields = removeOmitFields(df)
    val saveMode = step.options.get(SAVE_MODE).map(_.toLowerCase.capitalize).map(SaveMode.valueOf).getOrElse(SaveMode.Append)
    val format = step.options(FORMAT)
    val enrichedConnectionConfig = additionalConnectionConfig(format, step.options)

    val count = if (flagsConfig.enableCount) {
      dfWithoutOmitFields.count().toString
    } else if (!HAS_LOGGED_COUNT_DISABLE_WARNING) {
      LOGGER.warn("Count is disabled. It will help with performance. Defaulting to -1")
      HAS_LOGGED_COUNT_DISABLE_WARNING = true
      "-1"
    } else "-1"
    LOGGER.info(s"Pushing data to sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, num-records=$count, status=$STARTED")
    saveData(dfWithoutOmitFields, dataSourceName, step, enrichedConnectionConfig, saveMode, format, count, flagsConfig.enableFailOnError, startTime)
  }

  private def saveData(df: DataFrame, dataSourceName: String, step: Step, connectionConfig: Map[String, String],
                       saveMode: SaveMode, format: String, count: String, enableFailOnError: Boolean, startTime: LocalDateTime): SinkResult = {
    val baseSinkResult = SinkResult(dataSourceName, format, saveMode.name())
    //TODO might have use case where empty data can be tested, is it okay just to check for empty schema?
    val finalSinkResult = if (df.schema.isEmpty) {
      LOGGER.warn(s"Data schema is empty, not saving to data source, data-source-name=$dataSourceName, format=$format")
      baseSinkResult
    } else {
      val sinkResult = saveBatchData(dataSourceName, df, saveMode, connectionConfig, count, startTime)
      (sinkResult.isSuccess, sinkResult.exception) match {
        case (false, Some(exception)) =>
          LOGGER.error(s"Failed to save data for sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, " +
            s"num-records=$count, status=$FAILED, exception=${exception.getMessage.take(500)}")
          if (enableFailOnError) throw new RuntimeException(exception) else baseSinkResult
        case (true, None) =>
          LOGGER.info(s"Successfully saved data to sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, " +
            s"num-records=$count, status=$FINISHED")
          sinkResult
        case (isSuccess, optException) =>
          LOGGER.warn(s"Unexpected sink result scenario, is-success=$isSuccess, exception-exists=${optException.isDefined}")
          sinkResult
      }
    }

    df.unpersist()
    finalSinkResult
  }

  private def saveBatchData(dataSourceName: String, df: DataFrame, saveMode: SaveMode, connectionConfig: Map[String, String],
                            count: String, startTime: LocalDateTime): SinkResult = {
    val format = connectionConfig(FORMAT)

    // if format is iceberg, need to use dataframev2 api for partition and writing
    connectionConfig.filter(_._1.startsWith("spark.sql"))
      .foreach(conf => df.sqlContext.setConf(conf._1, conf._2))
    val trySaveData = if (format == ICEBERG) {
      Try(tryPartitionAndSaveDfV2(df, saveMode, connectionConfig))
    } else {
      val partitionedDf = partitionDf(df, connectionConfig)
      Try(partitionedDf
        .format(format)
        .mode(saveMode)
        .options(connectionConfig)
        .save())
    }

    val optException = trySaveData match {
      case Failure(exception) => Some(exception)
      case Success(_) => None
    }
    mapToSinkResult(dataSourceName, df, saveMode, connectionConfig, count, format, trySaveData.isSuccess, startTime, optException)
  }

  private def partitionDf(df: DataFrame, stepOptions: Map[String, String]): DataFrameWriter[Row] = {
    val partitionDf = stepOptions.get(PARTITIONS)
      .map(partitionNum => df.repartition(partitionNum.toInt)).getOrElse(df)
    stepOptions.get(PARTITION_BY)
      .map(partitionCols => partitionDf.write.partitionBy(partitionCols.split(",").map(_.trim): _*))
      .getOrElse(partitionDf.write)
  }

  private def tryPartitionAndSaveDfV2(df: DataFrame, saveMode: SaveMode, stepOptions: Map[String, String]): Unit = {
    val tableName = s"$ICEBERG.${stepOptions(TABLE)}"
    val repartitionDf = stepOptions.get(PARTITIONS)
      .map(partitionNum => df.repartition(partitionNum.toInt)).getOrElse(df)
    val baseTable = repartitionDf.writeTo(tableName).options(stepOptions)

    stepOptions.get(PARTITION_BY)
      .map(partitionCols => {
        val spt = partitionCols.split(",").map(c => col(c.trim))

        val partitionedDf = if (spt.length > 1) {
          baseTable.partitionedBy(spt.head, spt.tail: _*)
        } else if (spt.length == 1) {
          baseTable.partitionedBy(spt.head)
        } else {
          baseTable
        }

        saveDataframeV2(saveMode, tableName, baseTable, partitionedDf)
      })
      .getOrElse(saveDataframeV2(saveMode, tableName, baseTable, baseTable))
  }

  private def saveDataframeV2(saveMode: SaveMode, tableName: String, baseDf: DataFrameWriterV2[Row], partitionedDf: CreateTableWriter[Row]): Unit = {
    saveMode match {
      case SaveMode.Append | SaveMode.Ignore =>
        val tryCreate = Try(partitionedDf.create())
        tryCreate match {
          case Failure(exception) =>
            if (exception.isInstanceOf[TableAlreadyExistsException]) {
              LOGGER.debug(s"Table already exists, appending to existing table, table-name=$tableName")
              baseDf.append()
            } else {
              throw new RuntimeException(exception)
            }
          case Success(_) =>
            LOGGER.debug(s"Successfully created partitioned table, table-name=$tableName")
        }
      case SaveMode.Overwrite => baseDf.overwritePartitions()
      case SaveMode.ErrorIfExists => partitionedDf.create()
    }
  }

  private def additionalConnectionConfig(format: String, connectionConfig: Map[String, String]): Map[String, String] = {
    format match {
      case JDBC => if (connectionConfig(DRIVER).equalsIgnoreCase(POSTGRES_DRIVER) && !connectionConfig.contains("stringtype")) {
        connectionConfig ++ Map("stringtype" -> "unspecified")
      } else connectionConfig
      case DELTA => connectionConfig ++ DELTA_LAKE_SPARK_CONF
      case ICEBERG => connectionConfig ++ ICEBERG_SPARK_CONF
      case _ => connectionConfig
    }
  }

  private def mapToSinkResult(dataSourceName: String, df: DataFrame, saveMode: SaveMode, connectionConfig: Map[String, String],
                              count: String, format: String, isSuccess: Boolean, startTime: LocalDateTime,
                              optException: Option[Throwable]): SinkResult = {
    val cleansedOptions = ConfigUtil.cleanseOptions(connectionConfig)
    val sinkResult = SinkResult(dataSourceName, format, saveMode.name(), cleansedOptions, count.toLong, isSuccess, Array(), startTime, exception = optException)

    if (flagsConfig.enableSinkMetadata) {
      val sample = df.take(metadataConfig.numGeneratedSamples).map(_.json)
      val fields = getFieldMetadata(dataSourceName, df, connectionConfig, metadataConfig)
      sinkResult.copy(generatedMetadata = fields, sample = sample)
    } else {
      sinkResult
    }
  }

  private def removeOmitFields(df: DataFrame) = {
    val dfOmitFields = df.schema.fields
      .filter(field => field.metadata.contains(OMIT) && field.metadata.getString(OMIT).equalsIgnoreCase("true"))
      .map(_.name)
    val dfWithoutOmitFields = df.selectExpr(df.columns.filter(c => !dfOmitFields.contains(c)): _*)
    if (!dfWithoutOmitFields.storageLevel.useMemory) dfWithoutOmitFields.cache()
    dfWithoutOmitFields
  }
}
