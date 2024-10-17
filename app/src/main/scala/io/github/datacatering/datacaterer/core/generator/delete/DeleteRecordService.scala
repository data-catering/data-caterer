package io.github.datacatering.datacaterer.core.generator.delete

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DeleteRecordService extends Serializable {

  lazy val BATCH_SIZE = 1000

  def deleteRecords(dataSourceName: String, trackedRecords: DataFrame, options: Map[String, String])(implicit sparkSession: SparkSession): Unit
}
