package io.github.datacatering.datacaterer.core.generator.metadata.datasource.file

import io.github.datacatering.datacaterer.api.model.Constants.{DELTA, FORMAT, PARQUET, PATH}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import org.apache.spark.sql.SparkSession

case class FileMetadata(name: String, format: String, connectionConfig: Map[String, String]) extends DataSourceMetadata {

  override val hasSourceData: Boolean = true

  /**
   * Given a folder pathway, get all types of files and their corresponding pathways and other connection related metadata
   * that could be used for spark.read.options
   *
   * @return Array of connection config for files
   */
  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    val baseFolderPath = connectionConfig(PATH)
    val fileSuffix = format match {
      case DELTA => PARQUET
      case x => x
    }
    val df = sparkSession.read
      .option("pathGlobFilter", s"*.$fileSuffix")
      .option("recursiveFileLookup", "true")
      .text(baseFolderPath)
      .selectExpr("input_file_name() AS file_path")
    val filePaths = df.distinct().collect()
      .map(r => r.getAs[String]("file_path"))
    val baseFilePaths = getBaseFolderPathways(filePaths)
    baseFilePaths.map(p => SubDataSourceMetadata(connectionConfig ++ Map(PATH -> p.replaceFirst(".+?://", ""), FORMAT -> format)))
  }

  private def getBaseFolderPathways(filePaths: Array[String]): Array[String] = {
    val partitionedFiles = filePaths.filter(_.contains("="))
      .map(path => {
        val spt = path.split("/")
        val baseFolderPath = spt.takeWhile(!_.contains("="))
        baseFolderPath.mkString("/")
      }).distinct
    val nonPartitionedFiles = filePaths.filter(!_.contains("="))
      .map(path => {
        val spt = path.split("/")
        val baseFolderPath = spt.slice(0, spt.length - 1)
        baseFolderPath.mkString("/")
      }).distinct
    partitionedFiles ++ nonPartitionedFiles
  }
}
