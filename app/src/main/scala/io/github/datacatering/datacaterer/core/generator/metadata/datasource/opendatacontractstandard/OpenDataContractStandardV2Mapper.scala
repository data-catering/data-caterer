package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{ArrayType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType, TimestampType}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.SubDataSourceMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard.model.{OpenDataContractStandard, OpenDataContractStandardColumn, OpenDataContractStandardDataset}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object OpenDataContractStandardV2Mapper {

  private val LOGGER = Logger.getLogger(getClass.getName)

  implicit val columnMetadataEncoder: Encoder[FieldMetadata] = Encoders.kryo[FieldMetadata]

  def toSubDataSourceMetadata(
                               value: OpenDataContractStandard,
                               dataset: OpenDataContractStandardDataset,
                               connectionConfig: Map[String, String]
                             )(implicit sparkSession: SparkSession): SubDataSourceMetadata = {
    val readOptions = getDataSourceOptions(value, dataset, connectionConfig)
    val columnMetadata = dataset.columns.map(cols => {
      val mappedColumns = cols.map(column => {
        val dataType = getDataType(dataset, column)
        val metadata = getBaseColumnMetadata(column, dataType)
        FieldMetadata(column.column, readOptions, metadata)
      }).toList
      sparkSession.createDataset(mappedColumns)
    })
    SubDataSourceMetadata(readOptions, columnMetadata)
  }

  private def getDataSourceOptions(
                                    contract: OpenDataContractStandard,
                                    dataset: OpenDataContractStandardDataset,
                                    connectionConfig: Map[String, String]
                                  ): Map[String, String] = {
    //identifier should be based on schema name
    val baseMap = Map(METADATA_IDENTIFIER -> contract.uuid)
    //Tables, BigQuery, serverName, jdbc:driver, myDatabase, user, pw
    //(contract.`type`, contract.sourceSystem, contract.server, contract.driver, contract.database, contract.username, contract.password)
    val serverMap = contract.server.map(server => Map(URL -> server)).getOrElse(Map())
    val credentialsMap = Map(USERNAME -> contract.username, PASSWORD -> contract.password)
      .filter(_._2.nonEmpty)
      .map(kv => (kv._1, kv._2.getOrElse("")))
    val dataSourceMap = if (contract.driver.map(_.toLowerCase).contains(CASSANDRA_NAME) && contract.database.nonEmpty) {
      Map(
        FORMAT -> CASSANDRA,
        CASSANDRA_KEYSPACE -> contract.database.getOrElse(""),
        CASSANDRA_TABLE -> dataset.table
      )
    } else if (contract.driver.map(_.toLowerCase).contains(JDBC)) {
      Map(
        FORMAT -> JDBC,
        DRIVER -> contract.driver.getOrElse(""),
        JDBC_TABLE -> dataset.table
      )
    } else {
      contract.`type`.map(_.toLowerCase) match {
        case Some(CSV) | Some(JSON) | Some(PARQUET) | Some(ORC) | Some(KAFKA) | Some(HTTP) | Some(SOLACE) =>
          Map(FORMAT -> contract.`type`.get.toLowerCase)
        case _ =>
          LOGGER.warn(s"Defaulting to format CSV since contract type is not supported, name=${contract.datasetName}, type=${contract.`type`.getOrElse("")}")
          Map(FORMAT -> CSV)
        //        case _ => throw new RuntimeException(s"Unable to determine data source type from ODCS file, name=${contract.datasetName}, type=${contract.`type`.getOrElse("")}")
      }
    }

    baseMap ++ connectionConfig
  }

  private def getBaseColumnMetadata(column: OpenDataContractStandardColumn, dataType: DataType): Map[String, String] = {
    Map(
      FIELD_DATA_TYPE -> dataType.toString(),
      IS_NULLABLE -> column.isNullable.getOrElse(false).toString,
      ENABLED_NULL -> column.isNullable.getOrElse(false).toString,
      IS_PRIMARY_KEY -> column.isPrimaryKey.getOrElse(false).toString,
      PRIMARY_KEY_POSITION -> column.primaryKeyPosition.getOrElse("-1").toString,
      CLUSTERING_POSITION -> column.clusterKeyPosition.getOrElse("-1").toString,
      IS_UNIQUE -> column.isUnique.getOrElse(false).toString,
    )
  }

  private def getDataType(dataset: OpenDataContractStandardDataset, column: OpenDataContractStandardColumn): DataType = {
    column.logicalType.toLowerCase match {
      case "string" => StringType
      case "integer" => IntegerType
      case "double" => DoubleType
      case "float" => FloatType
      case "long" => LongType
      case "date" => DateType
      case "timestamp" => TimestampType
      case "array" => new ArrayType(StringType) //TODO: wait for how inner data type of array will be defined
      case "object" => new StructType(List()) //TODO: wait for how nested data structures will be made
      case x =>
        LOGGER.warn(s"Unable to find corresponding known data type for column in ODCS file, defaulting to string, dataset=${dataset.table} column=$column, data-type=$x")
        StringType
    }
  }

}
