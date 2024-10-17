package io.github.datacatering.datacaterer.core.generator.metadata.datasource.datacontractcli

import io.github.datacatering.datacaterer.api.model.Constants.{DATA_CONTRACT_FILE, DATA_CONTRACT_SCHEMA, FIELD_DATA_TYPE, FORMAT, IS_UNIQUE, JSON, KAFKA_TOPIC, MAXIMUM, MAXIMUM_LENGTH, METADATA_IDENTIFIER, MINIMUM, MINIMUM_LENGTH, ONE_OF_GENERATOR, PARQUET, PATH, REGEX_GENERATOR, SCHEMA, URL}
import io.github.datacatering.datacaterer.api.model.{ArrayType, BinaryType, BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType, TimestampType}
import io.github.datacatering.datacaterer.core.exception.{DataContractModelNotFoundException, InvalidDataContractFileFormatException, MissingDataContractFilePathException}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.datacontractcli.model.{DataContractSpecification, Field, FieldTypeEnum, KafkaServer, LocalServer, Model, PostgresServer, S3Server, SftpServer}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.{Failure, Success, Try}

case class DataContractCliDataSourceMetadata(
                                              name: String,
                                              format: String,
                                              connectionConfig: Map[String, String],
                                            ) extends DataSourceMetadata {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override val hasSourceData: Boolean = false

  override def toStepName(options: Map[String, String]): String = {
    options(METADATA_IDENTIFIER)
  }

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    val optDataContractFile = connectionConfig.get(DATA_CONTRACT_FILE)

    optDataContractFile match {
      case Some(dataContractPath) =>
        val dataContractFile = new File(dataContractPath)
        val tryParseYaml = Try(ObjectMapperUtil.yamlObjectMapper.readValue(dataContractFile, classOf[DataContractSpecification]))
        tryParseYaml match {
          case Failure(exception) => throw InvalidDataContractFileFormatException(dataContractPath, exception)
          case Success(value) =>
              //TODO server details can include variables such as '{model}' that need to be replaced with each of the model names
//            val serverOptions = parseServerOptions(value, dataContractPath)
            mapToArrayOfSubDataSourceMetadata(value)
        }
      case None => throw MissingDataContractFilePathException(name, format)
    }
  }

  private def mapToArrayOfSubDataSourceMetadata(value: DataContractSpecification)
                                               (implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    def mapToSubDataSourceMetadata(serverOptions: Map[String, String], m: (String, Model)) = {
      val name = m._1
      val mappedFields = getFieldMetadata(m._2.fields, name)
      val parsedFieldMetadata = sparkSession.createDataset(mappedFields)
      SubDataSourceMetadata(Map(METADATA_IDENTIFIER -> name) ++ serverOptions, Some(parsedFieldMetadata))
    }

    val optDataContractSchema = connectionConfig.get(DATA_CONTRACT_SCHEMA)
    val serverOptions = connectionConfig
    value.models.map(modelsMap => {
      optDataContractSchema.map(modelName => {
        val optModelSchema = modelsMap.get(modelName)
        if (optModelSchema.isEmpty) {
          throw DataContractModelNotFoundException(modelName, value.id)
        } else {
          Array(mapToSubDataSourceMetadata(serverOptions, modelName -> optModelSchema.get))
        }
      }).getOrElse(modelsMap.map(m => mapToSubDataSourceMetadata(serverOptions, m)).toArray)
    }).getOrElse(Array())
  }

  private def parseServerOptions(contract: DataContractSpecification, dataContractPath: String): Map[String, String] = {
    contract.servers.map(serverMap => {
      serverMap.head._2 match {
        case LocalServer(path, format) =>
          Map(
            PATH -> path,
            FORMAT -> format
          )
        case S3Server(location, _, format, _) =>
          Map(
            PATH -> location,
            FORMAT -> format.getOrElse(PARQUET)
          )
        case SftpServer(location, format, _) =>
          Map(
            PATH -> location,
            FORMAT -> format.getOrElse(PARQUET)
          )
        case PostgresServer(host, port, database, schema) =>
          Map(
            URL -> s"jdbc:postgresql://$host:$port/$database",
            SCHEMA -> schema
          )
        case KafkaServer(host, topic, format) =>
          Map(
            URL -> host,
            KAFKA_TOPIC -> topic,
            FORMAT -> format.getOrElse(JSON)
          )
        case x =>
          LOGGER.warn(s"Unsupported data source type, unable to extract out connection options from Data Contract CLI file, " +
            s"data-contract-path=$dataContractPath, data-source-type=${x.`type`}")
          Map[String, String]()
      }
    }).getOrElse(Map())
  }

  private def getFieldMetadata(fields: Map[String, Field], metadataIdentifier: String): List[FieldMetadata] = {
    fields.map(f => {
      val convertedType = getDataType(f._2)
      val options = getFieldOptions(f._2, convertedType)
      val nestedFields = if (convertedType == StructType) getFieldMetadata(f._2.fields.getOrElse(Map()), metadataIdentifier) else List()
      FieldMetadata(f._1, Map(METADATA_IDENTIFIER -> metadataIdentifier), options, nestedFields)
    }).toList
  }

  private def getFieldOptions(field: Field, dataType: DataType): Map[String, String] = {
    val contractOptionsMapping = Map(
      ONE_OF_GENERATOR -> field.`enum`.map(e => e.mkString(",")),
      IS_UNIQUE -> field.unique.map(_.toString),
      MINIMUM_LENGTH -> field.minLength.map(_.toString),
      MAXIMUM_LENGTH -> field.maxLength.map(_.toString),
      REGEX_GENERATOR -> field.pattern,
      MINIMUM -> field.minimum.map(_.toString),
      MAXIMUM -> field.maximum.map(_.toString),
    )
    val options = contractOptionsMapping.filter(x => x._2.isDefined)
      .map(x => x._1 -> x._2.get)
    options ++ Map(FIELD_DATA_TYPE -> dataType.toString())
  }

  private def getDataType(field: Field): DataType = {
    field.`type` match {
      case FieldTypeEnum.number | FieldTypeEnum.numeric | FieldTypeEnum.double => DoubleType
      case FieldTypeEnum.decimal | FieldTypeEnum.bigint => DecimalType
      case FieldTypeEnum.int | FieldTypeEnum.integer => IntegerType
      case FieldTypeEnum.long => LongType
      case FieldTypeEnum.float => FloatType
      case FieldTypeEnum.string | FieldTypeEnum.text | FieldTypeEnum.varchar => StringType
      case FieldTypeEnum.boolean => BooleanType
      case FieldTypeEnum.timestamp | FieldTypeEnum.timestamp_tz | FieldTypeEnum.timestamp_ntz => TimestampType
      case FieldTypeEnum.date => DateType
      case FieldTypeEnum.array => ArrayType
      case FieldTypeEnum.map | FieldTypeEnum.`object` | FieldTypeEnum.record | FieldTypeEnum.struct => StructType
      case FieldTypeEnum.bytes => BinaryType
      case FieldTypeEnum.`null` => StringType
    }
  }
}
