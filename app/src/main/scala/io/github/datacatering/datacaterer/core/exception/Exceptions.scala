package io.github.datacatering.datacaterer.core.exception

import io.github.datacatering.datacaterer.api.model.Constants.{DATA_CONTRACT_FILE, FOREIGN_KEY_DELIMITER, FOREIGN_KEY_PLAN_FILE_DELIMITER}
import io.github.datacatering.datacaterer.api.model.{Count, Field, Step}
import org.apache.spark.sql.types.{DataType, Metadata, StructField}

//files
case class ParseFileException(filePath: String, parseToType: String, throwable: Throwable) extends RuntimeException(
  s"Failed to parse file to expected type, file=$filePath, parse-to-type=$parseToType",
  throwable
)

case class SaveFileException(filePath: String, throwable: Throwable) extends RuntimeException(
  s"Failed to save to file system, file-path=$filePath",
  throwable
)

//Foreign key
case class InvalidForeignKeyFormatException(foreignKey: String) extends RuntimeException(
  s"Unexpected foreign key relation format. Should have at least 2 or 3 parts delimited " +
  s"by either $FOREIGN_KEY_DELIMITER or $FOREIGN_KEY_PLAN_FILE_DELIMITER, foreign-key=$foreignKey"
)

case class MissingDataSourceFromForeignKeyException(dataSourceName: String) extends RuntimeException(
  s"Cannot create target foreign key as one of the data sources not created. " +
    s"Please ensure there exists a data source with name (<plan dataSourceName>.<task step name>): $dataSourceName"
)

//UI exceptions
case class MissingConnectionForForeignKeyException(taskName: String) extends RuntimeException(
  s"No connection found with task name, task-name=$taskName"
)

case class InvalidMetadataDataSourceOptionsException(metadataSourceName: String) extends RuntimeException(
  s"Metadata source details/options should be non-empty, metadata-source-name=$metadataSourceName"
)

//data generation
case class UnsupportedDataGeneratorType(returnType: String) extends RuntimeException(
  s"Unsupported return type for data generator: type=$returnType"
)

case class UnsupportedRealTimeDataSourceFormat(format: String) extends RuntimeException(
  s"Unsupported data source format for creating real-time data, format=$format"
)

case class UnsupportedFakerReturnTypeException(expression: String, returnClass: AnyRef) extends RuntimeException(
  s"Unsupported return type from Faker for expression, expression=$expression, return-type=${returnClass.getClass.getName}"
)

case class InvalidDataGeneratorConfigurationException(structField: StructField, undefinedMetadataField: String) extends RuntimeException(
  s"Undefined configuration in metadata for the data generator defined. Please help to define 'undefined-metadata-field' " +
    s"in field 'metadata' to allow data to be generated, " +
    s"name=${structField.name}, data-type=${structField.dataType}, undefined-metadata-field=$undefinedMetadataField, metadata=${structField.metadata}"
)

case class InvalidFakerExpressionException(expression: String) extends RuntimeException(
  s"Expressions require '.' in definition (i.e. #{Name.name}), expression=$expression"
)

case class AddHttpHeaderException(headerKey: String, throwable: Throwable) extends RuntimeException(
  s"Failed to add HTTP header to request, header-key=$headerKey",
  throwable
)

case class FailedHttpRequestException() extends Exception("Failed HTTP request")

case class FailedSaveDataException(dataSourceName: String, stepName: String, saveMode: String, count: String, throwable: Throwable) extends RuntimeException(
  s"Failed to save data for sink, data-source-name=$dataSourceName, step-name=$stepName, " +
    s"save-mode=$saveMode, num-records=$count",
  throwable
)

case class FailedSaveDataDataFrameV2Exception(tableName: String, saveMode: String, throwable: Throwable) extends RuntimeException(
  s"Failed to save data for sink, table-name=$tableName, save-mode=$saveMode",
  throwable
)

case class ExhaustedUniqueValueGenerationException(retryCount: Int, name: String, metadata: Metadata, sample: List[String]) extends RuntimeException(
  s"Failed to generate new unique value for field, retries=$retryCount, name=$name, " +
    s"metadata=$metadata, sample-previously-generated=${sample.mkString(",")}"
)

//data validation
case class UnsupportedDataValidationTypeException(validationType: String) extends RuntimeException(
  s"Unsupported validation type, validation=$validationType"
)

case class UnsupportedDataValidationAggregateFunctionException(function: String, field: String) extends RuntimeException(
  s"Unknown or unsupported aggregate function name for data quality rule, function-name=$function, field=$field"
)

case class DataValidationMissingUpstreamDataSourceException(name: String) extends RuntimeException(
  s"Failed to find upstream data source configuration, data-source-name=$name"
)

//delete data
case class UnsupportedJdbcDeleteDataType(dataType: DataType, table: String) extends RuntimeException(
  s"Unsupported data type for deleting from JDBC data source: type=$dataType, table=$table"
)

case class InvalidDataSourceOptions(dataSourceName: String, missingConfig: String) extends RuntimeException(
  s"Missing config for data source connection, data-source-name=$dataSourceName, missing-config=$missingConfig"
)

//data tracking
case class UnsupportedDataFormatForTrackingException(format: String) extends RuntimeException(
  s"Unsupported data format for record tracking, format=$format"
)

//plan
case class PlanRunClassNotFoundException(className: String) extends RuntimeException(
  s"Failed to load class as either Java or Scala PlanRun, class=$className"
)

case class InvalidStepCountGeneratorConfigurationException(step: Step) extends RuntimeException(
  s"'total' or 'generator' needs to be defined in count for step, " +
    s"step-name=${step.name}, schema=${step.fields}, count=${step.count}"
)

case class InvalidFieldConfigurationException(field: Field) extends RuntimeException(
  s"Field should have ('generator' and 'type' defined) or 'schema' defined, name=${field.name}"
)

case class InvalidWaitConditionException(waitCondition: String) extends RuntimeException(
  s"Invalid wait condition for validation, wait-condition=$waitCondition"
)

case class InvalidConnectionException(connectionName: String) extends RuntimeException(
  "Connection does not follow defined pattern, it should contain at least 3 sections " +
    s"(connection name, type and group type), connection-name=$connectionName"
)

case class InvalidRandomSeedException(throwable: Throwable) extends RuntimeException(
  s"Failed to get random seed value from plan",
  throwable
)

case class NoSchemaDefinedException() extends RuntimeException(
  "Schema not defined from auto generation, metadata source or from user"
)

//data contract
case class InvalidDataContractFileFormatException(dataContractPath: String, throwable: Throwable) extends RuntimeException(
  s"Failed to parse data contract file, data-contract-path=$dataContractPath",
  throwable
)

case class DataContractModelNotFoundException(model: String, id: String) extends RuntimeException(
  s"Model name not found in data contract file, model=$model, data-contract-id=$id"
)

case class MissingDataContractFilePathException(name: String, format: String) extends RuntimeException(
  s"No $DATA_CONTRACT_FILE defined for data contract metadata source, name=$name, format=$format"
)

//openapi
case class MissingOpenApiServersException(openApiSchema: String) extends RuntimeException(
  s"Unable to get base URL from OpenAPI spec, please define at least one URL in servers, schema-location=$openApiSchema"
)

case class UnsupportedOpenApiDataTypeException(fieldType: String, fieldFormat: String, throwable: Throwable) extends RuntimeException(
  s"Unable to convert field to data type, field-type=$fieldType, field-format=$fieldFormat",
  throwable
)

//openlineage
case class FailedMarquezHttpCallException(url: String, throwable: Throwable) extends RuntimeException(
  s"Failed to call HTTP url, url=$url",
  throwable
)

case class InvalidMarquezResponseException(throwable: Throwable) extends RuntimeException(
  "Failed to parse response from Marquez",
  throwable
)

//openmetadata
case class FailedRetrieveOpenMetadataTestCasesException(errors: List[String]) extends RuntimeException(
  s"Failed to retrieve test cases from OpenMetadata, errors=${errors.mkString(",")}"
)

case class InvalidFullQualifiedNameException(fqn: String) extends RuntimeException(
  s"Unexpected number of '.' in fullyQualifiedName for table, expected 3 '.', " +
    s"unable to extract schema and table name, name=$fqn"
)

case class InvalidOpenMetadataConnectionConfigurationException(key: String) extends RuntimeException(
  s"Failed to get configuration value for OpenMetadata connection, key=$key"
)

case class InvalidOpenMetadataTableRowCountBetweenException() extends RuntimeException(
  "Expected at least one of 'minValue' or 'maxValue' to be defined in test parameters from OpenMetadata"
)

//confluent schema registry
case class FailedConfluentSchemaRegistryHttpCallException(url: String, throwable: Throwable) extends RuntimeException(
  s"Failed to call HTTP url for Confluent Schema Registry, url=$url",
  throwable
)

case class FailedConfluentSchemaRegistryResponseException(url: String, statusCode: Int, statusText: String) extends RuntimeException(
  s"Got non 200 response from HTTP call to Confluent Schema Registry, url=$url, status-code=$statusCode, status-text=$statusText"
)

case class InvalidConfluentSchemaRegistryResponseException(throwable: Throwable) extends RuntimeException(
  "Failed to parse response from Confluent Schema Registry",
  throwable
)

case class InvalidConfluentSchemaRegistrySchemaRequestException(missingField: String) extends RuntimeException(
  s"Required field for Confluent Schema Registry Schema Request is missing, missing-field(s)=$missingField"
)

//jms
case class FailedJmsMessageSendException(throwable: Throwable) extends RuntimeException(throwable)

case class FailedJmsMessageCreateException(throwable: Throwable) extends RuntimeException(throwable)

case class FailedJmsMessageGetBodyException(throwable: Throwable) extends RuntimeException(throwable)

//row
case class InvalidFieldAsDataTypeException(fieldName: String, throwable: Throwable) extends RuntimeException(
  s"Failed to get field as data type, field-name=$fieldName",
  throwable
)

case class MissingFieldException(fieldName: String) extends RuntimeException(
  s"Invalid schema definition due to missing field, field-name=$fieldName"
)

//protobuf
case class UnsupportedProtobufType(protobufType: String) extends RuntimeException(
  s"Unsupported protobuf data type, protobuf-type=$protobufType"
)

case class InvalidNumberOfProtobufMessages(filePath: String) extends RuntimeException(
  s"Only one message can be defined in .proto file for parsing, protobuf-file=$filePath"
)
