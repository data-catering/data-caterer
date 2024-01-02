package io.github.datacatering.datacaterer.core.exception

import io.github.datacatering.datacaterer.api.model.{Field, Step}
import org.apache.spark.sql.types.StructField

class ParseFileException(filePath: String, parseToType: String, throwable: Throwable) extends RuntimeException(throwable) {
  override def getMessage: String = s"Failed to parse file to expected type, file=$filePath, parse-to-type=$parseToType"
}

class UnsupportedDataGeneratorType(returnType: String) extends RuntimeException {
  override def getMessage: String = s"Unsupported return type for data generator: type=$returnType"
}

class UnsupportedRealTimeDataSourceFormat(format: String) extends RuntimeException {
  override def getMessage: String = s"Unsupported data source format for creating real-time data, format=$format"
}

class InvalidDataGeneratorConfigurationException(structField: StructField, undefinedMetadataField: String) extends RuntimeException {
  override def getMessage: String = s"Undefined configuration in metadata for the data generator defined. Please help to define 'undefined-metadata-field' " +
    s"in field 'metadata' to allow data to be generated, " +
    s"name=${structField.name}, data-type=${structField.dataType}, undefined-metadata-field=$undefinedMetadataField, metadata=${structField.metadata}"
}

class InvalidStepCountGeneratorConfigurationException(step: Step) extends RuntimeException {
  override def getMessage: String = s"'total' or 'generator' needs to be defined in count for step, step-name=${step.name}, schema=${step.schema}, count=${step.count}"
}

class InvalidFieldConfigurationException(field: Field) extends RuntimeException {
  override def getMessage: String = s"Field should have ('generator' and 'type' defined) or 'schema' defined, name=${field.name}"
}

class InvalidWaitConditionException(waitCondition: String) extends RuntimeException {
  override def getMessage: String = s"Invalid wait condition for validation, wait-condition=$waitCondition"
}
