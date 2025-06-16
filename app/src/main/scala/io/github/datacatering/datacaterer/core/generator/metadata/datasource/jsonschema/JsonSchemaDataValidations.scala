package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model.{JsonSchemaDefinition, JsonSchemaProperty}
import org.apache.log4j.Logger

object JsonSchemaDataValidations {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Convert JSON schema definition to Data Caterer validations
   */
  def getDataValidations(schema: JsonSchemaDefinition, schemaPath: String): List[ValidationBuilder] = {
    LOGGER.info(s"Converting JSON schema to validations, schema-path=$schemaPath, schema-type=${schema.`type`}")
    
    schema.`type` match {
      case Some("object") =>
        val objectValidations = getObjectValidations(schema)
        val propertyValidations = schema.properties.map(_.flatMap { case (fieldName, property) =>
          getPropertyValidations(fieldName, property)
        }.toList).getOrElse(List())
        objectValidations ++ propertyValidations
      case Some("array") =>
        getArrayValidations(schema)
      case _ =>
        // For primitive types at root level
        getRootTypeValidations(schema)
    }
  }

  /**
   * Get validations for object-type schemas
   */
  private def getObjectValidations(schema: JsonSchemaDefinition): List[ValidationBuilder] = {
    val validations = scala.collection.mutable.ListBuffer[ValidationBuilder]()

    // Required fields validation
    schema.required.foreach { requiredFields =>
      requiredFields.foreach { fieldName =>
        LOGGER.debug(s"Creating required field validation for: $fieldName")
        validations += ValidationBuilder()
          .field(fieldName)
          .isNull(true)
          .description(s"Field '$fieldName' is required")
      }
    }

    // Additional properties validation (if additionalProperties is false)
    if (schema.additionalProperties.contains(false)) {
      schema.properties.foreach { props =>
        val allowedFields = props.keys.toList
        LOGGER.debug(s"Creating additional properties validation, allowed-fields=${allowedFields.mkString(",")}")
        // This would need custom validation logic in Data Caterer to check field names
        validations += ValidationBuilder()
          .fieldNames
          .countEqual(allowedFields.size)
          .description("No additional properties allowed beyond schema definition")
      }
    }

    validations.toList
  }

  /**
   * Get validations for array-type schemas
   */
  private def getArrayValidations(schema: JsonSchemaDefinition): List[ValidationBuilder] = {
    val validations = scala.collection.mutable.ListBuffer[ValidationBuilder]()

    // Array length validations
    (schema.minItems, schema.maxItems) match {
      case (Some(min), Some(max)) =>
        LOGGER.debug(s"Creating array length validation: $min <= length <= $max")
        validations += ValidationBuilder()
          .field("root_array")
          .lengthBetween(min.toInt, max.toInt)
          .description(s"Array length must be between $min and $max")
      case (Some(min), None) =>
        LOGGER.debug(s"Creating array minimum length validation: length >= $min")
        validations += ValidationBuilder()
          .field("root_array")
          .expr(s"SIZE(root_array) >= $min")
          .description(s"Array length must be at least $min")
      case (None, Some(max)) =>
        LOGGER.debug(s"Creating array maximum length validation: length <= $max")
        validations += ValidationBuilder()
          .field("root_array")
          .expr(s"SIZE(root_array) <= $max")
          .description(s"Array length must be at most $max")
      case _ => // No length constraints
    }

    // Unique items validation
    if (schema.uniqueItems.contains(true)) {
      LOGGER.debug("Creating unique items validation for array")
      validations += ValidationBuilder()
        .field("root_array")
        .expr("SIZE(root_array) == SIZE(ARRAY_DISTINCT(root_array))")
        .description("Array items must be unique")
    }

    validations.toList
  }

  /**
   * Get validations for root-level primitive types
   */
  private def getRootTypeValidations(schema: JsonSchemaDefinition): List[ValidationBuilder] = {
    // For primitive types at root, we'd validate against a default field name
    getPropertyValidations("root_value", JsonSchemaProperty(
      `type` = schema.`type`,
      format = schema.format,
      pattern = schema.pattern,
      enum = schema.enum,
      const = schema.const,
      minimum = schema.minimum,
      maximum = schema.maximum,
      minLength = schema.minLength,
      maxLength = schema.maxLength
    ))
  }

  /**
   * Get validations for individual properties
   */
  def getPropertyValidations(fieldName: String, property: JsonSchemaProperty): List[ValidationBuilder] = {
    val validations = scala.collection.mutable.ListBuffer[ValidationBuilder]()

    // Type validation
    property.`type`.foreach { dataType =>
      LOGGER.debug(s"Creating type validation for field '$fieldName': $dataType")
      val expectedSparkType = dataType match {
        case "string" => "string"
        case "integer" => "int"
        case "number" => "double"
        case "boolean" => "boolean"
        case "array" => "array"
        case "object" => "struct"
        case _ => dataType
      }
      validations += ValidationBuilder()
        .field(fieldName)
        .hasType(expectedSparkType)
        .description(s"Field '$fieldName' must be of type $dataType")
    }

    // String validations
    if (property.`type`.contains("string")) {
      // Pattern validation
      property.pattern.foreach { pattern =>
        LOGGER.debug(s"Creating pattern validation for field '$fieldName': $pattern")
        validations += ValidationBuilder()
          .field(fieldName)
          .matches(pattern)
          .description(s"Field '$fieldName' must match pattern: $pattern")
      }

      // Length validations
      (property.minLength, property.maxLength) match {
        case (Some(min), Some(max)) =>
          LOGGER.debug(s"Creating string length validation for field '$fieldName': $min <= length <= $max")
          validations += ValidationBuilder()
            .field(fieldName)
            .lengthBetween(min.toInt, max.toInt)
            .description(s"Field '$fieldName' length must be between $min and $max")
        case (Some(min), None) =>
          LOGGER.debug(s"Creating string minimum length validation for field '$fieldName': length >= $min")
          validations += ValidationBuilder()
            .field(fieldName)
            .expr(s"LENGTH(`$fieldName`) >= $min")
            .description(s"Field '$fieldName' length must be at least $min")
        case (None, Some(max)) =>
          LOGGER.debug(s"Creating string maximum length validation for field '$fieldName': length <= $max")
          validations += ValidationBuilder()
            .field(fieldName)
            .expr(s"LENGTH(`$fieldName`) <= $max")
            .description(s"Field '$fieldName' length must be at most $max")
        case _ => // No length constraints
      }

      // Format validations
      property.format.foreach { format =>
        LOGGER.debug(s"Creating format validation for field '$fieldName': $format")
        val formatValidation = format match {
          case "email" =>
            ValidationBuilder()
              .field(fieldName)
              .matches("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
              .description(s"Field '$fieldName' must be a valid email address")
          case "uri" | "url" =>
            ValidationBuilder()
              .field(fieldName)
              .matches("^https?://[^\\s/$.?#].[^\\s]*$")
              .description(s"Field '$fieldName' must be a valid URI")
          case "uuid" =>
            ValidationBuilder()
              .field(fieldName)
              .matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
              .description(s"Field '$fieldName' must be a valid UUID")
          case "date" =>
            ValidationBuilder()
              .field(fieldName)
              .matches("^\\d{4}-\\d{2}-\\d{2}$")
              .description(s"Field '$fieldName' must be a valid date (YYYY-MM-DD)")
          case "date-time" =>
            ValidationBuilder()
              .field(fieldName)
              .matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z?$")
              .description(s"Field '$fieldName' must be a valid date-time (ISO 8601)")
          case "ipv4" =>
            ValidationBuilder()
              .field(fieldName)
              .matches("^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
              .description(s"Field '$fieldName' must be a valid IPv4 address")
          case "ipv6" =>
            ValidationBuilder()
              .field(fieldName)
              .matches("^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$")
              .description(s"Field '$fieldName' must be a valid IPv6 address")
          case _ =>
            LOGGER.warn(s"Unsupported format validation: $format for field '$fieldName'")
            null
        }
        if (formatValidation != null) {
          validations += formatValidation
        }
      }
    }

    // Numeric validations
    if (property.`type`.exists(t => t == "integer" || t == "number")) {
      // Range validations
      (property.minimum, property.maximum) match {
        case (Some(min), Some(max)) =>
          LOGGER.debug(s"Creating numeric range validation for field '$fieldName': $min <= value <= $max")
          validations += ValidationBuilder()
            .field(fieldName)
            .between(min.toDouble, max.toDouble)
            .description(s"Field '$fieldName' must be between $min and $max")
        case (Some(min), None) =>
          LOGGER.debug(s"Creating numeric minimum validation for field '$fieldName': value >= $min")
          validations += ValidationBuilder()
            .field(fieldName)
            .greaterThan(min.toDouble, false)
            .description(s"Field '$fieldName' must be at least $min")
        case (None, Some(max)) =>
          LOGGER.debug(s"Creating numeric maximum validation for field '$fieldName': value <= $max")
          validations += ValidationBuilder()
            .field(fieldName)
            .lessThan(max.toDouble, false)
            .description(s"Field '$fieldName' must be at most $max")
        case _ => // No range constraints
      }
    }

    // Enum validation
    property.enum.foreach { enumValues =>
      LOGGER.debug(s"Creating enum validation for field '$fieldName': ${enumValues.mkString(",")}")
      validations += ValidationBuilder()
        .field(fieldName)
        .in(enumValues: _*)
        .description(s"Field '$fieldName' must be one of: ${enumValues.mkString(", ")}")
    }

    // Const validation
    property.const.foreach { constValue =>
      LOGGER.debug(s"Creating const validation for field '$fieldName': $constValue")
      validations += ValidationBuilder()
        .field(fieldName)
        .isEqual(constValue)
        .description(s"Field '$fieldName' must equal: $constValue")
    }

    validations.toList
  }
} 