package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model.{JsonSchemaConstraints, JsonSchemaDefinition, JsonSchemaProperty}
import org.apache.log4j.Logger

import scala.collection.mutable

/**
 * Converts JSON Schema constraints to Data Caterer field options
 */
object JsonSchemaConstraintsConverter {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Convert JSON Schema constraints to Data Caterer field options
   */
  def convertConstraintsToOptions(constraints: JsonSchemaConstraints): Map[String, Any] = {
    val options = mutable.Map[String, Any]()

    // String constraints
    constraints.pattern.foreach(pattern => options += REGEX_GENERATOR -> pattern)
    constraints.minLength.foreach(min => options += MINIMUM_LENGTH -> min)
    constraints.maxLength.foreach(max => options += MAXIMUM_LENGTH -> max)

    // Numeric constraints - parse string values to double
    constraints.minimum.foreach { minStr =>
      try {
        val doubleValue = minStr.toDouble
        options += MINIMUM -> doubleValue
      } catch {
        case _: NumberFormatException =>
          LOGGER.warn(s"Invalid minimum value '$minStr', skipping constraint")
      }
    }
    constraints.maximum.foreach { maxStr =>
      try {
        val doubleValue = maxStr.toDouble
        options += MAXIMUM -> doubleValue
      } catch {
        case _: NumberFormatException =>
          LOGGER.warn(s"Invalid maximum value '$maxStr', skipping constraint")
      }
    }
    // Note: JSON Schema exclusive min/max would need special handling

    // Value constraints
    constraints.`enum`.foreach { enumValues =>
      options += ONE_OF_GENERATOR -> enumValues.map(_.toString).mkString(",")
    }
    constraints.const.foreach { constValue =>
      options += ONE_OF_GENERATOR -> constValue.toString
    }

    // Default value
    constraints.`default`.foreach { defaultValue =>
      options += "default" -> defaultValue
    }

    // Format-based generators for common patterns
    constraints.format.foreach { format =>
      options ++= mapFormatToOptions(format)
    }

    options.toMap
  }

  /**
   * Convert array-specific constraints from JsonSchemaProperty
   */
  def convertArrayConstraints(property: JsonSchemaProperty): Map[String, Any] = {
    val options = mutable.Map[String, Any]()

    property.minItems.foreach(min => options += ARRAY_MINIMUM_LENGTH -> min)
    property.maxItems.foreach(max => options += ARRAY_MAXIMUM_LENGTH -> max)
    
    // Unique items constraint
    property.uniqueItems.foreach { unique =>
      if (unique) options += IS_UNIQUE -> true
    }

    options.toMap
  }

  /**
   * Convert array-specific constraints from JsonSchemaDefinition
   */
  def convertArrayConstraintsFromDefinition(schema: JsonSchemaDefinition): Map[String, Any] = {
    val options = mutable.Map[String, Any]()

    schema.minItems.foreach(min => options += ARRAY_MINIMUM_LENGTH -> min)
    schema.maxItems.foreach(max => options += ARRAY_MAXIMUM_LENGTH -> max)
    
    // Unique items constraint
    schema.uniqueItems.foreach { unique =>
      if (unique) options += IS_UNIQUE -> true
    }

    options.toMap
  }

  /**
   * Map JSON Schema format to Data Caterer options
   */
  def mapFormatToOptions(format: String): Map[String, Any] = {
    format.toLowerCase match {
      case "email" => Map(REGEX_GENERATOR -> "^[a-zA-Z0-9._%+-]{2,20}@[a-zA-Z0-9.-]{3,10}}\\.[a-zA-Z]{2,3}$")
      case "uri" | "url" => Map(REGEX_GENERATOR -> "^https?://[a-zA-Z0-9.-]{3,20}\\.[a-zA-Z]{2,3}(/[.]{3,20})?$")
      case "uuid" => Map("uuid" -> "")
      case "date" => Map("type" -> "date")
      case "date-time" => Map("type" -> "timestamp")
      case "time" => Map(REGEX_GENERATOR -> "^([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$")
      case "ipv4" => Map(REGEX_GENERATOR -> "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
      case "ipv6" => Map(REGEX_GENERATOR -> "^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$")
      case "hostname" => Map(REGEX_GENERATOR -> "^[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,15}$")
      case _ =>
        LOGGER.debug(s"No specific mapping for format '$format', using default constraints")
        Map()
    }
  }
} 