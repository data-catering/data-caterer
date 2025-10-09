package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard.model.{DataQualityTypeEnum, OpenDataContractStandardDataQualityV3, OpenDataContractStandardElementV3, OpenDataContractStandardSchemaV3}
import org.apache.log4j.Logger

object OpenDataContractStandardDataValidations {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Convert ODCS data quality rules to Data Caterer validations
   * @param schema The ODCS schema containing quality rules
   * @return List of validation builders
   */
  def getDataValidations(schema: OpenDataContractStandardSchemaV3): List[ValidationBuilder] = {
    val schemaLevelValidations = schema.quality.map(_.flatMap(convertQualityCheck(_, None)).toList).getOrElse(List())
    val propertyLevelValidations = schema.properties.map(_.flatMap(property => {
      property.quality.map(_.flatMap(convertQualityCheck(_, Some(property))).toList).getOrElse(List())
    }).toList).getOrElse(List())

    val allValidations = schemaLevelValidations ++ propertyLevelValidations
    if (allValidations.nonEmpty) {
      LOGGER.info(s"Converted ODCS quality checks to validations, schema=${schema.name}, num-validations=${allValidations.size}")
    }
    allValidations
  }

  private def convertQualityCheck(
                                    quality: OpenDataContractStandardDataQualityV3,
                                    optProperty: Option[OpenDataContractStandardElementV3]
                                  ): Option[ValidationBuilder] = {
    quality.`type` match {
      case DataQualityTypeEnum.library => convertLibraryQualityCheck(quality, optProperty)
      case DataQualityTypeEnum.sql => convertSqlQualityCheck(quality, optProperty)
      case DataQualityTypeEnum.text => None // Text is just documentation
      case DataQualityTypeEnum.custom => convertCustomQualityCheck(quality, optProperty)
    }
  }

  private def convertLibraryQualityCheck(
                                          quality: OpenDataContractStandardDataQualityV3,
                                          optProperty: Option[OpenDataContractStandardElementV3]
                                        ): Option[ValidationBuilder] = {
    // Get the field name from quality.field or optProperty
    val fieldName = quality.field.orElse(optProperty.map(_.name))

    quality.rule.map(_.toLowerCase) match {
      case Some("nullcheck") | Some("null_check") =>
        fieldName.map(field => {
          val validation = ValidationBuilder().field(field).isNull(negate = true)
          addDescriptionAndThreshold(validation, quality)
        })

      case Some("uniquecheck") | Some("unique_check") =>
        fieldName.map(field => {
          val validation = ValidationBuilder().unique(field)
          addDescriptionAndThreshold(validation, quality)
        })

      case Some("countcheck") | Some("count_check") =>
        // Row count validation using groupBy and count
        val expr = if (quality.mustBeBetween.isDefined && quality.mustBeBetween.get.length == 2) {
          val range = quality.mustBeBetween.get
          s"count >= ${range(0)} AND count <= ${range(1)}"
        } else if (quality.mustBe.isDefined) {
          s"count == ${quality.mustBe.get}"
        } else if (quality.mustBeGreaterThan.isDefined) {
          s"count > ${quality.mustBeGreaterThan.get}"
        } else if (quality.mustBeGreaterOrEqualTo.isDefined) {
          s"count >= ${quality.mustBeGreaterOrEqualTo.get}"
        } else if (quality.mustBeLessThan.isDefined) {
          s"count < ${quality.mustBeLessThan.get}"
        } else if (quality.mustBeLessOrEqualTo.isDefined) {
          s"count <= ${quality.mustBeLessOrEqualTo.get}"
        } else {
          "count >= 0"
        }
        val validation = ValidationBuilder().groupBy().count().expr(expr)
        Some(addDescriptionAndThreshold(validation, quality))

      case Some(rule) if rule.contains("between") =>
        // Generic between check
        quality.mustBeBetween.flatMap(range => {
          if (range.length == 2) {
            fieldName.map(field => {
              val validation = ValidationBuilder().field(field).between(range(0), range(1))
              addDescriptionAndThreshold(validation, quality)
            })
          } else None
        })

      case Some(rule) if rule.contains("greaterthan") || rule.contains("greater_than") =>
        quality.mustBeGreaterThan.flatMap(value => {
          fieldName.map(field => {
            val validation = ValidationBuilder().field(field).greaterThan(value)
            addDescriptionAndThreshold(validation, quality)
          })
        })

      case Some(rule) if rule.contains("lessthan") || rule.contains("less_than") =>
        quality.mustBeLessThan.flatMap(value => {
          fieldName.map(field => {
            val validation = ValidationBuilder().field(field).lessThan(value)
            addDescriptionAndThreshold(validation, quality)
          })
        })

      case Some(rule) if rule.contains("inset") || rule.contains("in_set") =>
        quality.mustBe match {
          case Some(values: Iterable[_]) =>
            fieldName.map(field => {
              val validation = ValidationBuilder().field(field).in(values.map(_.toString).toSeq: _*)
              addDescriptionAndThreshold(validation, quality)
            })
          case _ => None
        }

      case Some(rule) if rule.contains("matchespattern") || rule.contains("matches_pattern") =>
        quality.mustBe.flatMap(pattern => {
          fieldName.map(field => {
            val validation = ValidationBuilder().field(field).matches(pattern.toString)
            addDescriptionAndThreshold(validation, quality)
          })
        })

      case _ =>
        LOGGER.warn(s"Unknown ODCS library quality check rule: ${quality.rule.getOrElse("none")}, dimension=${quality.dimension.getOrElse("none")}")
        None
    }
  }

  private def convertSqlQualityCheck(
                                      quality: OpenDataContractStandardDataQualityV3,
                                      optProperty: Option[OpenDataContractStandardElementV3]
                                    ): Option[ValidationBuilder] = {
    quality.query.map(sql => {
      val validation = ValidationBuilder().expr(sql)
      addDescriptionAndThreshold(validation, quality)
    })
  }

  private def convertCustomQualityCheck(
                                         quality: OpenDataContractStandardDataQualityV3,
                                         optProperty: Option[OpenDataContractStandardElementV3]
                                       ): Option[ValidationBuilder] = {
    // For custom checks, we can try to use the implementation or code fields as SQL expressions
    quality.implementation
      .orElse(quality.code)
      .map(expr => {
        val validation = ValidationBuilder().expr(expr)
        addDescriptionAndThreshold(validation, quality)
      })
  }

  private def addDescriptionAndThreshold(
                                          validation: ValidationBuilder,
                                          quality: OpenDataContractStandardDataQualityV3
                                        ): ValidationBuilder = {
    var result = validation

    // Add description
    quality.description.orElse(quality.name).foreach(desc => {
      result = result.description(desc)
    })

    // Add error threshold based on severity
    // For "error" severity, we want strict validation (no threshold)
    // For "warning", we can be more lenient
    quality.severity match {
      case Some("warning") | Some("info") =>
        // Allow up to 5% failures for warnings
        result = result.errorThreshold(0.05)
      case _ =>
      // For error or no severity, keep strict (default behavior)
    }

    result
  }
}
