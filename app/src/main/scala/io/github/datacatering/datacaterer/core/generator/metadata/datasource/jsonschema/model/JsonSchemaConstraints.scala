package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model

import com.fasterxml.jackson.annotation.JsonIgnore

/**
 * JSON Schema validation constraints extracted from property
 */
case class JsonSchemaConstraints(
                                  pattern: Option[String] = None,
                                  format: Option[String] = None,
                                  minimum: Option[String] = None,
                                  maximum: Option[String] = None,
                                  exclusiveMinimum: Option[String] = None,
                                  exclusiveMaximum: Option[String] = None,
                                  multipleOf: Option[String] = None,
                                  minLength: Option[Int] = None,
                                  maxLength: Option[Int] = None,
                                  minItems: Option[Int] = None,
                                  maxItems: Option[Int] = None,
                                  uniqueItems: Option[Boolean] = None,
                                  minProperties: Option[Int] = None,
                                  maxProperties: Option[Int] = None,
                                  `enum`: Option[List[Any]] = None,
                                  const: Option[Any] = None,
                                  `default`: Option[Any] = None,
                                  examples: Option[List[Any]] = None,
                                  minContains: Option[Int] = None,
                                  maxContains: Option[Int] = None
                                ) {

  @JsonIgnore
  def hasStringConstraints: Boolean = pattern.isDefined || format.isDefined || minLength.isDefined || maxLength.isDefined

  @JsonIgnore
  def hasNumericConstraints: Boolean = minimum.isDefined || maximum.isDefined || exclusiveMinimum.isDefined || 
    exclusiveMaximum.isDefined || multipleOf.isDefined

  @JsonIgnore
  def hasArrayConstraints: Boolean = minItems.isDefined || maxItems.isDefined || uniqueItems.isDefined || 
    minContains.isDefined || maxContains.isDefined

  @JsonIgnore
  def hasObjectConstraints: Boolean = minProperties.isDefined || maxProperties.isDefined

  @JsonIgnore
  def hasValueConstraints: Boolean = `enum`.isDefined || const.isDefined

  @JsonIgnore
  def isEmpty: Boolean = !hasStringConstraints && !hasNumericConstraints && !hasArrayConstraints && 
    !hasObjectConstraints && !hasValueConstraints
} 