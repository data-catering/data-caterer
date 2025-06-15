package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model.{JsonSchemaProperty, JsonSchemaReferenceResolutionException}
import org.apache.log4j.Logger

/**
 * Handles JSON Schema reference ($ref) resolution
 */
object JsonSchemaReferenceResolver {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Context for resolving JSON Schema references
   */
  case class ReferenceResolutionContext(
                                         definitions: Map[String, JsonSchemaProperty],
                                         schemaSource: String
                                       )

  /**
   * Resolve a JSON Schema reference
   */
  def resolveReference(refString: String, refContext: ReferenceResolutionContext): JsonSchemaProperty = {
    LOGGER.debug(s"Resolving reference: $refString")
    
    if (refString.startsWith("#/definitions/")) {
      val definitionName = refString.substring("#/definitions/".length)
      refContext.definitions.get(definitionName) match {
        case Some(definition) =>
          LOGGER.debug(s"Successfully resolved reference to definition: $definitionName")
          definition
        case None =>
          val availableDefinitions = refContext.definitions.keys.mkString(", ")
          val errorMsg = s"Definition '$definitionName' not found in schema. Available definitions: $availableDefinitions"
          LOGGER.error(errorMsg)
          throw JsonSchemaReferenceResolutionException(refString, new IllegalArgumentException(errorMsg))
      }
    } else if (refString.startsWith("#/")) {
      // Handle other JSON Pointer references (not implemented yet)
      val errorMsg = s"JSON Pointer references other than #/definitions/ are not yet supported: $refString"
      LOGGER.error(errorMsg)
      throw JsonSchemaReferenceResolutionException(refString, new UnsupportedOperationException(errorMsg))
    } else {
      // External references (not implemented yet)
      val errorMsg = s"External references are not yet supported: $refString"
      LOGGER.error(errorMsg)
      throw JsonSchemaReferenceResolutionException(refString, new UnsupportedOperationException(errorMsg))
    }
  }

  /**
   * Check if a reference string is supported
   */
  def isReferenceSupported(refString: String): Boolean = {
    refString.startsWith("#/definitions/")
  }

  /**
   * Extract definition name from reference string
   */
  def extractDefinitionName(refString: String): Option[String] = {
    if (refString.startsWith("#/definitions/")) {
      Some(refString.substring("#/definitions/".length))
    } else {
      None
    }
  }
} 