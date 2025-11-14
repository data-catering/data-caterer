package io.github.datacatering.datacaterer.api.model

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

/**
 * Custom Jackson deserializer for Option[Either[String, Connection]].
 * Handles both:
 * - String: connection: "db_name"
 * - Object: connection: {type: "postgres", ...}
 * - Null/missing: connection not specified
 */
class ConnectionDeserializer extends JsonDeserializer[Option[Either[String, Connection]]] {

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Option[Either[String, Connection]] = {
    p.getCurrentToken match {
      case JsonToken.VALUE_STRING =>
        // Simple string reference: connection: "db_name"
        Some(Left(p.getValueAsString))

      case JsonToken.START_OBJECT =>
        // Inline connection object: connection: {type: "postgres", ...}
        val connection = p.readValueAs(classOf[Connection])
        Some(Right(connection))

      case JsonToken.VALUE_NULL =>
        // Null value
        None

      case _ =>
        throw ctxt.wrongTokenException(p, classOf[Option[Either[String, Connection]]], p.getCurrentToken,
          "Expected string (connection name), object (inline connection), or null")
    }
  }

  override def getNullValue(ctxt: DeserializationContext): Option[Either[String, Connection]] = None
}
