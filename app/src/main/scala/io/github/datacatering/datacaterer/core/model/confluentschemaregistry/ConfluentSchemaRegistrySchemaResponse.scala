package io.github.datacatering.datacaterer.core.model.confluentschemaregistry

import com.fasterxml.jackson.annotation.JsonIgnoreProperties


@JsonIgnoreProperties(ignoreUnknown = true)
case class ConfluentSchemaRegistrySchemaResponse(
                                                  schemaType: String,
                                                  schema: String,
                                                )
