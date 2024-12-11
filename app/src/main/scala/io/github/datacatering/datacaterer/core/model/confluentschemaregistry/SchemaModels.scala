package io.github.datacatering.datacaterer.core.model.confluentschemaregistry

import com.fasterxml.jackson.annotation.JsonIgnoreProperties


@JsonIgnoreProperties(ignoreUnknown = true)
case class ConfluentSchemaRegistrySubjectVersionResponse(
                                                          subject: String,
                                                          version: Int,
                                                          id: Int,
                                                          schemaType: String,
                                                          schema: String,
                                                        )

@JsonIgnoreProperties(ignoreUnknown = true)
case class ConfluentSchemaRegistrySchemaResponse(
                                                  schemaType: String,
                                                  schema: String,
                                                )
