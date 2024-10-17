package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.ForeignKeyRelation

case class ForeignKeyRelationship(key: ForeignKeyRelation, foreignKey: ForeignKeyRelation)

case class ForeignKeyWithGenerateAndDelete(
                                            source: ForeignKeyRelation,
                                            generationLinks: List[ForeignKeyRelation],
                                            deleteLinks: List[ForeignKeyRelation]
                                          )

