package io.github.datacatering.datacaterer.core.model.openlineage

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

import java.sql.Timestamp
import java.util.UUID

//have to create own OpenLineageModels

case class ListDatasetResponse(datasets: List[OpenLineageDataset], totalCount: Int)

case class DatasetId(namespace: String, name: String)

@JsonIgnoreProperties(ignoreUnknown = true)
case class OpenLineageDataset(
                               id: DatasetId,
                               `type`: String,
                               name: String,
                               physicalName: String,
                               createdAt: Timestamp,
                               updatedAt: Timestamp,
                               namespace: String,
                               sourceName: String,
                               fields: List[DatasetField],
                               tags: Set[String],
                               lastModifiedAt: Timestamp,
                               description: Option[String],
                               columnLineage: List[Any],
                               facets: Map[String, Any],
                               currentVersion: UUID
                             )

case class DatasetField(name: String, `type`: Option[String], tags: Set[String], description: Option[String])
