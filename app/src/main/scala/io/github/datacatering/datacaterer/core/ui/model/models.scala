package io.github.datacatering.datacaterer.core.ui.model

import io.github.datacatering.datacaterer.api.model.Constants.{PLAN_RUN_EXECUTION_DELIMITER, PLAN_RUN_EXECUTION_DELIMITER_REGEX, PLAN_RUN_SUMMARY_DELIMITER}
import io.github.datacatering.datacaterer.core.model.Constants.{CONNECTION_GROUP_DATA_SOURCE, CONNECTION_GROUP_TYPE_MAP, TIMESTAMP_DATE_TIME_FORMATTER, TIMESTAMP_FORMAT}
import org.joda.time.DateTime

case class PlanRunRequests(plans: List[PlanRunRequest])

case class PlanRunRequest(
                           name: String,
                           id: String,
                           dataSources: List[DataSourceRequest],
                           foreignKeys: List[ForeignKeyRequest] = List(),
                           configuration: Option[ConfigurationRequest] = None,
                         )

case class DataSourceRequest(
                              name: String,
                              taskName: String,
                              `type`: Option[String] = None,
                              options: Option[Map[String, String]] = None,
                              fields: Option[FieldRequests] = None,
                              count: Option[RecordCountRequest] = None,
                              validations: Option[ValidationRequest] = None,
                            )

case class FieldRequest(
                         name: String,
                         `type`: String,
                         options: Option[Map[String, String]] = None,
                         nested: Option[FieldRequests] = None
                       )

case class FieldRequests(
                          optFields: Option[List[FieldRequest]] = None,
                          optMetadataSource: Option[MetadataSourceRequest] = None
                        )

case class MetadataSourceRequest(name: String, overrideOptions: Option[Map[String, String]] = None)

case class RecordCountRequest(
                               records: Option[Long] = None,
                               recordsMin: Option[Long] = None,
                               recordsMax: Option[Long] = None,
                               perColumnNames: Option[List[String]] = None,
                               perColumnRecords: Option[Long] = None,
                               perColumnRecordsMin: Option[Long] = None,
                               perColumnRecordsMax: Option[Long] = None,
                               perColumnRecordsDistribution: Option[String] = None,
                               perColumnRecordsDistributionRateParam: Option[String] = None,
                             )

case class ValidationRequest(
                              optValidations: Option[List[ValidationItemRequest]] = None,
                              optMetadataSource: Option[MetadataSourceRequest] = None
                            )

case class ValidationItemRequest(
                                  `type`: String,
                                  options: Option[Map[String, String]] = None,
                                  nested: Option[ValidationItemRequests] = None,
                                  waitRequest: Option[WaitRequest] = None
                                )

case class ValidationItemRequests(validations: List[ValidationItemRequest])

case class WaitRequest(`type`: String)

case class ForeignKeyRequest(
                              source: Option[ForeignKeyRequestItem] = None,
                              generationLinks: List[ForeignKeyRequestItem] = List(),
                              deleteLinks: List[ForeignKeyRequestItem] = List(),
                            )

case class ForeignKeyRequestItem(taskName: String, columns: String, options: Option[Map[String, String]] = None)

case class ConfigurationRequest(
                                 flag: Map[String, String] = Map(),
                                 folder: Map[String, String] = Map(),
                                 metadata: Map[String, String] = Map(),
                                 generation: Map[String, String] = Map(),
                                 validation: Map[String, String] = Map(),
                                 alert: Map[String, String] = Map(),
                               )


case class PlanRunExecution(
                             name: String,
                             id: String,
                             status: String,
                             failedReason: Option[String] = None,
                             runBy: String = "admin",
                             updatedBy: String = "admin",
                             createdTs: DateTime = DateTime.now(),
                             updatedTs: DateTime = DateTime.now(),
                             generationSummary: List[List[String]] = List(),
                             validationSummary: List[List[String]] = List(),
                             reportLink: Option[String] = None,
                             timeTaken: Option[String] = None,
                           ) {
  override def toString: String = {
    List(name, id, status, failedReason.getOrElse(""), runBy, updatedBy,
      createdTs.toString(TIMESTAMP_FORMAT), updatedTs.toString(TIMESTAMP_FORMAT),
      generationSummary.map(_.mkString(",")).mkString(PLAN_RUN_SUMMARY_DELIMITER),
      validationSummary.map(_.mkString(",")).mkString(PLAN_RUN_SUMMARY_DELIMITER),
      reportLink.getOrElse(""), timeTaken.getOrElse("0")
    )
      .mkString(PLAN_RUN_EXECUTION_DELIMITER) + "\n"
  }
}

object PlanRunExecution {
  def fromString(str: String): PlanRunExecution = {
    val spt = str.split(PLAN_RUN_EXECUTION_DELIMITER_REGEX)
    assert(spt.length == 12, s"Unexpected number of columns saved for plan execution, $str")
    PlanRunExecution(spt.head, spt(1), spt(2), Some(spt(3)), spt(4), spt(5),
      DateTime.parse(spt(6), TIMESTAMP_DATE_TIME_FORMATTER), DateTime.parse(spt(7), TIMESTAMP_DATE_TIME_FORMATTER),
      spt(8).split(PLAN_RUN_SUMMARY_DELIMITER).map(_.split(",").toList).toList,
      spt(9).split(PLAN_RUN_SUMMARY_DELIMITER).map(_.split(",").toList).toList, Some(spt(10)), Some(spt(11))
    )
  }
}

case class GetConnectionsResponse(connections: List[Connection])

case class SaveConnectionsRequest(connections: List[Connection])

case class Connection(name: String, `type`: String, groupType: Option[String], options: Map[String, String]) {
  override def toString: String = {
    val parsedGroupType = groupType.getOrElse(CONNECTION_GROUP_TYPE_MAP.getOrElse(`type`, CONNECTION_GROUP_DATA_SOURCE))
    (List(name, `type`, parsedGroupType) ++ options.map(x => s"${x._1}:${x._2}").toList).mkString(PLAN_RUN_EXECUTION_DELIMITER)
  }
}

object Connection {
  def fromString(str: String, masking: Boolean = true): Connection = {
    val spt = str.split(PLAN_RUN_EXECUTION_DELIMITER_REGEX)
    if (spt.length > 2) {
      val optionSplitIndex = if (spt(2).contains(":")) 2 else 3
      val options = spt.slice(optionSplitIndex, spt.length).map(o => {
        val optSpt = o.split(":", 2)
        if (masking && (optSpt.head.contains("password") || optSpt.head.contains("token"))) {
          (optSpt.head, "***")
        } else (optSpt.head, optSpt.last)
      }).toMap
      val groupType = if (optionSplitIndex == 2) {
        CONNECTION_GROUP_TYPE_MAP.getOrElse(spt(1), CONNECTION_GROUP_DATA_SOURCE)
      } else spt(2)
      Connection(spt.head, spt(1), Some(groupType), options)
    } else {
      throw new RuntimeException("File content does not contain connection details")
    }
  }
}
