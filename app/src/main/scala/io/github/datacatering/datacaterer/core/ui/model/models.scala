package io.github.datacatering.datacaterer.core.ui.model

import io.github.datacatering.datacaterer.api.model.Constants.{PLAN_RUN_EXECUTION_DELIMITER, PLAN_RUN_EXECUTION_DELIMITER_REGEX}
import io.github.datacatering.datacaterer.core.model.Constants.{TIMESTAMP_DATE_TIME_FORMATTER, TIMESTAMP_FORMAT}
import org.joda.time.DateTime

case class PlanRunRequests(plans: List[PlanRunRequest])

case class PlanRunRequest(name: String, id: String, dataSources: List[DataSourceRequest])

case class DataSourceRequest(
                              name: String,
                              `type`: Option[String] = None,
                              options: Option[Map[String, String]] = None,
                              fields: Option[List[FieldRequest]] = None,
                              count: Option[RecordCountRequest] = None,
                              validations: Option[ValidationRequest] = None,
                            )

case class FieldRequest(name: String, `type`: String, options: Option[Map[String, String]] = None)

case class RecordCountRequest(
                               records: Option[Long] = None,
                               recordsMin: Option[Long] = None,
                               recordsMax: Option[Long] = None,
                               perColumnNames: Option[List[String]] = None,
                               perColumnRecords: Option[Long] = None,
                               perColumnRecordsMin: Option[Long] = None,
                               perColumnRecordsMax: Option[Long] = None,
                             )

case class ValidationRequest(
                              validations: List[ValidationItemRequest] = List(),
                              waitRequest: Option[WaitRequest] = None
                            )

case class ValidationItemRequest(`type`: String, options: Option[Map[String, String]] = None)

case class WaitRequest(`type`: String)


case class PlanRunExecution(
                             name: String,
                             id: String,
                             status: String,
                             failedReason: Option[String] = None,
                             runBy: String = "admin",
                             updatedBy: String = "admin",
                             createdTs: DateTime = DateTime.now(),
                             updatedTs: DateTime = DateTime.now()
                           ) {
  override def toString: String = {
    List(name, id, status, failedReason.getOrElse(""), runBy, updatedBy, createdTs.toString(TIMESTAMP_FORMAT), updatedTs.toString(TIMESTAMP_FORMAT))
      .mkString(PLAN_RUN_EXECUTION_DELIMITER) + "\n"
  }
}

object PlanRunExecution {
  def fromString(str: String): PlanRunExecution = {
    val spt = str.split(PLAN_RUN_EXECUTION_DELIMITER_REGEX)
    assert(spt.length == 8, s"Unexpected number of columns saved for plan execution, $str")
    PlanRunExecution(spt.head, spt(1), spt(2), Some(spt(3)), spt(4), spt(5),
      DateTime.parse(spt(6), TIMESTAMP_DATE_TIME_FORMATTER), DateTime.parse(spt(7), TIMESTAMP_DATE_TIME_FORMATTER))
  }
}

case class GetConnectionsResponse(connections: List[Connection])

case class SaveConnectionsRequest(connections: List[Connection])

case class Connection(name: String, `type`: String, options: Map[String, String]) {
  override def toString: String = {
    (List(name, `type`) ++ options.map(x => s"${x._1}:${x._2}").toList).mkString(PLAN_RUN_EXECUTION_DELIMITER)
  }
}

object Connection {
  def fromString(str: String): Connection = {
    val spt = str.split(PLAN_RUN_EXECUTION_DELIMITER_REGEX)
    val options = spt.slice(2, spt.length).map(o => {
      val optSpt = o.split(":", 2)
      (optSpt.head, optSpt.last)
    }).toMap
    if (spt.length > 1) {
      Connection(spt.head, spt(1), options)
    } else {
      throw new RuntimeException("File content does not contain connection details")
    }
  }
}
