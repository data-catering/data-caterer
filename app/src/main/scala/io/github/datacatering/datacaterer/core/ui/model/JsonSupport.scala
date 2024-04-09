package io.github.datacatering.datacaterer.core.ui.model

import io.github.datacatering.datacaterer.core.ui.mapper.DateTimeFormat
import io.github.datacatering.datacaterer.core.ui.plan.PlanRepository.{ExecutionsById, GroupedPlanRunsByName, PlanRunExecutionDetails}
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.joda.time.DateTime
import spray.json.{DefaultJsonProtocol, JsFalse, JsNumber, JsString, JsTrue, JsValue, JsonFormat, RootJsonFormat}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any): JsValue = x match {
      case n: BigDecimal => JsNumber(n)
      case s: String => JsString(s)
      case b: Boolean if b => JsTrue
      case b: Boolean if !b => JsFalse
    }

    def read(value: JsValue): Any = value match {
      case JsNumber(n) => n
      case JsString(s) => s
      case JsTrue => true
      case JsFalse => false
    }
  }

  implicit val metadataSourceRequestFormat: RootJsonFormat[MetadataSourceRequest] = rootFormat(lazyFormat(jsonFormat2(MetadataSourceRequest.apply)))
  implicit val fieldRequestsFormat: RootJsonFormat[FieldRequests] = rootFormat(lazyFormat(jsonFormat2(FieldRequests.apply)))
  implicit val fieldRequestFormat: RootJsonFormat[FieldRequest] = rootFormat(lazyFormat(jsonFormat4(FieldRequest.apply)))
  implicit val recordCountRequestFormat: RootJsonFormat[RecordCountRequest] = jsonFormat9(RecordCountRequest.apply)
  implicit val waitRequestFormat: RootJsonFormat[WaitRequest] = jsonFormat1(WaitRequest.apply)
  implicit val validationItemRequestsFormat: RootJsonFormat[ValidationItemRequests] = rootFormat(lazyFormat(jsonFormat1(ValidationItemRequests.apply)))
  implicit val validationItemRequestFormat: RootJsonFormat[ValidationItemRequest] = rootFormat(lazyFormat(jsonFormat4(ValidationItemRequest.apply)))
  implicit val foreignKeyItemRequestFormat: RootJsonFormat[ForeignKeyRequestItem] = jsonFormat2(ForeignKeyRequestItem.apply)
  implicit val foreignKeyRequestFormat: RootJsonFormat[ForeignKeyRequest] = jsonFormat3(ForeignKeyRequest.apply)
  implicit val dataSourceRequestFormat: RootJsonFormat[DataSourceRequest] = jsonFormat7(DataSourceRequest.apply)
  implicit val configurationRequestFormat: RootJsonFormat[ConfigurationRequest] = jsonFormat6(ConfigurationRequest.apply)
  implicit val planRunRequestFormat: RootJsonFormat[PlanRunRequest] = jsonFormat5(PlanRunRequest.apply)
  implicit val planRunRequestsFormat: RootJsonFormat[PlanRunRequests] = jsonFormat1(PlanRunRequests.apply)
  implicit val dateTimeFormat: RootJsonFormat[DateTime] = DateTimeFormat
  implicit val planRunExecutionFormat: RootJsonFormat[PlanRunExecution] = jsonFormat12(PlanRunExecution.apply)
  implicit val executionsByIdFormat: RootJsonFormat[ExecutionsById] = jsonFormat2(ExecutionsById.apply)
  implicit val groupedPlanRunsByNameFormat: RootJsonFormat[GroupedPlanRunsByName] = jsonFormat2(GroupedPlanRunsByName.apply)
  implicit val planRunExecutionDetailsFormat: RootJsonFormat[PlanRunExecutionDetails] = jsonFormat1(PlanRunExecutionDetails.apply)
  implicit val connectionFormat: RootJsonFormat[Connection] = jsonFormat4(Connection.apply)
  implicit val saveConnectionsRequestFormat: RootJsonFormat[SaveConnectionsRequest] = jsonFormat1(SaveConnectionsRequest.apply)
  implicit val getConnectionsResponseFormat: RootJsonFormat[GetConnectionsResponse] = jsonFormat1(GetConnectionsResponse.apply)

}
