package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.model.Constants.{HTTP, ONE_OF_GENERATOR, OPEN_API, PASSWORD, REGEX_GENERATOR, SCHEMA_LOCATION, USERNAME}
import io.github.datacatering.datacaterer.core.model.Constants.CONNECTION_TYPE
import io.github.datacatering.datacaterer.core.ui.model.{DataSourceRequest, FieldRequest, FieldRequests, MetadataSourceRequest}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FieldMapperTest extends AnyFunSuite {

  test("Can convert UI field mapping") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", fields = Some(FieldRequests(Some(List(
      FieldRequest("name", "string"),
      FieldRequest("account_id", "string", Some(Map(REGEX_GENERATOR -> "acc[0-9]{1}"))),
      FieldRequest("status", "string", Some(Map(ONE_OF_GENERATOR -> "open,closed"))),
      FieldRequest("details", "struct", nested = Some(FieldRequests(Some(List(FieldRequest("age", "integer")))))),
    )))))
    val optFieldMapping = FieldMapper.fieldMapping(dataSourceRequest)
    assert(optFieldMapping._2.isDefined)
    val res = optFieldMapping._2.get
    assertResult(4)(res.size)
    val nameField = res.find(_.field.name == "name")
    assert(nameField.exists(_.field.`type`.contains("string")))
    val accountField = res.find(_.field.name == "account_id")
    assert(accountField.exists(_.field.`type`.contains("string")))
    assert(accountField.exists(_.field.generator.exists(_.`type` == REGEX_GENERATOR)))
    assert(accountField.exists(_.field.generator.exists(_.options.get(REGEX_GENERATOR).contains("acc[0-9]{1}"))))
    val statusField = res.find(_.field.name == "status")
    assert(statusField.exists(_.field.generator.exists(_.`type` == ONE_OF_GENERATOR)))
    assert(statusField.exists(_.field.generator.exists(_.options.get(ONE_OF_GENERATOR).contains(List("open", "closed")))))
    val detailsField = res.find(_.field.name == "details")
    assertResult(Some("struct<>"))(detailsField.get.field.`type`)
    assert(detailsField.exists(_.field.schema.isDefined))
    assert(detailsField.exists(_.field.schema.get.fields.isDefined))
    assert(detailsField.exists(_.field.schema.get.fields.get.size == 1))
    assert(detailsField.exists(_.field.schema.get.fields.get.head.name == "age"))
    assert(detailsField.exists(_.field.schema.get.fields.get.head.`type`.contains("integer")))
  }

  test("Can convert UI connection mapping for HTTP with OpenAPI spec as a metadata source") {
    val dataSourceRequest = DataSourceRequest(
      "http-name",
      "task-1",
      Some(HTTP),
      Some(Map(USERNAME -> "root", PASSWORD -> "root")),
      Some(FieldRequests(optMetadataSource = Some(MetadataSourceRequest("my-openapi", Some(Map(CONNECTION_TYPE -> OPEN_API, SCHEMA_LOCATION -> "/opt/open-api-spec.json"))))))
    )
    val res = FieldMapper.fieldMapping(dataSourceRequest)
    assert(res._1.isDefined)
    val metadataSource = res._1.get.metadataSource
    assertResult(OPEN_API)(metadataSource.`type`)
    assertResult(1)(metadataSource.connectionOptions.size)
    assertResult("/opt/open-api-spec.json")(metadataSource.connectionOptions(SCHEMA_LOCATION))
  }
}
