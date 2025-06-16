package io.github.datacatering.datacaterer.core.generator.metadata.datasource.http

import io.github.datacatering.datacaterer.api.model.Constants.{ENABLED_NULL, FIELD_DATA_TYPE, HTTP_HEADER_FIELD_PREFIX, HTTP_PARAMETER_TYPE, HTTP_PATH_PARAMETER, HTTP_PATH_PARAM_FIELD_PREFIX, HTTP_QUERY_PARAMETER, HTTP_QUERY_PARAM_FIELD_PREFIX, IS_NULLABLE, ONE_OF_GENERATOR, POST_SQL_EXPRESSION, REAL_TIME_BODY_CONTENT_FIELD, REAL_TIME_BODY_FIELD, REAL_TIME_CONTENT_TYPE_FIELD, REAL_TIME_METHOD_FIELD, REAL_TIME_URL_FIELD, SQL_GENERATOR, STATIC}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.swagger.v3.oas.models.PathItem.HttpMethod
import io.swagger.v3.oas.models.media.{Content, MediaType, Schema}
import io.swagger.v3.oas.models.parameters.Parameter.StyleEnum
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.servers.Server
import io.swagger.v3.oas.models.{Components, OpenAPI, Operation}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters.seqAsJavaListConverter

class OpenAPIConverterTest extends AnyFunSuite {

  test("Can convert GET request to field metadata") {
    val openAPI = new OpenAPI().addServersItem(new Server().url("http://localhost:80"))
    val operation = new Operation()
    val openAPIConverter = new OpenAPIConverter(openAPI)

    val result = openAPIConverter.toFieldMetadata("/", HttpMethod.GET, operation, Map())

    assertResult(2)(result.size)
    assertResult(Map(FIELD_DATA_TYPE -> "string", SQL_GENERATOR -> "CONCAT('http://localhost:80/', ARRAY_JOIN(ARRAY(), '&'))"))(result.filter(_.field == REAL_TIME_URL_FIELD).head.metadata)
    assertResult(Map(FIELD_DATA_TYPE -> "string", STATIC -> "GET"))(result.filter(_.field == REAL_TIME_METHOD_FIELD).head.metadata)
  }

  test("Can convert flat structure to field metadata") {
    val openAPIConverter = new OpenAPIConverter(null)
    val schema = new Schema[String]()
    val stringSchema = new Schema[String]()
    stringSchema.setType("string")
    val integerSchema = new Schema[Int]()
    integerSchema.setType("integer")
    schema.addProperty("name", stringSchema)
    schema.addProperty("age", integerSchema)

    val result = openAPIConverter.getFieldMetadata(schema)

    assertResult(2)(result.size)
    assertResult("string")(result.filter(_.field == "name").head.metadata(FIELD_DATA_TYPE))
    assertResult("integer")(result.filter(_.field == "age").head.metadata(FIELD_DATA_TYPE))
  }

  test("Can convert array to field metadata") {
    val openAPIConverter = new OpenAPIConverter()
    val schema = getInnerArrayStruct

    val result = openAPIConverter.getFieldMetadata(schema)

    assertResult(1)(result.size)
    assertResult("array<struct<name: string,tags: array<string>>>")(result.head.metadata(FIELD_DATA_TYPE))
    assertResult(1)(result.head.nestedFields.size)
    assertResult("struct<name: string,tags: array<string>>")(result.head.nestedFields.head.metadata(FIELD_DATA_TYPE))
  }

  test("Can convert component to field metadata") {
    val schema = getInnerArrayStruct
    val openAPI = new OpenAPI()
    openAPI.setComponents(new Components().addSchemas("NewPet", schema))
    val openAPIConverter = new OpenAPIConverter(openAPI)
    val petSchemaRef = new Schema[Object]()
    petSchemaRef.set$ref("#/components/schemas/NewPet")

    val result = openAPIConverter.getFieldMetadata(petSchemaRef)

    assertResult(1)(result.size)
    assertResult("array<struct<name: string,tags: array<string>>>")(result.head.metadata(FIELD_DATA_TYPE))
    assertResult(1)(result.head.nestedFields.size)
    assertResult("struct<name: string,tags: array<string>>")(result.head.nestedFields.head.metadata(FIELD_DATA_TYPE))
    assertResult(2)(result.head.nestedFields.head.nestedFields.size)
    assert(result.head.nestedFields.head.nestedFields.exists(_.field == "name"))
    assert(result.head.nestedFields.head.nestedFields.filter(_.field == "name").head.metadata(FIELD_DATA_TYPE) == "string")
    assert(result.head.nestedFields.head.nestedFields.exists(_.field == "tags"))
    assert(result.head.nestedFields.head.nestedFields.filter(_.field == "tags").head.metadata(FIELD_DATA_TYPE) == "array<string>")
  }

  test("Can convert parameters to headers") {
    val stringSchema = new Schema[String]()
    stringSchema.setType("string")
    stringSchema.setEnum(List("application/json", "application/xml").asJava)
    val integerSchema = new Schema[Int]()
    integerSchema.setType("integer")
    val contentTypeHeader = new Parameter()
    contentTypeHeader.setIn("header")
    contentTypeHeader.setName("Content-Type")
    contentTypeHeader.setSchema(stringSchema)
    contentTypeHeader.setRequired(true)
    val contentLengthHeader = new Parameter()
    contentLengthHeader.setIn("header")
    contentLengthHeader.setName("Content-Length")
    contentLengthHeader.setSchema(integerSchema)
    contentLengthHeader.setRequired(false)
    val params = List(new Parameter(), contentTypeHeader, contentLengthHeader)

    val result = new OpenAPIConverter().getHeaders(params)

    assertResult(2)(result.size)
    val contentTypeRes = result.find(_.field == s"${HTTP_HEADER_FIELD_PREFIX}Content_Type")
    val contentLengthRes = result.find(_.field == s"${HTTP_HEADER_FIELD_PREFIX}Content_Length")
    assert(contentTypeRes.isDefined)
    assert(contentLengthRes.isDefined)
    assert(contentTypeRes.get.nestedFields.isEmpty)
    assertResult("false")(contentTypeRes.get.metadata(IS_NULLABLE))
    assertResult("application/json,application/xml")(contentTypeRes.get.metadata(ONE_OF_GENERATOR))
    assertResult("Content-Type")(contentTypeRes.get.metadata(HTTP_HEADER_FIELD_PREFIX))
    assertResult("string")(contentTypeRes.get.metadata(FIELD_DATA_TYPE))
    assert(contentLengthRes.get.nestedFields.isEmpty)
    assertResult("true")(contentLengthRes.get.metadata(IS_NULLABLE))
    assertResult("true")(contentLengthRes.get.metadata(ENABLED_NULL))
    assertResult("Content-Length")(contentLengthRes.get.metadata(HTTP_HEADER_FIELD_PREFIX))
    assertResult("integer")(contentLengthRes.get.metadata(FIELD_DATA_TYPE))
  }

  test("Can convert URL to SQL generated URL from path and query params defined") {
    val baseUrl = "http://localhost:80/id/{id}/data"
    val pathParams = List(FieldMetadata("id"))
    val queryParams = List(FieldMetadata("limit"), FieldMetadata("tags", metadata = Map(POST_SQL_EXPRESSION -> "CONCAT('tags=', ARRAY_JOIN(tags, ','))")))

    val result = new OpenAPIConverter().urlSqlGenerator(baseUrl, pathParams, queryParams)

    assertResult("CONCAT(CONCAT(REPLACE('http://localhost:80/id/{id}/data', '{id}', URL_ENCODE(`id`)), '?'), ARRAY_JOIN(ARRAY(CAST(`limit` AS STRING),CAST(CONCAT('tags=', ARRAY_JOIN(tags, ',')) AS STRING)), '&'))")(result)
  }

  test("Can get path params from parameters list") {
    val stringSchema = new Schema[String]()
    stringSchema.setType("string")
    val idPathParam = new Parameter()
    idPathParam.setIn(HTTP_PATH_PARAMETER)
    idPathParam.setName("id")
    idPathParam.setSchema(stringSchema)
    idPathParam.setRequired(true)

    val result = new OpenAPIConverter().getPathParams(List(new Parameter(), idPathParam))

    assertResult(1)(result.size)
    assert(result.exists(_.field == s"${HTTP_PATH_PARAM_FIELD_PREFIX}id"))
    val resIdPath = result.find(_.field == s"${HTTP_PATH_PARAM_FIELD_PREFIX}id").get
    assertResult(4)(resIdPath.metadata.size)
    assertResult("false")(resIdPath.metadata(IS_NULLABLE))
    assertResult("false")(resIdPath.metadata(ENABLED_NULL))
    assertResult(HTTP_PATH_PARAMETER)(resIdPath.metadata(HTTP_PARAMETER_TYPE))
    assertResult("string")(resIdPath.metadata(FIELD_DATA_TYPE))
  }

  test("Can get query params from parameters list") {
    val stringSchema = new Schema[String]()
    stringSchema.setType("string")
    val stringParam = new Parameter()
    stringParam.setIn(HTTP_QUERY_PARAMETER)
    stringParam.setName("name")
    stringParam.setSchema(stringSchema)
    stringParam.setRequired(true)
    val baseArraySchema = new Schema[Array[String]]()
    baseArraySchema.setType("array")
    val baseQueryParam = new Parameter()
    baseQueryParam.setIn(HTTP_QUERY_PARAMETER)
    baseQueryParam.setName("tags")
    baseQueryParam.setSchema(baseArraySchema)
    baseQueryParam.setRequired(true)
    val formQueryParam = new Parameter()
    formQueryParam.setIn(HTTP_QUERY_PARAMETER)
    formQueryParam.setName("form")
    formQueryParam.setSchema(baseArraySchema)
    formQueryParam.setRequired(false)
    formQueryParam.setStyle(StyleEnum.FORM)
    formQueryParam.setExplode(false)
    val spaceQueryParam = new Parameter()
    spaceQueryParam.setIn(HTTP_QUERY_PARAMETER)
    spaceQueryParam.setName("space")
    spaceQueryParam.setSchema(baseArraySchema)
    spaceQueryParam.setRequired(false)
    spaceQueryParam.setStyle(StyleEnum.SPACEDELIMITED)
    spaceQueryParam.setExplode(false)
    val pipeQueryParam = new Parameter()
    pipeQueryParam.setIn(HTTP_QUERY_PARAMETER)
    pipeQueryParam.setName("pipe")
    pipeQueryParam.setSchema(baseArraySchema)
    pipeQueryParam.setRequired(false)
    pipeQueryParam.setStyle(StyleEnum.PIPEDELIMITED)
    pipeQueryParam.setExplode(false)

    val result = new OpenAPIConverter().getQueryParams(List(new Parameter(), stringParam, baseQueryParam, formQueryParam, spaceQueryParam, pipeQueryParam))
    def expectedSqlGenerator(name: String, delim: String): String = {
      s"CASE WHEN ARRAY_SIZE($HTTP_QUERY_PARAM_FIELD_PREFIX$name) > 0 THEN CONCAT('$name=', ARRAY_JOIN($HTTP_QUERY_PARAM_FIELD_PREFIX$name, '$delim')) ELSE null END"
    }
    def checkResult(name: String, delim: String): Unit = {
      val optQueryRes = result.find(_.field == s"$HTTP_QUERY_PARAM_FIELD_PREFIX$name")
      assert(optQueryRes.isDefined)
      assertResult(5)(optQueryRes.get.metadata.size)
      assertResult(expectedSqlGenerator(name, delim))(optQueryRes.get.metadata(POST_SQL_EXPRESSION))
      assertResult("true")(optQueryRes.get.metadata(IS_NULLABLE))
      assertResult("true")(optQueryRes.get.metadata(ENABLED_NULL))
      assertResult(HTTP_QUERY_PARAMETER)(optQueryRes.get.metadata(HTTP_PARAMETER_TYPE))
      assertResult("array<string>")(optQueryRes.get.metadata(FIELD_DATA_TYPE))
    }

    assertResult(5)(result.size)
    assert(result.exists(_.field == s"${HTTP_QUERY_PARAM_FIELD_PREFIX}name"))
    val resNameQuery = result.find(_.field == s"${HTTP_QUERY_PARAM_FIELD_PREFIX}name").get
    assertResult(5)(resNameQuery.metadata.size)
    assertResult(s"CONCAT('name=', ${HTTP_QUERY_PARAM_FIELD_PREFIX}name)")(resNameQuery.metadata(POST_SQL_EXPRESSION))
    assertResult("false")(resNameQuery.metadata(IS_NULLABLE))
    assertResult("false")(resNameQuery.metadata(ENABLED_NULL))
    assertResult(HTTP_QUERY_PARAMETER)(resNameQuery.metadata(HTTP_PARAMETER_TYPE))
    assertResult("string")(resNameQuery.metadata(FIELD_DATA_TYPE))

    assert(result.exists(_.field == s"${HTTP_QUERY_PARAM_FIELD_PREFIX}tags"))
    val resTagsQuery = result.find(_.field == s"${HTTP_QUERY_PARAM_FIELD_PREFIX}tags").get
    assertResult(5)(resTagsQuery.metadata.size)
    assertResult(expectedSqlGenerator("tags", s"&tags="))(resTagsQuery.metadata(POST_SQL_EXPRESSION))
    assertResult("false")(resTagsQuery.metadata(IS_NULLABLE))
    assertResult("false")(resTagsQuery.metadata(ENABLED_NULL))
    assertResult(HTTP_QUERY_PARAMETER)(resTagsQuery.metadata(HTTP_PARAMETER_TYPE))
    assertResult("array<string>")(resTagsQuery.metadata(FIELD_DATA_TYPE))
    checkResult("form", ",")
    checkResult("space", "%20")
    checkResult("pipe", "|")
  }

  test("Can convert operation into request body field metadata") {
    val schema = new Schema[String]()
    schema.setType("string")
    schema.setName("name")
    val mediaType = new MediaType()
    mediaType.setSchema(schema)
    val content = new Content()
    content.addMediaType("application/json", mediaType)
    val requestBody = new RequestBody()
    requestBody.setContent(content)
    val operation = new Operation()
    operation.setRequestBody(requestBody)

    val result = new OpenAPIConverter().getRequestBodyMetadata(operation)

    assertResult(4)(result.size)
    assert(result.exists(_.field == REAL_TIME_BODY_FIELD))
    assert(result.exists(_.field == REAL_TIME_BODY_CONTENT_FIELD))
    assert(result.exists(_.field == REAL_TIME_CONTENT_TYPE_FIELD))
    assert(result.exists(_.field == "name"))
    assertResult(s"TO_JSON($REAL_TIME_BODY_CONTENT_FIELD)")(result.find(_.field == REAL_TIME_BODY_FIELD).get.metadata(SQL_GENERATOR))
    assertResult("string")(result.find(_.field == REAL_TIME_BODY_CONTENT_FIELD).get.metadata(FIELD_DATA_TYPE))
    assertResult(1)(result.find(_.field == REAL_TIME_BODY_CONTENT_FIELD).get.nestedFields.size)
    assertResult("application/json")(result.find(_.field == REAL_TIME_CONTENT_TYPE_FIELD).get.metadata(STATIC))
  }

  test("Can convert operation into request body field metadata with url encoded body") {
    val schema = new Schema[String]()
    schema.setType("string")
    schema.setName("name")
    val mediaType = new MediaType()
    mediaType.setSchema(schema)
    val content = new Content()
    content.addMediaType("application/x-www-form-urlencoded", mediaType)
    val requestBody = new RequestBody()
    requestBody.setContent(content)
    val operation = new Operation()
    operation.setRequestBody(requestBody)

    val result = new OpenAPIConverter().getRequestBodyMetadata(operation)

    assertResult(4)(result.size)
    assert(result.exists(_.field == REAL_TIME_BODY_FIELD))
    assert(result.exists(_.field == REAL_TIME_BODY_CONTENT_FIELD))
    assert(result.exists(_.field == REAL_TIME_CONTENT_TYPE_FIELD))
    assert(result.exists(_.field == "name"))
    assertResult("ARRAY_JOIN(ARRAY(CONCAT('name=', CAST(`name` AS STRING))), '&')")(result.find(_.field == REAL_TIME_BODY_FIELD).get.metadata(SQL_GENERATOR))
    assertResult("application/x-www-form-urlencoded")(result.find(_.field == REAL_TIME_CONTENT_TYPE_FIELD).get.metadata(STATIC))
  }

  private def getInnerArrayStruct: Schema[_] = {
    val stringSchema = new Schema[String]()
    stringSchema.setType("string")
    val innerArray = new Schema[Array[String]]()
    innerArray.setType("array")
    innerArray.setItems(stringSchema)
    val innerSchema = new Schema[Object]()
      .addProperty("name", stringSchema)
      .addProperty("tags", innerArray)
    innerSchema.setType("object")
    val schema = new Schema[Array[Object]]()
    schema.setItems(innerSchema)
    schema.setType("array")
    schema
  }
}
