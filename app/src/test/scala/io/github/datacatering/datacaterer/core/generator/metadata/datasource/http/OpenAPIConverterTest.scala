package io.github.datacatering.datacaterer.core.generator.metadata.datasource.http

import io.github.datacatering.datacaterer.api.model.Constants.{ENABLED_NULL, FIELD_DATA_TYPE, HTTP_PARAMETER_TYPE, HTTP_PATH_PARAMETER, HTTP_QUERY_PARAMETER, IS_NULLABLE, ONE_OF_GENERATOR, POST_SQL_EXPRESSION, SQL_GENERATOR, STATIC}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.model.Constants.{HTTP_HEADER_COL_PREFIX, HTTP_PATH_PARAM_COL_PREFIX, HTTP_QUERY_PARAM_COL_PREFIX, REAL_TIME_BODY_COL, REAL_TIME_BODY_CONTENT_COL, REAL_TIME_CONTENT_TYPE_COL, REAL_TIME_METHOD_COL, REAL_TIME_URL_COL}
import io.swagger.v3.oas.models.PathItem.HttpMethod
import io.swagger.v3.oas.models.media.{Content, MediaType, Schema}
import io.swagger.v3.oas.models.parameters.Parameter.StyleEnum
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.servers.Server
import io.swagger.v3.oas.models.{Components, OpenAPI, Operation}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.collection.JavaConverters.seqAsJavaListConverter

@RunWith(classOf[JUnitRunner])
class OpenAPIConverterTest extends AnyFunSuite {

  test("Can convert GET request to column metadata") {
    val openAPI = new OpenAPI().addServersItem(new Server().url("http://localhost:80"))
    val operation = new Operation()
    val openAPIConverter = new OpenAPIConverter(openAPI)

    val result = openAPIConverter.toColumnMetadata("/", HttpMethod.GET, operation, Map())

    assert(result.size == 2)
    assert(result.filter(_.field == REAL_TIME_URL_COL).head.metadata == Map(FIELD_DATA_TYPE -> "string", SQL_GENERATOR -> "CONCAT('http://localhost:80/', URL_ENCODE(ARRAY_JOIN(ARRAY(), '&')))"))
    assert(result.filter(_.field == REAL_TIME_METHOD_COL).head.metadata == Map(FIELD_DATA_TYPE -> "string", STATIC -> "GET"))
  }

  test("Can convert flat structure to column metadata") {
    val openAPIConverter = new OpenAPIConverter(null)
    val schema = new Schema[String]()
    val stringSchema = new Schema[String]()
    stringSchema.setType("string")
    val integerSchema = new Schema[Int]()
    integerSchema.setType("integer")
    schema.addProperty("name", stringSchema)
    schema.addProperty("age", integerSchema)

    val result = openAPIConverter.getFieldMetadata(schema)

    assert(result.size == 2)
    assert(result.filter(_.field == "name").head.metadata(FIELD_DATA_TYPE) == "string")
    assert(result.filter(_.field == "age").head.metadata(FIELD_DATA_TYPE) == "integer")
  }

  test("Can convert array to column metadata") {
    val openAPIConverter = new OpenAPIConverter()
    val schema = getInnerArrayStruct

    val result = openAPIConverter.getFieldMetadata(schema)

    assert(result.size == 1)
    assert(result.head.metadata(FIELD_DATA_TYPE) == "array<struct<name: string,tags: array<string>>>")
    assert(result.head.nestedFields.size == 1)
    assert(result.head.nestedFields.head.metadata(FIELD_DATA_TYPE) == "struct<name: string,tags: array<string>>")
  }

  test("Can convert component to column metadata") {
    val schema = getInnerArrayStruct
    val openAPI = new OpenAPI()
    openAPI.setComponents(new Components().addSchemas("NewPet", schema))
    val openAPIConverter = new OpenAPIConverter(openAPI)
    val petSchemaRef = new Schema[Object]()
    petSchemaRef.set$ref("#/components/schemas/NewPet")

    val result = openAPIConverter.getFieldMetadata(petSchemaRef)

    assert(result.size == 1)
    assert(result.head.metadata(FIELD_DATA_TYPE) == "array<struct<name: string,tags: array<string>>>")
    assert(result.head.nestedFields.size == 1)
    assert(result.head.nestedFields.head.metadata(FIELD_DATA_TYPE) == "struct<name: string,tags: array<string>>")
    assert(result.head.nestedFields.head.nestedFields.size == 2)
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

    assert(result.size == 2)
    val contentTypeRes = result.find(_.field == s"${HTTP_HEADER_COL_PREFIX}Content_Type")
    val contentLengthRes = result.find(_.field == s"${HTTP_HEADER_COL_PREFIX}Content_Length")
    assert(contentTypeRes.isDefined)
    assert(contentLengthRes.isDefined)
    assert(contentTypeRes.get.nestedFields.isEmpty)
    assert(contentTypeRes.get.metadata(IS_NULLABLE) == "false")
    assert(contentTypeRes.get.metadata(ONE_OF_GENERATOR) == "application/json,application/xml")
    assert(contentTypeRes.get.metadata(HTTP_HEADER_COL_PREFIX) == "Content-Type")
    assert(contentTypeRes.get.metadata(FIELD_DATA_TYPE) == "string")
    assert(contentLengthRes.get.nestedFields.isEmpty)
    assert(contentLengthRes.get.metadata(IS_NULLABLE) == "true")
    assert(contentLengthRes.get.metadata(ENABLED_NULL) == "true")
    assert(contentLengthRes.get.metadata(HTTP_HEADER_COL_PREFIX) == "Content-Length")
    assert(contentLengthRes.get.metadata(FIELD_DATA_TYPE) == "integer")
  }

  test("Can convert URL to SQL generated URL from path and query params defined") {
    val baseUrl = "http://localhost:80/id/{id}/data"
    val pathParams = List(FieldMetadata("id"))
    val queryParams = List(FieldMetadata("limit"), FieldMetadata("tags", metadata = Map(POST_SQL_EXPRESSION -> "CONCAT('tags=', ARRAY_JOIN(tags, ','))")))

    val result = new OpenAPIConverter().urlSqlGenerator(baseUrl, pathParams, queryParams)

    assert(result == "CONCAT(CONCAT(REPLACE('http://localhost:80/id/{id}/data', '{id}', URL_ENCODE(`id`)), '?'), URL_ENCODE(ARRAY_JOIN(ARRAY(CAST(`limit`` AS STRING),CAST(CONCAT('tags=', ARRAY_JOIN(tags, ',')) AS STRING)), '&')))")
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

    assert(result.size == 1)
    assert(result.exists(_.field == s"${HTTP_PATH_PARAM_COL_PREFIX}id"))
    val resIdPath = result.find(_.field == s"${HTTP_PATH_PARAM_COL_PREFIX}id").get
    assert(resIdPath.metadata.size == 4)
    assert(resIdPath.metadata(IS_NULLABLE) == "false")
    assert(resIdPath.metadata(ENABLED_NULL) == "false")
    assert(resIdPath.metadata(HTTP_PARAMETER_TYPE) == HTTP_PATH_PARAMETER)
    assert(resIdPath.metadata(FIELD_DATA_TYPE) == "string")
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
      s"CASE WHEN ARRAY_SIZE($HTTP_QUERY_PARAM_COL_PREFIX$name) > 0 THEN CONCAT('$name=', ARRAY_JOIN($HTTP_QUERY_PARAM_COL_PREFIX$name, '$delim')) ELSE null END"
    }
    def checkResult(name: String, delim: String): Unit = {
      val optQueryRes = result.find(_.field == s"$HTTP_QUERY_PARAM_COL_PREFIX$name")
      assert(optQueryRes.isDefined)
      assert(optQueryRes.get.metadata.size == 5)
      assert(optQueryRes.get.metadata(POST_SQL_EXPRESSION) == expectedSqlGenerator(name, delim))
      assert(optQueryRes.get.metadata(IS_NULLABLE) == "true")
      assert(optQueryRes.get.metadata(ENABLED_NULL) == "true")
      assert(optQueryRes.get.metadata(HTTP_PARAMETER_TYPE) == HTTP_QUERY_PARAMETER)
      assert(optQueryRes.get.metadata(FIELD_DATA_TYPE) == "array<string>")
    }

    assert(result.size == 5)
    assert(result.exists(_.field == s"${HTTP_QUERY_PARAM_COL_PREFIX}name"))
    val resNameQuery = result.find(_.field == s"${HTTP_QUERY_PARAM_COL_PREFIX}name").get
    assert(resNameQuery.metadata.size == 5)
    assert(resNameQuery.metadata(POST_SQL_EXPRESSION) == s"CONCAT('name=', ${HTTP_QUERY_PARAM_COL_PREFIX}name)")
    assert(resNameQuery.metadata(IS_NULLABLE) == "false")
    assert(resNameQuery.metadata(ENABLED_NULL) == "false")
    assert(resNameQuery.metadata(HTTP_PARAMETER_TYPE) == HTTP_QUERY_PARAMETER)
    assert(resNameQuery.metadata(FIELD_DATA_TYPE) == "string")

    assert(result.exists(_.field == s"${HTTP_QUERY_PARAM_COL_PREFIX}tags"))
    val resTagsQuery = result.find(_.field == s"${HTTP_QUERY_PARAM_COL_PREFIX}tags").get
    assert(resTagsQuery.metadata.size == 5)
    assert(resTagsQuery.metadata(POST_SQL_EXPRESSION) == expectedSqlGenerator("tags", s"&tags="))
    assert(resTagsQuery.metadata(IS_NULLABLE) == "false")
    assert(resTagsQuery.metadata(ENABLED_NULL) == "false")
    assert(resTagsQuery.metadata(HTTP_PARAMETER_TYPE) == HTTP_QUERY_PARAMETER)
    assert(resTagsQuery.metadata(FIELD_DATA_TYPE) == "array<string>")
    checkResult("form", ",")
    checkResult("space", "%20")
    checkResult("pipe", "|")
  }

  test("Can convert operation into request body column metadata") {
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

    assert(result.size == 4)
    assert(result.exists(_.field == REAL_TIME_BODY_COL))
    assert(result.exists(_.field == REAL_TIME_BODY_CONTENT_COL))
    assert(result.exists(_.field == REAL_TIME_CONTENT_TYPE_COL))
    assert(result.exists(_.field == "name"))
    assert(result.find(_.field == REAL_TIME_BODY_COL).get.metadata(SQL_GENERATOR) == s"TO_JSON($REAL_TIME_BODY_CONTENT_COL)")
    assert(result.find(_.field == REAL_TIME_BODY_CONTENT_COL).get.metadata(FIELD_DATA_TYPE) == s"string")
    assert(result.find(_.field == REAL_TIME_BODY_CONTENT_COL).get.nestedFields.size == 1)
    assert(result.find(_.field == REAL_TIME_CONTENT_TYPE_COL).get.metadata(STATIC) == "application/json")
  }

  test("Can convert operation into request body column metadata with url encoded body") {
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

    assert(result.size == 4)
    assert(result.exists(_.field == REAL_TIME_BODY_COL))
    assert(result.exists(_.field == REAL_TIME_BODY_CONTENT_COL))
    assert(result.exists(_.field == REAL_TIME_CONTENT_TYPE_COL))
    assert(result.exists(_.field == "name"))
    assert(result.find(_.field == REAL_TIME_BODY_COL).get.metadata(SQL_GENERATOR) == s"ARRAY_JOIN(ARRAY(CONCAT('name=', CAST(`name` AS STRING))), '&')")
    assert(result.find(_.field == REAL_TIME_CONTENT_TYPE_COL).get.metadata(STATIC) == "application/x-www-form-urlencoded")
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
