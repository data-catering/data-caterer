package io.github.datacatering.datacaterer.core.generator.metadata.datasource.http

import io.github.datacatering.datacaterer.api.model.Constants.{ARRAY_MAXIMUM_LENGTH, ARRAY_MINIMUM_LENGTH, ENABLED_NULL, FIELD_DATA_TYPE, HTTP_HEADER_PARAMETER, HTTP_PARAMETER_TYPE, HTTP_PATH_PARAMETER, HTTP_QUERY_PARAMETER, IS_NULLABLE, MAXIMUM, MAXIMUM_LENGTH, MINIMUM, MINIMUM_LENGTH, ONE_OF_GENERATOR, ONE_OF_GENERATOR_DELIMITER, POST_SQL_EXPRESSION, REGEX_GENERATOR, SQL_GENERATOR, STATIC}
import io.github.datacatering.datacaterer.api.model.{ArrayType, BinaryType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType, TimestampType}
import io.github.datacatering.datacaterer.core.exception.UnsupportedOpenApiDataTypeException
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.model.Constants.{HTTP_HEADER_FIELD_PREFIX, HTTP_PATH_PARAM_FIELD_PREFIX, HTTP_QUERY_PARAM_FIELD_PREFIX, REAL_TIME_BODY_FIELD, REAL_TIME_BODY_CONTENT_FIELD, REAL_TIME_CONTENT_TYPE_FIELD, REAL_TIME_METHOD_FIELD, REAL_TIME_URL_FIELD}
import io.swagger.v3.oas.models.PathItem.HttpMethod
import io.swagger.v3.oas.models.media.Schema
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.parameters.Parameter.StyleEnum
import io.swagger.v3.oas.models.{OpenAPI, Operation}
import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaSetConverter, mapAsScalaMapConverter}
import scala.util.{Failure, Success, Try}

class OpenAPIConverter(openAPI: OpenAPI = new OpenAPI()) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def toFieldMetadata(path: String, method: HttpMethod, operation: Operation, readOptions: Map[String, String]): List[FieldMetadata] = {
    val requestBodyMetadata = getRequestBodyMetadata(operation)
    val params = if (operation.getParameters != null) operation.getParameters.asScala.toList else List()
    val headers = getHeaders(params)
    val pathParams = getPathParams(params)
    val queryParams = getQueryParams(params)
    val baseUrl = openAPI.getServers.get(0).getUrl
    val variableReplacedUrl = if (openAPI.getServers.get(0).getVariables != null) {
      val variables = openAPI.getServers.get(0).getVariables.asScala.map(v => (v._1, v._2.getDefault))
      variables.foldLeft(baseUrl)((url, v) => url.replace(s"{${v._1}}", v._2))
    } else baseUrl
    val urlCol = getUrl(variableReplacedUrl + path, pathParams, queryParams)
    val methodCol = getMethod(method)

    val allFields = urlCol ++ methodCol ++ requestBodyMetadata ++ headers ++ pathParams ++ queryParams
    allFields.map(c => c.copy(dataSourceReadOptions = readOptions))
  }

  def getRequestBodyMetadata(operation: Operation): List[FieldMetadata] = {
    if (operation.getRequestBody != null) {
      val requestContent = operation.getRequestBody.getContent.asScala.head
      val requestContentType = requestContent._1
      val schema = requestContent._2.getSchema
      val fieldsMetadata = getFieldMetadata(schema)
      val sqlGenerator = requestContentType.toLowerCase match {
        case "application/x-www-form-urlencoded" =>
          val colNameToValue = fieldsMetadata.map(c => s"CONCAT('${c.field}=', CAST(`${c.field}` AS STRING))").mkString(",")
          s"ARRAY_JOIN(ARRAY($colNameToValue), '&')"
        case "application/json" =>
          s"TO_JSON($REAL_TIME_BODY_CONTENT_FIELD)"
        case x =>
          LOGGER.warn(s"Unsupported request body content type, defaulting to 'application/json', content-type=$x")
          s"TO_JSON($REAL_TIME_BODY_CONTENT_FIELD)"
      }
      val bodyContentDataType = if (fieldsMetadata.size > 1) {
        val bodyContentFieldsDataType = fieldsMetadata.map(c => s"${c.field}: ${c.metadata(FIELD_DATA_TYPE)}").mkString(",")
        s"struct<$bodyContentFieldsDataType>"
      } else if (fieldsMetadata.size == 1) {
        fieldsMetadata.head.metadata(FIELD_DATA_TYPE)
      } else {
        StringType.toString
      }

      List(
        FieldMetadata(REAL_TIME_BODY_FIELD, Map(), Map(FIELD_DATA_TYPE -> StringType.toString, SQL_GENERATOR -> sqlGenerator)),
        FieldMetadata(REAL_TIME_BODY_CONTENT_FIELD, Map(), Map(FIELD_DATA_TYPE -> bodyContentDataType), fieldsMetadata),
        FieldMetadata(REAL_TIME_CONTENT_TYPE_FIELD, Map(), Map(STATIC -> requestContentType, FIELD_DATA_TYPE -> StringType.toString))
      ) ++ fieldsMetadata
    } else List()
  }


  def getQueryParams(params: List[Parameter]): List[FieldMetadata] = {
    val queryParams = params
      .filter(p => p.getIn != null && p.getIn == HTTP_QUERY_PARAMETER)
      .map(p => {
        val colName = s"$HTTP_QUERY_PARAM_FIELD_PREFIX${p.getName}"
        val sqlGenerator = p.getSchema.getType match {
          case "array" =>
            val style = if (p.getStyle == null) StyleEnum.FORM else p.getStyle
            val explode = if (p.getExplode == null) true else p.getExplode.booleanValue()
            val delimiter = (style, explode) match {
              case (StyleEnum.FORM, false) => ","
              case (StyleEnum.SPACEDELIMITED, false) => "%20"
              case (StyleEnum.PIPEDELIMITED, false) => "|"
              case _ => s"&${p.getName}="
            }
            s"""CASE WHEN ARRAY_SIZE($colName) > 0 THEN CONCAT('${p.getName}=', ARRAY_JOIN($colName, '$delimiter')) ELSE null END"""
          case _ => s"CONCAT('${p.getName}=', $colName)"
        }

        val metadata = Map(
          HTTP_PARAMETER_TYPE -> HTTP_QUERY_PARAMETER,
          POST_SQL_EXPRESSION -> sqlGenerator,
          IS_NULLABLE -> (!p.getRequired).toString,
          ENABLED_NULL -> (!p.getRequired).toString,
        ) ++ getSchemaMetadata(p.getSchema)
        FieldMetadata(colName, Map(), metadata)
      })
    queryParams
  }

  def getPathParams(params: List[Parameter]): List[FieldMetadata] = {
    params
      .filter(p => p.getIn != null && p.getIn == HTTP_PATH_PARAMETER)
      .map(p => {
        val baseMetadata = getSchemaMetadata(p.getSchema) ++ Map(
          HTTP_PARAMETER_TYPE -> HTTP_PATH_PARAMETER,
          IS_NULLABLE -> "false", //path param cannot be nullable
          ENABLED_NULL -> "false" //path param cannot be nullable
        )
        FieldMetadata(s"$HTTP_PATH_PARAM_FIELD_PREFIX${p.getName}", Map(), baseMetadata)
      })
  }

  def getHeaders(params: List[Parameter]): List[FieldMetadata] = {
    val headerParams = params.filter(p => p.getIn != null && p.getIn == HTTP_HEADER_PARAMETER)
    if (headerParams.isEmpty) {
      List()
    } else {
      headerParams
        .map(p => {
          val headerValueMetadata = getSchemaMetadata(p.getSchema) ++ Map(HTTP_HEADER_FIELD_PREFIX -> p.getName)
          val isNullableMap = if (p.getRequired) {
            Map(IS_NULLABLE -> "false")
          } else {
            Map(IS_NULLABLE -> "true", ENABLED_NULL -> "true")
          }
          val isContentLengthMap = if (p.getName == "Content-Length") Map(SQL_GENERATOR -> s"LENGTH($REAL_TIME_BODY_FIELD)") else Map()
          val cleanColName = p.getName.replaceAll("-", "_")
          FieldMetadata(
            s"$HTTP_HEADER_FIELD_PREFIX$cleanColName",
            Map(),
            headerValueMetadata ++ isNullableMap ++ isContentLengthMap
          )
        })
    }
  }

  def getMethod(method: HttpMethod): List[FieldMetadata] = {
    List(FieldMetadata(REAL_TIME_METHOD_FIELD, Map(), Map(STATIC -> method.name(), FIELD_DATA_TYPE -> StringType.toString)))
  }

  def getUrl(baseUrl: String, pathParams: List[FieldMetadata], queryParams: List[FieldMetadata]): List[FieldMetadata] = {
    val sqlGenerator = urlSqlGenerator(baseUrl, pathParams, queryParams)
    List(FieldMetadata(REAL_TIME_URL_FIELD, Map(), Map(SQL_GENERATOR -> sqlGenerator, FIELD_DATA_TYPE -> StringType.toString)))
  }

  def urlSqlGenerator(baseUrl: String, pathParams: List[FieldMetadata], queryParams: List[FieldMetadata]): String = {
    val urlWithPathParamReplace = pathParams.foldLeft(s"'$baseUrl'")((url, pathParam) => {
      val colName = pathParam.field
      val colNameWithoutPrefix = colName.replaceFirst(HTTP_PATH_PARAM_FIELD_PREFIX, "")
      val replaceValue = pathParam.metadata.getOrElse(POST_SQL_EXPRESSION, s"`$colName`")
      s"REPLACE($url, '{$colNameWithoutPrefix}', URL_ENCODE($replaceValue))"
    })
    val urlWithPathAndQuery = if (queryParams.nonEmpty) s"CONCAT($urlWithPathParamReplace, '?')" else urlWithPathParamReplace
    val combinedQueryParams = queryParams.map(q => s"CAST(${q.metadata.getOrElse(POST_SQL_EXPRESSION, s"`${q.field}``")} AS STRING)").mkString(",")
    val combinedQuerySql = s"URL_ENCODE(ARRAY_JOIN(ARRAY($combinedQueryParams), '&'))"
    s"CONCAT($urlWithPathAndQuery, $combinedQuerySql)"
  }

  def getFieldMetadata(schema: Schema[_]): List[FieldMetadata] = {
    if (schema.getType == "array") {
      val arrayMetadata = getSchemaMetadata(schema)
      val arrayName = if (schema.getName != null) schema.getName else "array"
      List(FieldMetadata(arrayName, Map(), arrayMetadata, getFieldMetadata(schema.getItems)))
    } else if (schema.getType == "object") {
      val objectMetadata = getSchemaMetadata(schema)
      val objectName = if (schema.getName != null) schema.getName else "object"
      val nestedCols = schema.getProperties.asScala.flatMap(prop => {
        prop._2.setName(prop._1)
        getFieldMetadata(prop._2)
      }).toList
      List(FieldMetadata(objectName, Map(), objectMetadata, nestedCols))
    } else {
      val nestedFields = getFields(schema)
      if (nestedFields.nonEmpty) {
        nestedFields
          .map(field => {
            val fieldMetadata = getSchemaMetadata(field._2, Some(field._1), Some(schema))
            val nestedCols = if (field._2.getType == "array" && field._2.getItems != null) {
              getFieldMetadata(field._2.getItems)
            } else {
              getFieldMetadata(field._2)
            }
            FieldMetadata(field._1, Map(), fieldMetadata, nestedCols)
          }).toList
      } else if (schema.getName != null) {
        val fieldMetadata = getSchemaMetadata(schema)
        List(FieldMetadata(schema.getName, Map(), fieldMetadata))
      } else {
        List()
      }
    }
  }

  @tailrec
  private def getFields(schema: Schema[_]): Set[(String, Schema[_])] = {
    if (schema.getProperties != null) {
      schema.getProperties.entrySet().asScala.map(entry => (entry.getKey, entry.getValue)).toSet
    } else if (schema.get$ref() != null) {
      getFields(getRefSchema(schema))
    } else if (schema.getType == "array") {
      val arrayName = if (schema.getName != null) schema.getName else "array"
      Set(arrayName -> schema)
    } else {
      Set()
    }
  }

  private def getSchemaMetadata(field: Schema[_], optFieldName: Option[String] = None, optParentSchema: Option[Schema[_]] = None): Map[String, String] = {
    val dataType = getDataType(field).toString
    val minMaxMap = getMinMax(field)
    val miscMap = getMiscMetadata(field, optFieldName, optParentSchema)
    Map(FIELD_DATA_TYPE -> dataType) ++ minMaxMap ++ miscMap
  }

  private def getDataType(schema: Schema[_]): DataType = {
    if (schema == null) {
      LOGGER.warn("Schema is missing from OpenAPI document, defaulting to data type string")
      StringType
    } else {
      if (schema.get$ref() != null) {
        val fields = getFields(schema)
        val mappedFields = fields.map(field => field._1 -> getDataType(field._2)).toList
        new StructType(mappedFields)
      } else {
        (schema.getType, schema.getFormat) match {
          case ("string", "date") => DateType
          case ("string", "date-time") => TimestampType
          case ("string", "binary") => BinaryType
          case ("string", _) => StringType
          case ("number", "float") => FloatType
          case ("number", _) => DoubleType
          case ("integer", "int64") => LongType
          case ("integer", _) => IntegerType
          case ("array", _) =>
            val innerType = getDataType(schema.getItems)
            new ArrayType(innerType)
          case ("object", _) =>
            val innerType = schema.getProperties.asScala.map(s => (s._1, getDataType(s._2))).toList
            new StructType(innerType)
          case (x, _) =>
            val tryDataType = Try(DataType.fromString(x))
            tryDataType match {
              case Failure(exception) =>
                LOGGER.error(s"Unable to convert field to data type, schema=$schema, field-type=${schema.getType}, field-format=${schema.getFormat}")
                throw UnsupportedOpenApiDataTypeException(schema.getType, schema.getType, exception)
              case Success(value) => value
            }
        }
      }
    }
  }

  private def getMiscMetadata(field: Schema[_], optFieldName: Option[String], optParentSchema: Option[Schema[_]]): Map[String, String] = {
    val nullabilityMap = optParentSchema.map(schema => {
      val parentSchema = if (schema.get$ref() != null) getRefSchema(schema) else schema
      val requiredFields = if (parentSchema.getRequired != null) parentSchema.getRequired.asScala.toList else List()
      if (requiredFields.contains(optFieldName.getOrElse(""))) {
        Map(IS_NULLABLE -> false)
      } else {
        Map(
          IS_NULLABLE -> true,
          ENABLED_NULL -> true
        )
      }
    }).getOrElse(Map())
    val oneOfMap = if (field.getEnum != null) {
      Map(ONE_OF_GENERATOR -> field.getEnum.asScala.map(_.toString).mkString(ONE_OF_GENERATOR_DELIMITER))
    } else Map()

    (nullabilityMap ++ oneOfMap ++ Map(REGEX_GENERATOR -> field.getPattern))
      .flatMap(getMetadataFromSchemaAsMap)
  }

  private def getRefSchema(schema: Schema[_]): Schema[_] = {
    val componentName = schema.get$ref().split("/").last
    openAPI.getComponents.getSchemas.get(componentName)
  }

  private def getMinMax(schema: Schema[_]): Map[String, String] = {
    val minMap = Map(
      MINIMUM -> schema.getMinimum,
      MINIMUM_LENGTH -> schema.getMinLength,
      ARRAY_MINIMUM_LENGTH -> schema.getMinItems
    ).flatMap(getMetadataFromSchemaAsMap)

    val maxMap = Map(
      MAXIMUM -> schema.getMaximum,
      MAXIMUM_LENGTH -> schema.getMaxLength,
      ARRAY_MAXIMUM_LENGTH -> schema.getMaxItems
    ).flatMap(getMetadataFromSchemaAsMap)

    minMap ++ maxMap
  }

  private def getMetadataFromSchemaAsMap[T](keyVal: (String, T)): Map[String, String] = {
    if (keyVal._2 != null) {
      Map(keyVal._1 -> keyVal._2.toString)
    } else {
      Map()
    }
  }
}
