package io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import io.github.datacatering.datacaterer.core.exception.{FailedRetrieveOpenMetadataTestCasesException, InvalidFullQualifiedNameException, InvalidOpenMetadataConnectionConfigurationException}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.openmetadata.client.api.{TablesApi, TestCasesApi}
import org.openmetadata.client.gateway.OpenMetadata
import org.openmetadata.client.model.Column.DataTypeEnum
import org.openmetadata.client.model.{Column, Table}
import org.openmetadata.schema.security.client.{Auth0SSOClientConfig, AzureSSOClientConfig, CustomOIDCSSOClientConfig, GoogleSSOClientConfig, OktaSSOClientConfig, OpenMetadataJWTClientConfig}
import org.openmetadata.schema.security.credentials.BasicAuth
import org.openmetadata.schema.services.connections.metadata.{AuthProvider, OpenMetadataConnection}

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter, seqAsJavaListConverter}

case class OpenMetadataDataSourceMetadata(
                                           name: String,
                                           format: String,
                                           connectionConfig: Map[String, String],
                                         ) extends DataSourceMetadata {
  private val LOGGER = Logger.getLogger(getClass.getName)
  private val DATA_TYPE_ARRAY_REGEX = "^array<(.+?)>$".r
  private val DATA_TYPE_MAP_REGEX = "^map<(.+?)>$".r
  private val DATA_TYPE_STRUCT_REGEX = "^struct<(.+?)>$".r
  private val DATA_TYPE_CHAR_VAR_REGEX = "^character varying\\(([0-9]+)\\)$".r
  private val DATA_TYPE_CHAR_REGEX = "^char\\(([0-9]+)\\)$".r
  private val AUTH_CONFIGURATION = List(
    OPEN_METADATA_BASIC_AUTH_USERNAME,
    OPEN_METADATA_BASIC_AUTH_PASSWORD,
    OPEN_METADATA_GOOGLE_AUTH_AUDIENCE,
    OPEN_METADATA_GOOGLE_AUTH_SECRET_KEY,
    OPEN_METADATA_OKTA_AUTH_CLIENT_ID,
    OPEN_METADATA_OKTA_AUTH_ORG_URL,
    OPEN_METADATA_OKTA_AUTH_EMAIL,
    OPEN_METADATA_OKTA_AUTH_SCOPES,
    OPEN_METADATA_OKTA_AUTH_PRIVATE_KEY,
    OPEN_METADATA_AUTH0_CLIENT_ID,
    OPEN_METADATA_AUTH0_SECRET_KEY,
    OPEN_METADATA_AUTH0_DOMAIN,
    OPEN_METADATA_AZURE_CLIENT_ID,
    OPEN_METADATA_AZURE_CLIENT_SECRET,
    OPEN_METADATA_AZURE_SCOPES,
    OPEN_METADATA_AZURE_AUTHORITY,
    OPEN_METADATA_JWT_TOKEN,
    OPEN_METADATA_CUSTOM_OIDC_CLIENT_ID,
    OPEN_METADATA_CUSTOM_OIDC_SECRET_KEY,
    OPEN_METADATA_CUSTOM_OIDC_TOKEN_ENDPOINT
  )
  private lazy val gateway = getGateway

  override val hasSourceData: Boolean = false

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    val tablesApi = gateway.buildClient(classOf[TablesApi])
    val configWithoutAuth = connectionConfig.filter(c => !AUTH_CONFIGURATION.contains(c._1))
      .map(x => (x._1, x._2.asInstanceOf[Object])).asJava
    val allTables = if (connectionConfig.contains(OPEN_METADATA_TABLE_FQN)) {
      List(tablesApi.getTableByFQN(connectionConfig(OPEN_METADATA_TABLE_FQN), configWithoutAuth))
    } else {
      tablesApi.listTables(configWithoutAuth).getData.asScala
    }

    allTables.map(table => {
      val readOptions = table.getServiceType match {
        case Table.ServiceTypeEnum.DBT | Table.ServiceTypeEnum.GLUE =>
          //need more information to determine read options
          Map("" -> "")
        case _ =>
          Map(
            JDBC_TABLE -> getSchemaAndTableName(table.getFullyQualifiedName)
          )
      }
      val allOptions = readOptions ++ Map(METADATA_IDENTIFIER -> table.getFullyQualifiedName)

      val fieldMetadata = table.getColumns.asScala.map(col => {
        val dataType = dataTypeMapping(col)
        val (miscMetadata, dataTypeWithMisc) = getMiscMetadata(col, dataType)
        val description = if (col.getDescription == null) "" else col.getDescription
        val metadata = Map(
          FIELD_DATA_TYPE -> dataTypeWithMisc.toString,
          FIELD_DESCRIPTION -> description,
          SOURCE_FIELD_DATA_TYPE -> col.getDataType.getValue.toLowerCase
        ) ++ miscMetadata
        FieldMetadata(col.getName, allOptions, metadata)
      })
      val fieldMetadataDataset = sparkSession.createDataset(fieldMetadata)
      SubDataSourceMetadata(allOptions, Some(fieldMetadataDataset))
    }).toArray
  }

  private def getGateway: OpenMetadata = {
    val server = new OpenMetadataConnection
    server.setHostPort(getConfig(OPEN_METADATA_HOST))
    server.setApiVersion(getConfig(OPEN_METADATA_API_VERSION))
    authenticate(server)
    new OpenMetadata(server)
  }

  override def getDataSourceValidations(dataSourceReadOptions: Map[String, String]): List[ValidationBuilder] = {
    val entityNameWithFieldPattern = "^(.+?)\\.(.+?)\\.(.+?)\\.(.+?)\\.(.+?)$".r
    val testCasesApi = gateway.buildClient(classOf[TestCasesApi])
    val listTestCases = testCasesApi.listTestCases(Map.empty[String, Object].asJava)

    if (listTestCases.getErrors != null && listTestCases.getErrors.size() > 0) {
      LOGGER.error("Failed to retrieve test cases from OpenMetadata")
      throw FailedRetrieveOpenMetadataTestCasesException(listTestCases.getErrors.asScala.toList)
    } else {
      listTestCases.getData.asScala
        .filter(t => t.getEntityFQN.startsWith(dataSourceReadOptions(METADATA_IDENTIFIER)) && !t.getDeleted)
        .flatMap(testCase => {
          val testParams = testCase.getParameterValues.asScala.map(testParam => {
            (testParam.getName, testParam.getValue)
          }).toMap
          val optFieldName = testCase.getEntityFQN match {
            case entityNameWithFieldPattern(_, _, _, _, field) => Some(field)
            case _ => None
          }

          OpenMetadataDataValidations.getDataValidations(testCase, testParams, optFieldName)
        }).toList
    }
  }

  private def getMiscMetadata(column: Column, dataType: DataType): (Map[String, String], DataType) = {
    val (precisionScaleMeta, updatedDataType) = if (dataType.toString == DecimalType.toString) {
      val precisionMap = Map(NUMERIC_PRECISION -> column.getPrecision.toString, NUMERIC_SCALE -> column.getScale.toString)
      (precisionMap, new DecimalType(column.getPrecision, column.getScale))
    } else (Map(), dataType)

    val miscMetadata = if (column.getProfile != null) {
      val profile = column.getProfile
      val minMaxMetadata = dataType match {
        case StringType => Map(MINIMUM_LENGTH -> profile.getMinLength, MAXIMUM_LENGTH -> profile.getMaxLength)
        case DecimalType | IntegerType | DoubleType | ShortType | LongType | FloatType => Map(
          MINIMUM -> profile.getMin, MAXIMUM -> profile.getMax, STANDARD_DEVIATION -> profile.getStddev, MEAN -> profile.getMean
        )
        case _ => Map[String, Any]()
      }

      val nullMetadata = if (profile.getNullCount > 0) Map(IS_NULLABLE -> "true", ENABLED_NULL -> "true") else Map(IS_NULLABLE -> "false")

      (minMaxMetadata ++ nullMetadata).map(x => (x._1, x._2.toString))
    } else Map()

    (precisionScaleMeta ++ miscMetadata, updatedDataType)
  }

  //col.getDataTypeDisplay => array<struct<product_id:character varying(24),price:int,onsale:boolean,tax:int,weight:int,others:int,vendor:character varying(64), stock:int>>
  private def dataTypeMapping(col: Column): DataType = {
    col.getDataType match {
      case DataTypeEnum.NUMBER | DataTypeEnum.NUMERIC | DataTypeEnum.DOUBLE => DoubleType
      case DataTypeEnum.TINYINT | DataTypeEnum.SMALLINT => ShortType
      case DataTypeEnum.BIGINT | DataTypeEnum.INT | DataTypeEnum.YEAR => IntegerType
      case DataTypeEnum.LONG => LongType
      case DataTypeEnum.DECIMAL => new DecimalType(col.getScale, col.getPrecision)
      case DataTypeEnum.FLOAT => FloatType
      case DataTypeEnum.BOOLEAN => BooleanType
      case DataTypeEnum.BLOB | DataTypeEnum.MEDIUMBLOB | DataTypeEnum.LONGBLOB | DataTypeEnum.BYTEA |
           DataTypeEnum.BYTES | DataTypeEnum.VARBINARY | DataTypeEnum.VARBINARY => BinaryType
      case DataTypeEnum.BYTEINT => ByteType
      case DataTypeEnum.STRING | DataTypeEnum.TEXT | DataTypeEnum.VARCHAR | DataTypeEnum.CHAR | DataTypeEnum.JSON |
           DataTypeEnum.XML | DataTypeEnum.NTEXT | DataTypeEnum.IPV4 | DataTypeEnum.IPV6 | DataTypeEnum.CIDR |
           DataTypeEnum.UUID | DataTypeEnum.INET | DataTypeEnum.CLOB | DataTypeEnum.MACADDR | DataTypeEnum.ENUM |
           DataTypeEnum.MEDIUMTEXT => StringType
      case DataTypeEnum.DATE => DateType
      case DataTypeEnum.TIMESTAMP | DataTypeEnum.TIMESTAMPZ | DataTypeEnum.DATETIME | DataTypeEnum.TIME => TimestampType
      case DataTypeEnum.ARRAY | DataTypeEnum.MAP | DataTypeEnum.STRUCT => getInnerDataType(col.getDataTypeDisplay)
      case _ => StringType
    }
  }

  private def getInnerDataType(dataTypeDisplay: String): DataType = {
    dataTypeDisplay match {
      case DATA_TYPE_ARRAY_REGEX(innerType) => new ArrayType(getInnerDataType(innerType))
      case DATA_TYPE_MAP_REGEX(innerType) =>
        val spt = innerType.split(",")
        val key = getInnerDataType(spt.head)
        val value = getInnerDataType(spt.last)
        new StructType(List("key" -> key, "value" -> value))
      case DATA_TYPE_STRUCT_REGEX(innerType) =>
        val keyValues = innerType.split(",").map(t => t.split(":"))
        val mappedInnerType = keyValues.map(kv => (kv.head, getInnerDataType(kv.last))).toList
        new StructType(mappedInnerType)
      case x =>
        val col = new Column
        val openMetadataDataType = x match {
          case DATA_TYPE_CHAR_VAR_REGEX(_) => DataTypeEnum.VARCHAR
          case DATA_TYPE_CHAR_REGEX(_) => DataTypeEnum.CHAR
          case _ => DataTypeEnum.fromValue(x.toUpperCase)
        }
        col.setDataType(openMetadataDataType)
        col.setDataTypeDisplay(x)
        dataTypeMapping(col)
    }
  }

  private def getSchemaAndTableName(fullTableName: String): String = {
    val spt = fullTableName.split("\\.")
    if (spt.length == 4) {
      s"${spt(2)}.${spt.last}"
    } else {
      throw InvalidFullQualifiedNameException(fullTableName)
    }
  }

  private def authenticate(server: OpenMetadataConnection): OpenMetadataConnection = {
    val authTypeConfig = connectionConfig.getOrElse(OPEN_METADATA_AUTH_TYPE, AuthProvider.NO_AUTH.value())
    val authType = AuthProvider.fromValue(authTypeConfig)
    server.setAuthProvider(authType)
    val securityConfig = authType match {
      case AuthProvider.NO_AUTH => return server
      case AuthProvider.BASIC =>
        val basicAuth = new BasicAuth
        basicAuth.setUsername(getConfig(OPEN_METADATA_BASIC_AUTH_USERNAME))
        basicAuth.setPassword(getConfig(OPEN_METADATA_BASIC_AUTH_PASSWORD))
        basicAuth
      case AuthProvider.AZURE =>
        val azureConf = new AzureSSOClientConfig
        azureConf.setClientId(getConfig(OPEN_METADATA_AZURE_CLIENT_ID))
        azureConf.setClientSecret(getConfig(OPEN_METADATA_AZURE_CLIENT_SECRET))
        azureConf.setScopes(getConfig(OPEN_METADATA_AZURE_SCOPES).split(",").toList.asJava)
        azureConf.setAuthority(getConfig(OPEN_METADATA_AZURE_AUTHORITY))
        azureConf
      case AuthProvider.GOOGLE =>
        val googleConf = new GoogleSSOClientConfig
        googleConf.setAudience(getConfig(OPEN_METADATA_GOOGLE_AUTH_AUDIENCE))
        googleConf.setSecretKey(getConfig(OPEN_METADATA_GOOGLE_AUTH_SECRET_KEY))
        googleConf
      case AuthProvider.OKTA =>
        val oktaConf = new OktaSSOClientConfig
        oktaConf.setClientId(getConfig(OPEN_METADATA_OKTA_AUTH_CLIENT_ID))
        oktaConf.setOrgURL(getConfig(OPEN_METADATA_OKTA_AUTH_ORG_URL))
        oktaConf.setEmail(getConfig(OPEN_METADATA_OKTA_AUTH_EMAIL))
        oktaConf.setScopes(getConfig(OPEN_METADATA_OKTA_AUTH_SCOPES).split(",").toList.asJava)
        oktaConf.setPrivateKey(getConfig(OPEN_METADATA_OKTA_AUTH_PRIVATE_KEY))
        oktaConf
      case AuthProvider.AUTH_0 =>
        val auth0Conf = new Auth0SSOClientConfig
        auth0Conf.setClientId(getConfig(OPEN_METADATA_AUTH0_CLIENT_ID))
        auth0Conf.setSecretKey(getConfig(OPEN_METADATA_AUTH0_SECRET_KEY))
        auth0Conf.setDomain(getConfig(OPEN_METADATA_AUTH0_DOMAIN))
        auth0Conf
      case AuthProvider.AWS_COGNITO =>
        LOGGER.warn("AWS Cognito authentication not supported yet for OpenMetadata")
        return server
      case AuthProvider.CUSTOM_OIDC =>
        val customConf = new CustomOIDCSSOClientConfig
        customConf.setClientId(OPEN_METADATA_CUSTOM_OIDC_CLIENT_ID)
        customConf.setSecretKey(OPEN_METADATA_CUSTOM_OIDC_SECRET_KEY)
        customConf.setTokenEndpoint(OPEN_METADATA_CUSTOM_OIDC_TOKEN_ENDPOINT)
        customConf
      case AuthProvider.LDAP =>
        LOGGER.warn("LDAP authentication not supported yet for OpenMetadata")
        return server
      case AuthProvider.SAML =>
        LOGGER.warn("SAML authentication not supported yet for OpenMetadata")
        return server
      case AuthProvider.OPENMETADATA =>
        val openMetadataConf = new OpenMetadataJWTClientConfig
        openMetadataConf.setJwtToken(getConfig(OPEN_METADATA_JWT_TOKEN))
        openMetadataConf
    }
    server.setSecurityConfig(securityConfig)
    server
  }

  private def getConfig(key: String): String = {
    connectionConfig.get(key) match {
      case Some(value) => value
      case None => throw InvalidOpenMetadataConnectionConfigurationException(key)
    }
  }
}
