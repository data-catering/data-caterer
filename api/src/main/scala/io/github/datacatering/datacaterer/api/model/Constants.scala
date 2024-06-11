package io.github.datacatering.datacaterer.api.model

import java.util.UUID

object Constants {

  lazy val PLAN_CLASS = "PLAN_CLASS"

  //supported data formats
  lazy val CASSANDRA = "org.apache.spark.sql.cassandra"
  lazy val CASSANDRA_NAME = "cassandra"
  lazy val JDBC = "jdbc"
  lazy val POSTGRES = "postgres"
  lazy val MYSQL = "mysql"
  lazy val HTTP = "http"
  lazy val JMS = "jms"
  lazy val KAFKA = "kafka"
  lazy val SOLACE = "solace"
  lazy val RATE = "rate"
  //file formats
  lazy val CSV = "csv"
  lazy val DELTA = "delta"
  lazy val JSON = "json"
  lazy val ORC = "orc"
  lazy val PARQUET = "parquet"
  lazy val HUDI = "hudi"
  lazy val ICEBERG = "iceberg"
  lazy val XML = "xml"
  //jdbc drivers
  lazy val POSTGRES_DRIVER = "org.postgresql.Driver"
  lazy val MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

  //spark data options
  lazy val FORMAT = "format"
  lazy val PATH = "path"
  lazy val SAVE_MODE = "saveMode"
  lazy val CASSANDRA_KEYSPACE = "keyspace"
  lazy val CASSANDRA_TABLE = "table"
  lazy val JDBC_TABLE = "dbtable"
  lazy val SCHEMA = "schema"
  lazy val TABLE = "table"
  lazy val JDBC_QUERY = "query"
  lazy val URL = "url"
  lazy val USERNAME = "user"
  lazy val PASSWORD = "password"
  lazy val DRIVER = "driver"
  lazy val PARTITIONS = "partitions"
  lazy val PARTITION_BY = "partitionBy"
  lazy val BODY_FIELD = "bodyField"
  lazy val KAFKA_TOPIC = "topic"
  lazy val JMS_DESTINATION_NAME = "destinationName"
  lazy val JMS_INITIAL_CONTEXT_FACTORY = "initialContextFactory"
  lazy val JMS_CONNECTION_FACTORY = "connectionFactory"
  lazy val JMS_VPN_NAME = "vpnName"
  lazy val SCHEMA_LOCATION = "schemaLocation"
  lazy val GREAT_EXPECTATIONS_FILE = "expectationsFile"
  lazy val DATA_CONTRACT_FILE = "dataContractFile"
  lazy val ROWS_PER_SECOND = "rowsPerSecond"
  lazy val HUDI_TABLE_NAME = "hoodie.table.name"
  lazy val ICEBERG_CATALOG_TYPE = "catalogType"
  lazy val ICEBERG_CATALOG_URI = "catalogUri"
  lazy val ICEBERG_CATALOG_DEFAULT_NAMESPACE = "catalogDefaultNamespace"

  //field metadata
  lazy val FIELD_DATA_TYPE = "type"
  lazy val FIELD_DESCRIPTION = "description"
  lazy val RANDOM_SEED = "seed"
  lazy val ENABLED_NULL = "enableNull"
  lazy val PROBABILITY_OF_NULL = "nullProb"
  lazy val ENABLED_EDGE_CASE = "enableEdgeCase"
  lazy val PROBABILITY_OF_EDGE_CASE = "edgeCaseProb"
  lazy val AVERAGE_LENGTH = "avgLen"
  lazy val MINIMUM_LENGTH = "minLen"
  lazy val ARRAY_MINIMUM_LENGTH = "arrayMinLen"
  lazy val MAXIMUM_LENGTH = "maxLen"
  lazy val ARRAY_MAXIMUM_LENGTH = "arrayMaxLen"
  lazy val SOURCE_MAXIMUM_LENGTH = "sourceMaxLen"
  lazy val MINIMUM = "min"
  lazy val MAXIMUM = "max"
  lazy val STANDARD_DEVIATION = "stddev"
  lazy val MEAN = "mean"
  lazy val DISTRIBUTION = "distribution"
  lazy val DISTRIBUTION_RATE_PARAMETER = "distributionRateParam"
  lazy val DISTRIBUTION_UNIFORM = "uniform"
  lazy val DISTRIBUTION_EXPONENTIAL = "exponential"
  lazy val DISTRIBUTION_NORMAL = "normal"
  lazy val ARRAY_TYPE = "arrayType"
  lazy val EXPRESSION = "expression"
  lazy val DISTINCT_COUNT = "distinctCount"
  lazy val ROW_COUNT = "count"
  lazy val IS_PRIMARY_KEY = "isPrimaryKey"
  lazy val PRIMARY_KEY_POSITION = "primaryKeyPos"
  lazy val IS_UNIQUE = "isUnique"
  lazy val IS_NULLABLE = "isNullable"
  lazy val NULL_COUNT = "nullCount"
  lazy val HISTOGRAM = "histogram"
  lazy val SOURCE_COLUMN_DATA_TYPE = "sourceDataType"
  lazy val NUMERIC_PRECISION = "precision"
  lazy val NUMERIC_SCALE = "scale"
  lazy val DEFAULT_VALUE = "defaultValue"
  lazy val DATA_SOURCE_GENERATION = "dataSourceGeneration"
  lazy val OMIT = "omit"
  lazy val CONSTRAINT_TYPE = "constraintType"
  lazy val STATIC = "static"
  lazy val CLUSTERING_POSITION = "clusteringPos"
  lazy val METADATA_IDENTIFIER = "metadataIdentifier"
  lazy val FIELD_LABEL = "label"
  lazy val IS_PII = "isPII"
  lazy val HTTP_PARAMETER_TYPE = "httpParamType"
  lazy val POST_SQL_EXPRESSION = "postSqlExpression"

  //field labels
  lazy val LABEL_NAME = "name"
  lazy val LABEL_USERNAME = "username"
  lazy val LABEL_ADDRESS = "address"
  lazy val LABEL_APP = "app"
  lazy val LABEL_NATION = "nation"
  lazy val LABEL_MONEY = "money"
  lazy val LABEL_INTERNET = "internet"
  lazy val LABEL_FOOD = "food"
  lazy val LABEL_JOB = "job"
  lazy val LABEL_RELATIONSHIP = "relationship"
  lazy val LABEL_WEATHER = "weather"
  lazy val LABEL_PHONE = "phone"
  lazy val LABEL_GEO = "geo"

  //expressions
  lazy val FAKER_EXPR_FIRST_NAME = "Name.firstname"
  lazy val FAKER_EXPR_LAST_NAME = "Name.lastname"
  lazy val FAKER_EXPR_USERNAME = "Name.username"
  lazy val FAKER_EXPR_NAME = "Name.name"
  lazy val FAKER_EXPR_CITY = "Address.city"
  lazy val FAKER_EXPR_COUNTRY = "Address.country"
  lazy val FAKER_EXPR_COUNTRY_CODE = "Address.countryCode"
  lazy val FAKER_EXPR_NATIONALITY = "Nation.nationality"
  lazy val FAKER_EXPR_LANGUAGE = "Nation.language"
  lazy val FAKER_EXPR_CAPITAL = "Nation.capitalCity"
  lazy val FAKER_EXPR_APP_VERSION = "App.version"
  lazy val FAKER_EXPR_PAYMENT_METHODS = "Subscription.paymentMethods"
  lazy val FAKER_EXPR_MAC_ADDRESS = "Internet.macAddress"
  lazy val FAKER_EXPR_CURRENCY = "Money.currency"
  lazy val FAKER_EXPR_CURRENCY_CODE = "Money.currencyCode"
  lazy val FAKER_EXPR_CREDIT_CARD = "Finance.creditCard"
  lazy val FAKER_EXPR_FOOD = "Food.dish"
  lazy val FAKER_EXPR_FOOD_INGREDIENT = "Food.ingredient"
  lazy val FAKER_EXPR_JOB_FIELD = "Job.field"
  lazy val FAKER_EXPR_JOB_POSITION = "Job.position"
  lazy val FAKER_EXPR_JOB_TITLE = "Job.title"
  lazy val FAKER_EXPR_RELATIONSHIP = "Relationship.any"
  lazy val FAKER_EXPR_WEATHER = "Weather.description"
  lazy val FAKER_EXPR_PHONE = "PhoneNumber.cellPhone"
  lazy val FAKER_EXPR_EMAIL = "Internet.emailAddress"
  lazy val FAKER_EXPR_IPV4 = "Internet.ipV4Address"
  lazy val FAKER_EXPR_IPV6 = "Internet.ipV6Address"
  lazy val FAKER_EXPR_ADDRESS = "Address.fullAddress"
  lazy val FAKER_EXPR_ADDRESS_POSTCODE = "Address.postcode"

  //generator types
  lazy val RANDOM_GENERATOR = "random"
  lazy val ONE_OF_GENERATOR = "oneOf"
  lazy val REGEX_GENERATOR = "regex"
  lazy val SQL_GENERATOR = "sql"

  //flag names
  lazy val ENABLE_DATA_GENERATION = "enableDataGeneration"
  lazy val ENABLE_DATA_VALIDATION = "enableDataValidation"

  //flags defaults
  lazy val DEFAULT_ENABLE_COUNT = true
  lazy val DEFAULT_ENABLE_GENERATE_DATA = true
  lazy val DEFAULT_ENABLE_RECORD_TRACKING = false
  lazy val DEFAULT_ENABLE_DELETE_GENERATED_RECORDS = false
  lazy val DEFAULT_ENABLE_GENERATE_PLAN_AND_TASKS = false
  lazy val DEFAULT_ENABLE_FAIL_ON_ERROR = true
  lazy val DEFAULT_ENABLE_UNIQUE_CHECK = false
  lazy val DEFAULT_ENABLE_SINK_METADATA = false
  lazy val DEFAULT_ENABLE_SAVE_REPORTS = true
  lazy val DEFAULT_ENABLE_VALIDATION = true
  lazy val DEFAULT_ENABLE_SUGGEST_VALIDATIONS = false
  lazy val DEFAULT_ENABLE_ALERTS = true

  //folders defaults
  lazy val DEFAULT_PLAN_FILE_PATH = "/opt/app/plan/customer-create-plan.yaml"
  lazy val DEFAULT_TASK_FOLDER_PATH = "/opt/app/task"
  lazy val DEFAULT_GENERATED_PLAN_AND_TASK_FOLDER_PATH = "/tmp"
  lazy val DEFAULT_GENERATED_REPORTS_FOLDER_PATH = "/opt/app/report"
  lazy val DEFAULT_RECORD_TRACKING_FOLDER_PATH = "/opt/app/record-tracking"
  lazy val DEFAULT_VALIDATION_FOLDER_PATH = "/opt/app/validation"
  lazy val DEFAULT_RECORD_TRACKING_VALIDATION_FOLDER_PATH = "/opt/app/record-tracking-validation"

  //metadata defaults
  lazy val DEFAULT_NUM_RECORD_FROM_DATA_SOURCE = 10000
  lazy val DEFAULT_NUM_RECORD_FOR_ANALYSIS = 10000
  lazy val DEFAULT_ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD = 0.2
  lazy val DEFAULT_ONE_OF_MIN_COUNT = 1000
  lazy val DEFAULT_NUM_GENERATED_SAMPLES = 10

  //generation defaults
  lazy val DEFAULT_NUM_RECORDS_PER_BATCH = 100000

  //spark defaults
  lazy val DEFAULT_MASTER = "local[*]"
  lazy val DEFAULT_RUNTIME_CONFIG = Map(
    "spark.sql.cbo.enabled" -> "true",
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.cbo.planStats.enabled" -> "true",
    "spark.sql.legacy.allowUntypedScalaUDF" -> "true",
    "spark.sql.legacy.allowParameterlessCount" -> "true",
    "spark.sql.statistics.histogram.enabled" -> "true",
    "spark.sql.shuffle.partitions" -> "10",
    "spark.sql.catalog.postgres" -> "",
    "spark.sql.catalog.cassandra" -> "com.datastax.spark.connector.datasource.CassandraCatalog",
    "spark.sql.catalog.iceberg" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type" -> "hadoop",
    //    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    //    "spark.sql.catalog.hudi" -> "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    //    "spark.kryo.registrator" -> "org.apache.spark.HoodieSparkKryoRegistrar",
    //    "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    "spark.hadoop.fs.s3a.directory.marker.retention" -> "keep",
    "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled" -> "true",
    "spark.hadoop.fs.hdfs.impl" -> "org.apache.hadoop.hdfs.DistributedFileSystem",
    "spark.hadoop.fs.file.impl" -> "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem",
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  )

  //jdbc defaults
  lazy val DEFAULT_POSTGRES_URL = "jdbc:postgresql://postgresserver:5432/customer"
  lazy val DEFAULT_POSTGRES_USERNAME = "postgres"
  lazy val DEFAULT_POSTGRES_PASSWORD = "postgres"
  lazy val DEFAULT_MYSQL_URL = "jdbc:mysql://mysqlserver:3306/customer"
  lazy val DEFAULT_MYSQL_USERNAME = "root"
  lazy val DEFAULT_MYSQL_PASSWORD = "root"

  //cassandra defaults
  lazy val DEFAULT_CASSANDRA_URL = "cassandraserver:9042"
  lazy val DEFAULT_CASSANDRA_USERNAME = "cassandra"
  lazy val DEFAULT_CASSANDRA_PASSWORD = "cassandra"

  //solace defaults
  lazy val DEFAULT_SOLACE_URL = "smf://solaceserver:55554"
  lazy val DEFAULT_SOLACE_USERNAME = "admin"
  lazy val DEFAULT_SOLACE_PASSWORD = "admin"
  lazy val DEFAULT_SOLACE_VPN_NAME = "default"
  lazy val DEFAULT_SOLACE_CONNECTION_FACTORY = "/jms/cf/default"
  lazy val DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY = "com.solacesystems.jndi.SolJNDIInitialContextFactory"

  //kafka defaults
  lazy val DEFAULT_KAFKA_URL = "kafkaserver:9092"

  //http defaults
  lazy val DEFAULT_REAL_TIME_HEADERS_INNER_DATA_TYPE = "struct<key: string, value: binary>"
  lazy val DEFAULT_REAL_TIME_HEADERS_DATA_TYPE = s"array<$DEFAULT_REAL_TIME_HEADERS_INNER_DATA_TYPE>"
  lazy val HTTP_PATH_PARAMETER = "path"
  lazy val HTTP_QUERY_PARAMETER = "query"
  lazy val HTTP_HEADER_PARAMETER = "header"

  //iceberg defaults
  lazy val DEFAULT_ICEBERG_CATALOG_TYPE = ICEBERG_CATALOG_HADOOP

  //foreign key defaults
  lazy val DEFAULT_FOREIGN_KEY_COLUMN = "default_column"
  lazy val FOREIGN_KEY_DELIMITER = "||"
  lazy val FOREIGN_KEY_DELIMITER_REGEX = "\\|\\|"
  lazy val FOREIGN_KEY_PLAN_FILE_DELIMITER = "."
  lazy val FOREIGN_KEY_PLAN_FILE_DELIMITER_REGEX = "\\."

  //task defaults
  def DEFAULT_TASK_NAME: String = UUID.randomUUID().toString

  lazy val DEFAULT_DATA_SOURCE_NAME = "json"
  lazy val DEFAULT_TASK_SUMMARY_ENABLE = true

  //step defaults
  def DEFAULT_STEP_NAME: String = UUID.randomUUID().toString

  lazy val DEFAULT_STEP_TYPE = "json"
  lazy val DEFAULT_STEP_ENABLED = true

  //field defaults
  def DEFAULT_FIELD_NAME: String = UUID.randomUUID().toString

  lazy val DEFAULT_FIELD_TYPE = "string"
  lazy val DEFAULT_FIELD_NULLABLE = true
  lazy val ONE_OF_GENERATOR_DELIMITER = ","

  //generator defaults
  lazy val DEFAULT_GENERATOR_TYPE = "random"

  //count defaults
  lazy val DEFAULT_COUNT_RECORDS = 1000L
  lazy val DEFAULT_PER_COLUMN_COUNT_RECORDS = 10L

  //validation defaults
  lazy val DEFAULT_VALIDATION_CONFIG_NAME = "default_validation"
  lazy val DEFAULT_VALIDATION_DESCRIPTION = "Validation of data sources after generating data"
  lazy val DEFAULT_VALIDATION_JOIN_TYPE = "outer"
  lazy val DEFAULT_VALIDATION_NUM_ERROR_RECORDS = 5
  lazy val DEFAULT_VALIDATION_DELETE_RECORD_TRACKING_FILES = true
  lazy val DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME = "tmp_http_data_source"
  lazy val DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD = "GET"
  lazy val DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES = List(200)
  lazy val DEFAULT_VALIDATION_COLUMN_NAME_TYPE = VALIDATION_COLUMN_NAME_COUNT_EQUAL

  //metadata source
  lazy val METADATA_SOURCE_TYPE = "metadata_source_type"
  lazy val METADATA_SOURCE_NAME = "metadata_source_name"
  lazy val METADATA_SOURCE_HAS_OPEN_LINEAGE_SUPPORT = "metadata_source_has_open_lineage_support"
  lazy val METADATA_SOURCE_URL = "metadata_source_url"
  lazy val MARQUEZ = "marquez"
  lazy val OPEN_METADATA = "open_metadata"
  lazy val OPEN_API = "open_api"
  lazy val GREAT_EXPECTATIONS = "great_expectations"
  lazy val OPEN_DATA_CONTRACT_STANDARD = "open_data_contract_standard"
  lazy val AMUNDSEN = "amundsen"
  lazy val DATAHUB = "datahub"
  lazy val DEFAULT_METADATA_SOURCE_NAME = "default_metadata_source"

  //alert source
  lazy val SLACK = "slack"

  //openlineage
  lazy val OPEN_LINEAGE_NAMESPACE = "namespace"
  lazy val OPEN_LINEAGE_DATASET = "dataset"
  lazy val DATASET_NAME = "name"
  lazy val FACET_DATA_SOURCE = "dataSource"
  lazy val DATA_SOURCE_NAME = "dataSourceName"
  lazy val URI = "uri"
  lazy val FACET_DATA_QUALITY_METRICS = "dataQualityMetrics"
  lazy val FACET_DATA_QUALITY_ASSERTIONS = "dataQualityAssertions"

  //openmetadata
  lazy val OPEN_METADATA_HOST = "host"
  lazy val OPEN_METADATA_API_VERSION = "apiVersion"
  lazy val OPEN_METADATA_DEFAULT_API_VERSION = "v1"
  lazy val OPEN_METADATA_AUTH_TYPE = "authType"
  lazy val OPEN_METADATA_AUTH_TYPE_BASIC = "basic"
  lazy val OPEN_METADATA_AUTH_TYPE_NO_AUTH = "no-auth"
  lazy val OPEN_METADATA_AUTH_TYPE_AZURE = "azure"
  lazy val OPEN_METADATA_AUTH_TYPE_GOOGLE = "google"
  lazy val OPEN_METADATA_AUTH_TYPE_OKTA = "okta"
  lazy val OPEN_METADATA_AUTH_TYPE_AUTH0 = "auth0"
  lazy val OPEN_METADATA_AUTH_TYPE_AWS_COGNITO = "aws-cognito"
  lazy val OPEN_METADATA_AUTH_TYPE_CUSTOM_OIDC = "custom-oidc"
  lazy val OPEN_METADATA_AUTH_TYPE_OPEN_METADATA = "openmetadata"
  lazy val OPEN_METADATA_BASIC_AUTH_USERNAME = "basicAuthUsername"
  lazy val OPEN_METADATA_BASIC_AUTH_PASSWORD = "basicAuthPassword"
  lazy val OPEN_METADATA_GOOGLE_AUTH_AUDIENCE = "googleAudience"
  lazy val OPEN_METADATA_GOOGLE_AUTH_SECRET_KEY = "googleSecretKey"
  lazy val OPEN_METADATA_OKTA_AUTH_CLIENT_ID = "oktaClientId"
  lazy val OPEN_METADATA_OKTA_AUTH_ORG_URL = "oktaOrgUrl"
  lazy val OPEN_METADATA_OKTA_AUTH_EMAIL = "oktaEmail"
  lazy val OPEN_METADATA_OKTA_AUTH_SCOPES = "oktaScopes"
  lazy val OPEN_METADATA_OKTA_AUTH_PRIVATE_KEY = "oktaPrivateKey"
  lazy val OPEN_METADATA_AUTH0_CLIENT_ID = "auth0ClientId"
  lazy val OPEN_METADATA_AUTH0_SECRET_KEY = "auth0SecretKey"
  lazy val OPEN_METADATA_AUTH0_DOMAIN = "auth0Domain"
  lazy val OPEN_METADATA_AZURE_CLIENT_ID = "azureClientId"
  lazy val OPEN_METADATA_AZURE_CLIENT_SECRET = "azureClientSecret"
  lazy val OPEN_METADATA_AZURE_SCOPES = "azureScopes"
  lazy val OPEN_METADATA_AZURE_AUTHORITY = "azureAuthority"
  lazy val OPEN_METADATA_JWT_TOKEN = "openMetadataJwtToken"
  lazy val OPEN_METADATA_CUSTOM_OIDC_CLIENT_ID = "customOidcClientId"
  lazy val OPEN_METADATA_CUSTOM_OIDC_SECRET_KEY = "customOidcSecretKey"
  lazy val OPEN_METADATA_CUSTOM_OIDC_TOKEN_ENDPOINT = "customOidcTokenEndpoint"
  lazy val OPEN_METADATA_DATABASE = "database"
  lazy val OPEN_METADATA_DATABASE_SCHEMA = "databaseSchema"
  lazy val OPEN_METADATA_TABLE_FQN = "tableFqn"
  lazy val OPEN_METADATA_SERVICE = "service"

  //delta
  lazy val DELTA_LAKE_SPARK_CONF = Map(
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  )

  //iceberg
  lazy val SPARK_ICEBERG_CATALOG_TYPE = "spark.sql.catalog.iceberg.type"
  lazy val SPARK_ICEBERG_CATALOG_WAREHOUSE = "spark.sql.catalog.iceberg.warehouse"
  lazy val SPARK_ICEBERG_CATALOG_URI = "spark.sql.catalog.iceberg.uri"
  lazy val SPARK_ICEBERG_CATALOG_DEFAULT_NAMESPACE = "spark.sql.catalog.iceberg.default-namespace"
  lazy val ICEBERG_CATALOG_HIVE = "hive"
  lazy val ICEBERG_CATALOG_HADOOP = "hadoop"
  lazy val ICEBERG_CATALOG_REST = "rest"
  lazy val ICEBERG_CATALOG_GLUE = "glue"
  lazy val ICEBERG_CATALOG_JDBC = "jdbc"
  lazy val ICEBERG_CATALOG_NESSIE = "nessie"
  lazy val ICEBERG_SPARK_CONF = Map(
    "spark.sql.catalog.local" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.local.type" -> "hadoop",
    "spark.sql.catalog.local.warehouse" -> "/tmp/iceberg/warehouse",
    "spark.sql.defaultCatalog" -> "local",
  )

  //aggregation types
  lazy val AGGREGATION_SUM = "sum"
  lazy val AGGREGATION_COUNT = "count"
  lazy val AGGREGATION_MAX = "max"
  lazy val AGGREGATION_MIN = "min"
  lazy val AGGREGATION_AVG = "avg"
  lazy val AGGREGATION_STDDEV = "stddev"

  //validation types
  lazy val VALIDATION_COLUMN = "column"
  lazy val VALIDATION_FIELD = "field"
  lazy val VALIDATION_COLUMN_NAMES = "columnNames"
  lazy val VALIDATION_FIELD_NAMES = "fieldNames"
  lazy val VALIDATION_UPSTREAM = "upstream"
  lazy val VALIDATION_GROUP_BY = "groupBy"
  //validation support
  lazy val VALIDATION_DESCRIPTION = "description"
  lazy val VALIDATION_ERROR_THRESHOLD = "errorThreshold"
  //column validations
  lazy val VALIDATION_EQUAL = "equal"
  lazy val VALIDATION_NOT_EQUAL = "notEqual"
  lazy val VALIDATION_NULL = "null"
  lazy val VALIDATION_NOT_NULL = "notNull"
  lazy val VALIDATION_CONTAINS = "contains"
  lazy val VALIDATION_NOT_CONTAINS = "notContains"
  lazy val VALIDATION_UNIQUE = "unique"
  lazy val VALIDATION_LESS_THAN = "lessThan"
  lazy val VALIDATION_LESS_THAN_OR_EQUAL = "equalOrLessThan"
  lazy val VALIDATION_GREATER_THAN = "greaterThan"
  lazy val VALIDATION_GREATER_THAN_OR_EQUAL = "equalOrGreaterThan"
  lazy val VALIDATION_BETWEEN = "between"
  lazy val VALIDATION_NOT_BETWEEN = "notBetween"
  lazy val VALIDATION_IN = "in"
  lazy val VALIDATION_NOT_IN = "notIn"
  lazy val VALIDATION_MATCHES = "matches"
  lazy val VALIDATION_NOT_MATCHES = "notMatches"
  lazy val VALIDATION_STARTS_WITH = "startsWith"
  lazy val VALIDATION_NOT_STARTS_WITH = "notStartsWith"
  lazy val VALIDATION_ENDS_WITH = "endsWith"
  lazy val VALIDATION_NOT_ENDS_WITH = "notEndsWith"
  lazy val VALIDATION_SIZE = "size"
  lazy val VALIDATION_NOT_SIZE = "notSize"
  lazy val VALIDATION_LESS_THAN_SIZE = "lessThanSize"
  lazy val VALIDATION_LESS_THAN_OR_EQUAL_SIZE = "equalOrLessThanSize"
  lazy val VALIDATION_GREATER_THAN_SIZE = "greaterThanSize"
  lazy val VALIDATION_GREATER_THAN_OR_EQUAL_SIZE = "equalOrGreaterThanSize"
  lazy val VALIDATION_LUHN_CHECK = "luhnCheck"
  lazy val VALIDATION_HAS_TYPE = "hasType"
  lazy val VALIDATION_SQL = "sql"
  //group by validations
  lazy val VALIDATION_AGGREGATION_TYPE = "aggType"
  lazy val VALIDATION_AGGREGATION_COLUMN = "aggCol"
  lazy val VALIDATION_MIN = "min"
  lazy val VALIDATION_MAX = "max"
  lazy val VALIDATION_COUNT = "count"
  lazy val VALIDATION_SUM = "sum"
  lazy val VALIDATION_AVERAGE = "average"
  lazy val VALIDATION_STANDARD_DEVIATION = "standardDeviation"
  lazy val VALIDATION_GROUP_BY_COLUMNS = "groupByColumns"
  //upstream validations
  lazy val VALIDATION_UPSTREAM_TASK_NAME = "upstreamTaskName"
  lazy val VALIDATION_UPSTREAM_JOIN_COLUMNS = "joinColumns"
  lazy val VALIDATION_UPSTREAM_JOIN_TYPE = "joinType"
  lazy val VALIDATION_UPSTREAM_JOIN_EXPR = "joinExpr"
  //column name validations
  lazy val VALIDATION_COLUMN_NAMES_COUNT_EQUAL = "countEqual"
  lazy val VALIDATION_COLUMN_NAMES_COUNT_BETWEEN = "countBetween"
  lazy val VALIDATION_COLUMN_NAMES_MATCH_ORDER = "matchOrder"
  lazy val VALIDATION_COLUMN_NAMES_MATCH_SET = "matchSet"

  lazy val VALIDATION_OPTION_DELIMITER = ","
  lazy val VALIDATION_SUPPORTING_OPTIONS = List(VALIDATION_COLUMN, VALIDATION_FIELD, VALIDATION_MIN, VALIDATION_MAX, VALIDATION_GROUP_BY_COLUMNS, VALIDATION_DESCRIPTION, VALIDATION_ERROR_THRESHOLD)

  lazy val VALIDATION_PREFIX_JOIN_EXPRESSION = "expr:"
  lazy val VALIDATION_COLUMN_NAME_COUNT_EQUAL = "column_count_equal"
  lazy val VALIDATION_COLUMN_NAME_COUNT_BETWEEN = "column_count_between"
  lazy val VALIDATION_COLUMN_NAME_MATCH_ORDER = "column_name_match_order"
  lazy val VALIDATION_COLUMN_NAME_MATCH_SET = "column_name_match_set"

  //configuration names
  //flags config
  lazy val CONFIG_FLAGS_COUNT = "enableCount"
  lazy val CONFIG_FLAGS_GENERATE_DATA = "enableGenerateData"
  lazy val CONFIG_FLAGS_RECORD_TRACKING = "enableRecordTracking"
  lazy val CONFIG_FLAGS_DELETE_GENERATED_RECORDS = "enableDeleteGeneratedRecords"
  lazy val CONFIG_FLAGS_GENERATE_PLAN_AND_TASKS = "enableGeneratePlanAndTasks"
  lazy val CONFIG_FLAGS_FAIL_ON_ERROR = "enableFailOnError"
  lazy val CONFIG_FLAGS_UNIQUE_CHECK = "enableUniqueCheck"
  lazy val CONFIG_FLAGS_SINK_METADATA = "enableSinkMetadata"
  lazy val CONFIG_FLAGS_SAVE_REPORTS = "enableSaveReports"
  lazy val CONFIG_FLAGS_VALIDATION = "enableValidation"
  lazy val CONFIG_FLAGS_GENERATE_VALIDATIONS = "enableGenerateValidations"
  lazy val CONFIG_FLAGS_ALERTS = "enableAlerts"
  //folder config
  lazy val CONFIG_FOLDER_PLAN_FILE_PATH = "planFilePath"
  lazy val CONFIG_FOLDER_TASK_FOLDER_PATH = "taskFolderPath"
  lazy val CONFIG_FOLDER_GENERATED_PLAN_AND_TASK_FOLDER_PATH = "generatedPlanAndTasksFolderPath"
  lazy val CONFIG_FOLDER_GENERATED_REPORTS_FOLDER_PATH = "generatedReportsFolderPath"
  lazy val CONFIG_FOLDER_RECORD_TRACKING_FOLDER_PATH = "recordTrackingFolderPath"
  lazy val CONFIG_FOLDER_VALIDATION_FOLDER_PATH = "validationFolderPath"
  lazy val CONFIG_FOLDER_RECORD_TRACKING_FOR_VALIDATION_FOLDER_PATH = "recordTrackingForValidationFolderPath"
  //metadata config
  lazy val CONFIG_METADATA_NUM_RECORDS_FROM_DATA_SOURCE = "numRecordsFromDataSource"
  lazy val CONFIG_METADATA_NUM_RECORDS_FOR_ANALYSIS = "numRecordsForAnalysis"
  lazy val CONFIG_METADATA_ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD = "oneOfDistinctCountVsCountThreshold"
  lazy val CONFIG_METADATA_ONE_OF_MIN_COUNT = "oneOfMinCount"
  lazy val CONFIG_METADATA_NUM_GENERATED_SAMPLES = "numGeneratedSamples"
  //generation config
  lazy val CONFIG_GENERATION_NUM_RECORDS_PER_BATCH = "numRecordsPerBatch"
  lazy val CONFIG_GENERATION_NUM_RECORDS_PER_STEP = "numRecordsPerStep"
  //validation config
  lazy val CONFIG_VALIDATION_NUM_SAMPLE_ERROR_RECORDS = "numSampleErrorRecords"
  lazy val CONFIG_VALIDATION_ENABLE_DELETE_RECORD_TRACKING_FILES = "enableDeleteRecordTrackingFiles"
  //alert config
  lazy val CONFIG_ALERT_TRIGGER_ON = "triggerOn"
  lazy val CONFIG_ALERT_SLACK_TOKEN = "slackToken"
  lazy val CONFIG_ALERT_SLACK_CHANNELS = "slackChannels"

  //alert trigger on
  lazy val ALERT_TRIGGER_ON_ALL = "all"
  lazy val ALERT_TRIGGER_ON_FAILURE = "failure"
  lazy val ALERT_TRIGGER_ON_SUCCESS = "success"
  lazy val ALERT_TRIGGER_ON_GENERATION_FAILURE = "generation_failure"
  lazy val ALERT_TRIGGER_ON_VALIDATION_FAILURE = "validation_failure"
  lazy val ALERT_TRIGGER_ON_GENERATION_SUCCESS = "generation_success"
  lazy val ALERT_TRIGGER_ON_VALIDATION_SUCCESS = "validation_success"

  //trial
  lazy val API_KEY = "API_KEY"

  //ui
  lazy val PLAN_RUN_EXECUTION_DELIMITER = "||"
  lazy val PLAN_RUN_EXECUTION_DELIMITER_REGEX = "\\|\\|"
  lazy val PLAN_RUN_SUMMARY_DELIMITER = "&&"

}
