package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.connection.{ConnectionTaskBuilder, FileBuilder, JdbcBuilder}
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.DataType
import io.github.datacatering.datacaterer.api.{BasePlanRun, ConnectionConfigWithTaskBuilder, CountBuilder, DataCatererConfigurationBuilder, FieldBuilder, GeneratorBuilder, PlanRun, ValidationBuilder}
import io.github.datacatering.datacaterer.core.ui.model.{DataSourceRequest, PlanRunRequest}

object UiMapper {

  def mapToPlanRun(planRunRequest: PlanRunRequest): PlanRun = {
    val plan = new BasePlanRun()
    val planBuilder = plan.plan.name(planRunRequest.name)
    val connections = planRunRequest.dataSources.map(dataSourceToConnection)
    plan.execute(planBuilder, DataCatererConfigurationBuilder(), connections.head, connections.tail: _*)
    plan
  }

  def dataSourceToConnection(dataSourceRequest: DataSourceRequest): ConnectionTaskBuilder[_] = {
    val baseConnection = dataSourceRequest.`type` match {
      case Some(CASSANDRA_NAME) => createCassandraConnection(dataSourceRequest)
      case Some(POSTGRES) => createJdbcConnection(dataSourceRequest, POSTGRES)
      case Some(MYSQL) => createJdbcConnection(dataSourceRequest, POSTGRES)
      case Some(CSV) => createFileConnection(dataSourceRequest, CSV)
      case Some(JSON) => createFileConnection(dataSourceRequest, JSON)
      case Some(PARQUET) => createFileConnection(dataSourceRequest, PARQUET)
      case Some(ORC) => createFileConnection(dataSourceRequest, ORC)
      case Some(SOLACE) =>
        val opt = dataSourceRequest.options.getOrElse(Map())
        checkOptions(dataSourceRequest.name, List(URL, USERNAME, PASSWORD, JMS_VPN_NAME, JMS_CONNECTION_FACTORY, JMS_INITIAL_CONTEXT_FACTORY), opt)
        ConnectionConfigWithTaskBuilder().solace(dataSourceRequest.name, opt(URL), opt(USERNAME), opt(PASSWORD),
          opt(JMS_VPN_NAME), opt(JMS_CONNECTION_FACTORY), opt(JMS_INITIAL_CONTEXT_FACTORY), opt)
      case Some(KAFKA) =>
        val opt = dataSourceRequest.options.getOrElse(Map())
        checkOptions(dataSourceRequest.name, List(URL), opt)
        ConnectionConfigWithTaskBuilder().kafka(dataSourceRequest.name, opt(URL), opt)
      case Some(HTTP) =>
        val opt = dataSourceRequest.options.getOrElse(Map())
        checkOptions(dataSourceRequest.name, List(URL, USERNAME, PASSWORD), opt)
        ConnectionConfigWithTaskBuilder().http(dataSourceRequest.name, opt(USERNAME), opt(PASSWORD), opt)
      case Some(x) =>
        throw new IllegalArgumentException(s"Unsupported data source from UI, data-source-type=$x")
    }

    val mappedFields = dataSourceRequest.fields.getOrElse(List()).map(field => {
      assert(field.name.nonEmpty, s"Field name cannot be empty, data-source-name=${dataSourceRequest.name}")
      assert(field.`type`.nonEmpty, s"Field type cannot be empty, data-source-name=${dataSourceRequest.name}, field-name=${field.name}")
      FieldBuilder().name(field.name).`type`(DataType.fromString(field.`type`)).options(field.options.getOrElse(Map()))
    })

    val countBuilder = dataSourceRequest.count.map(recordCountRequest => {
      val baseRecordCount = (recordCountRequest.records, recordCountRequest.recordsMin, recordCountRequest.recordsMax) match {
        case (Some(records), None, None) => CountBuilder().records(records)
        case (None, Some(min), Some(max)) => CountBuilder().generator(GeneratorBuilder().min(min).max(max))
        case _ => CountBuilder().records(DEFAULT_COUNT_RECORDS)
      }

      val perColumnNames = recordCountRequest.perColumnNames.getOrElse(List())
      if (perColumnNames.nonEmpty) {
        (recordCountRequest.perColumnRecords, recordCountRequest.perColumnRecordsMin, recordCountRequest.perColumnRecordsMax) match {
          case (Some(records), None, None) => baseRecordCount.recordsPerColumn(records, perColumnNames: _*)
          case (None, Some(min), Some(max)) => baseRecordCount.recordsPerColumnGenerator(GeneratorBuilder().min(min).max(max), perColumnNames: _*)
          case _ => baseRecordCount.recordsPerColumn(DEFAULT_PER_COLUMN_COUNT_RECORDS, perColumnNames: _*)
        }
      } else {
        baseRecordCount
      }
    }).getOrElse(CountBuilder())

    val mappedValidations = dataSourceRequest.validations.map(validationRequest => {
      validationRequest.validations.map(validateItem => {
        validateItem.`type` match {
          case VALIDATION_COLUMN =>
            //map type of column validation to builder method
            //each option is a new validation
            val mappedValids = validateItem.options.map(opts => {
              val colName = opts(VALIDATION_COLUMN)
              opts.filter(o => !VALIDATION_SUPPORTING_OPTIONS.contains(o._1)).map(opt => {
                val baseValid = ValidationBuilder().col(colName)
                opt._1 match {
                  case VALIDATION_EQUAL => baseValid.isEqualCol(opt._2)
                  case VALIDATION_NOT_EQUAL => baseValid.isNotEqualCol(opt._2)
                  case VALIDATION_NULL => baseValid.isNull
                  case VALIDATION_NOT_NULL => baseValid.isNotNull
                  case VALIDATION_CONTAINS => baseValid.contains(opt._2)
                  case VALIDATION_NOT_CONTAINS => baseValid.notContains(opt._2)
                  case VALIDATION_UNIQUE => ValidationBuilder().unique(colName)
                  case VALIDATION_LESS_THAN => baseValid.lessThan(opt._2)
                  case VALIDATION_LESS_THAN_OR_EQUAL => baseValid.lessThanOrEqual(opt._2)
                  case VALIDATION_GREATER_THAN => baseValid.greaterThan(opt._2)
                  case VALIDATION_GREATER_THAN_OR_EQUAL => baseValid.greaterThanOrEqual(opt._2)
                  case VALIDATION_BETWEEN =>
                    val min = opts(VALIDATION_MIN)
                    val max = opts(VALIDATION_MAX)
                    baseValid.betweenCol(min, max)
                  case VALIDATION_NOT_BETWEEN =>
                    val min = opts(VALIDATION_MIN)
                    val max = opts(VALIDATION_MAX)
                    baseValid.notBetweenCol(min, max)
                  case VALIDATION_IN => baseValid.in(opt._2.split(VALIDATION_IN_DELIMITER): _*)
                  case VALIDATION_MATCHES => baseValid.matches(opt._2)
                  case VALIDATION_NOT_MATCHES => baseValid.notMatches(opt._2)
                  case VALIDATION_STARTS_WITH => baseValid.startsWith(opt._2)
                  case VALIDATION_NOT_STARTS_WITH => baseValid.notStartsWith(opt._2)
                  case VALIDATION_ENDS_WITH => baseValid.endsWith(opt._2)
                  case VALIDATION_NOT_ENDS_WITH => baseValid.notEndsWith(opt._2)
                  case VALIDATION_SIZE => baseValid.size(opt._2.toInt)
                  case VALIDATION_NOT_SIZE => baseValid.notSize(opt._2.toInt)
                  case VALIDATION_LESS_THAN_SIZE => baseValid.lessThanSize(opt._2.toInt)
                  case VALIDATION_LESS_THAN_OR_EQUAL_SIZE => baseValid.lessThanOrEqualSize(opt._2.toInt)
                  case VALIDATION_GREATER_THAN_SIZE => baseValid.greaterThanSize(opt._2.toInt)
                  case VALIDATION_GREATER_THAN_OR_EQUAL_SIZE => baseValid.greaterThanOrEqualSize(opt._2.toInt)
                  case VALIDATION_LUHN_CHECK => baseValid.luhnCheck
                  case VALIDATION_HAS_TYPE => baseValid.hasType(opt._2)
                  case VALIDATION_SQL => baseValid.expr(opt._2)
                  case _ => baseValid.isNotNull
                }
              }).toList
            }).getOrElse(List())
            mappedValids
          case VALIDATION_COLUMN_NAMES =>
          case VALIDATION_UPSTREAM =>
          case VALIDATION_GROUP_BY =>
          case _ =>
        }
      })
    })

    baseConnection.schema(mappedFields: _*).count(countBuilder)
  }

  private def createFileConnection(dataSourceRequest: DataSourceRequest, format: String): FileBuilder = {
    val opt = dataSourceRequest.options.getOrElse(Map())
    checkOptions(dataSourceRequest.name, List(PATH), opt)
    ConnectionConfigWithTaskBuilder().file(dataSourceRequest.name, format, opt(PATH), opt)
  }

  private def createJdbcConnection(dataSourceRequest: DataSourceRequest, format: String): JdbcBuilder[_] = {
    val opt = dataSourceRequest.options.getOrElse(Map())
    checkOptions(dataSourceRequest.name, List(URL, USERNAME, PASSWORD), opt)
    val connectionConfigWithTaskBuilder = ConnectionConfigWithTaskBuilder()

    val baseConnection = format match {
      case POSTGRES => connectionConfigWithTaskBuilder.postgres(dataSourceRequest.name, opt(URL), opt(USERNAME), opt(PASSWORD), opt)
      case MYSQL => connectionConfigWithTaskBuilder.mysql(dataSourceRequest.name, opt(URL), opt(USERNAME), opt(PASSWORD), opt)
      case x => throw new IllegalArgumentException(s"Unsupported connection format, format=$x")
    }

    (opt.get(SCHEMA), opt.get(TABLE)) match {
      case (Some(schema), Some(table)) => baseConnection.table(schema, table)
      case (Some(schema), None) =>
        assert(schema.nonEmpty, s"Empty schema name for $format connection, data-source-name=${dataSourceRequest.name}")
        throw new IllegalArgumentException(s"Missing table name for $format connection, data-source-name=${dataSourceRequest.name}, schema=$schema")
      case (None, Some(table)) =>
        assert(table.nonEmpty, s"Empty table name for $format connection, data-source-name=${dataSourceRequest.name}")
        throw new IllegalArgumentException(s"Missing schema name for $format connection, data-source-name=${dataSourceRequest.name}, table=$table")
      case (None, None) => baseConnection // TODO this is allowed only when there is metadata collection enabled
    }
  }

  private def createCassandraConnection(dataSourceRequest: DataSourceRequest) = {
    val opt = dataSourceRequest.options.getOrElse(Map())
    checkOptions(dataSourceRequest.name, List(URL, USERNAME, PASSWORD), opt)

    val cassandraConnection = ConnectionConfigWithTaskBuilder().cassandra(dataSourceRequest.name, opt(URL), opt(USERNAME), opt(PASSWORD), opt)
    (opt.get(CASSANDRA_KEYSPACE), opt.get(CASSANDRA_TABLE)) match {
      case (Some(keyspace), Some(table)) => cassandraConnection.table(keyspace, table)
      case (Some(keyspace), None) =>
        assert(keyspace.nonEmpty, s"Empty keyspace name for Cassandra connection, data-source-name=${dataSourceRequest.name}")
        throw new IllegalArgumentException(s"Missing table name for Cassandra connection, data-source-name=${dataSourceRequest.name}, keyspace=$keyspace")
      case (None, Some(table)) =>
        assert(table.nonEmpty, s"Empty table name for Cassandra connection, data-source-name=${dataSourceRequest.name}")
        throw new IllegalArgumentException(s"Missing keyspace name for Cassandra connection, data-source-name=${dataSourceRequest.name}, table=$table")
      case (None, None) => cassandraConnection // TODO this is allowed only when there is metadata collection enabled
    }
  }

  private def checkOptions(dataSourceName: String, requiredOptions: List[String], options: Map[String, String]): Unit = {
    requiredOptions.foreach(opt =>
      assert(
        options.contains(opt) && options(opt).nonEmpty,
        s"Data source missing required configuration or is empty, data-source-name=$dataSourceName, config=$opt"
      )
    )
  }

}
