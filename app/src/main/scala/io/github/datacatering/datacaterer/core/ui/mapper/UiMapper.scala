package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.connection.{ConnectionTaskBuilder, FileBuilder, JdbcBuilder}
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.DataType
import io.github.datacatering.datacaterer.api.{BasePlanRun, ColumnNamesValidationBuilder, ColumnValidationBuilder, ConnectionConfigWithTaskBuilder, CountBuilder, DataCatererConfigurationBuilder, FieldBuilder, GeneratorBuilder, GroupByValidationBuilder, PlanBuilder, PlanRun, ValidationBuilder}
import io.github.datacatering.datacaterer.core.ui.model.{DataSourceRequest, ForeignKeyRequest, PlanRunRequest, ValidationItemRequest, ValidationItemRequests}
import org.apache.log4j.Logger

object UiMapper {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def mapToPlanRun(planRunRequest: PlanRunRequest): PlanRun = {
    val plan = new BasePlanRun()
    val planBuilder = plan.plan.name(planRunRequest.name)
    val configuration = DataCatererConfigurationBuilder()
    val connections = planRunRequest.dataSources.map(dataSourceToConnection)
    val planBuilderWithForeignKeys = foreignKeyMapping(planRunRequest.foreignKeys, connections, planBuilder)
    // after initial connection mapping, need to get the upstream validations based on the connection mapping
    val connectionsWithUpstreamValidations = connectionsWithUpstreamValidationMapping(connections, planRunRequest.dataSources)

    plan.execute(planBuilderWithForeignKeys, configuration, connectionsWithUpstreamValidations.head, connectionsWithUpstreamValidations.tail: _*)
    plan
  }

  def dataSourceToConnection(dataSourceRequest: DataSourceRequest): ConnectionTaskBuilder[_] = {
    val baseConnection = connectionMapping(dataSourceRequest)
    val mappedFields = fieldMapping(dataSourceRequest)
    val countBuilder = countMapping(dataSourceRequest)
    val mappedValidations = validationMapping(dataSourceRequest)

    baseConnection
      .name(dataSourceRequest.taskName)
      .schema(mappedFields: _*)
      .count(countBuilder)
      .validations(mappedValidations: _*)
      .enableDataGeneration(mappedFields.nonEmpty)
      .enableDataValidation(mappedValidations.nonEmpty)
  }

  def foreignKeyMapping(foreignKeyRequests: List[ForeignKeyRequest], connections: List[ConnectionTaskBuilder[_]], planBuilder: PlanBuilder): PlanBuilder = {
    val mappedWithConnections = foreignKeyRequests.map(fkr => {
      val sourceConnection = getConnectionByTaskName(connections, fkr.source.get.taskName)
      val sourceColumns = fkr.source.get.columns.split(VALIDATION_OPTION_DELIMITER).toList
      val linkConnections = fkr.links.map(link => {
        val matchingConnection = getConnectionByTaskName(connections, link.taskName)
        (matchingConnection, link.columns.split(VALIDATION_OPTION_DELIMITER).toList)
      })

      (sourceConnection, sourceColumns, linkConnections)
    })
    mappedWithConnections.foldLeft(planBuilder)((pb, fk) => pb.addForeignKeyRelationship(fk._1, fk._2, fk._3))
  }

  private def getConnectionByTaskName(connections: List[ConnectionTaskBuilder[_]], taskName: String): ConnectionTaskBuilder[_] = {
    val matchingConnection = connections.find(c => c.task.exists(taskBuilder => taskBuilder.task.name == taskName))
    matchingConnection match {
      case Some(value) => value
      case None => throw new RuntimeException(s"No connection found with matching task name, task-name=$taskName")
    }
  }

  private def validationMapping(dataSourceRequest: DataSourceRequest): List[ValidationBuilder] = {
    dataSourceRequest.validations
      .map(validations => validations.flatMap(validationItemRequestToValidationBuilders))
      .getOrElse(List())
  }

  private def validationItemRequestToValidationBuilders(validateItem: ValidationItemRequest): List[ValidationBuilder] = {
    validateItem.`type` match {
      case VALIDATION_COLUMN =>
        //map type of column validation to builder method
        //each option is a new validation
        val mappedValids = validateItem.options.map(opts => {
          val colName = opts(VALIDATION_COLUMN)
          opts
            .filter(o => !VALIDATION_SUPPORTING_OPTIONS.contains(o._1))
            .map(opt => {
              val baseValid = ValidationBuilder().col(colName)
              columnValidationMapping(baseValid, opts, colName, opt)
            })
            .toList
        }).getOrElse(List())
        mappedValids
      case VALIDATION_COLUMN_NAMES =>
        val baseValid = ValidationBuilder().columnNames
        validateItem.options.map(opts => {
          opts
            .filter(o => !VALIDATION_SUPPORTING_OPTIONS.contains(o._1))
            .map(opt => columnNamesValidationMapping(baseValid, opts, opt))
            .toList
        }).getOrElse(List())
      case VALIDATION_UPSTREAM =>
        // require upstream ConnectionTaskBuilder
        List()
      case VALIDATION_GROUP_BY =>
        validateItem.options.map(opts => {
          val groupByCols = opts(VALIDATION_GROUP_BY_COLUMNS).split(VALIDATION_OPTION_DELIMITER)
          val baseValid = ValidationBuilder().groupBy(groupByCols: _*)
          groupByValidationMapping(baseValid, validateItem.nested)
        }).getOrElse(List())
      case _ => List()
    }
  }

  private def countMapping(dataSourceRequest: DataSourceRequest): CountBuilder = {
    dataSourceRequest.count.map(recordCountRequest => {
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
  }

  private def fieldMapping(dataSourceRequest: DataSourceRequest): List[FieldBuilder] = {
    dataSourceRequest.fields.getOrElse(List()).map(field => {
      assert(field.name.nonEmpty, s"Field name cannot be empty, data-source-name=${dataSourceRequest.name}")
      assert(field.`type`.nonEmpty, s"Field type cannot be empty, data-source-name=${dataSourceRequest.name}, field-name=${field.name}")
      FieldBuilder().name(field.name).`type`(DataType.fromString(field.`type`)).options(field.options.getOrElse(Map()))
    })
  }

  private def connectionMapping(dataSourceRequest: DataSourceRequest): ConnectionTaskBuilder[_] = {
    dataSourceRequest.`type` match {
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
  }

  private def connectionsWithUpstreamValidationMapping(connections: List[ConnectionTaskBuilder[_]], dataSources: List[DataSourceRequest]): List[ConnectionTaskBuilder[_]] = {
    val dataSourcesWithUpstreamValidation = dataSources.filter(ds => ds.validations.getOrElse(List()).exists(_.`type` == VALIDATION_UPSTREAM))
      .map(ds => (ds.name, ds.validations.getOrElse(List())))
      .toMap
    dataSourcesWithUpstreamValidation.map(ds => {
      val optConnection = connections.find(c => c.task.exists(_.task.name == ds._1))
      optConnection match {
        case Some(connection) =>
          val upstreamValidations = ds._2.filter(_.`type` == VALIDATION_UPSTREAM)
          val mappedValidations = upstreamValidationMapping(connection, upstreamValidations)
          val allValidations = connection.getValidations ++ mappedValidations
          connection.validations(allValidations: _*)
        case None =>

      }
    })
    connections.map(connection => {
      val connectionTaskName = connection.task.map(_.task.name).getOrElse("")
      val optDataSourceWithUpstreamValidation = dataSourcesWithUpstreamValidation.get(connectionTaskName)
      optDataSourceWithUpstreamValidation match {
        case Some(value) =>
          val upstreamValidations = value.filter(_.`type` == VALIDATION_UPSTREAM)
          val mappedValidations = upstreamValidationMapping(connection, upstreamValidations)
          val allValidations = connection.getValidations ++ mappedValidations
          connection.validations(allValidations: _*)
        case None =>
          LOGGER.debug(s"Task does not have any upstream validations defined, task-name=$connectionTaskName")
          connection
      }
    })
  }

  private def upstreamValidationMapping(connection: ConnectionTaskBuilder[_], upstreamValidations: List[ValidationItemRequest]): List[ValidationBuilder] = {
    upstreamValidations.map(upstreamValidation => {
      val baseValid = ValidationBuilder().upstreamData(connection)

      // check for join options
      def getOption(k: String): Option[String] = upstreamValidation.options.flatMap(_.get(k))

      val joinValidation = (getOption(VALIDATION_UPSTREAM_JOIN_TYPE), getOption(VALIDATION_UPSTREAM_JOIN_COLUMNS), getOption(VALIDATION_UPSTREAM_JOIN_EXPR)) match {
        case (Some(joinType), Some(joinCols), _) => baseValid.joinType(joinType).joinColumns(joinCols.split(","): _*)
        case (Some(joinType), None, Some(joinExpr)) => baseValid.joinType(joinType).joinExpr(joinExpr)
        case (None, Some(joinCols), _) => baseValid.joinType(DEFAULT_VALIDATION_JOIN_TYPE).joinColumns(joinCols.split(","): _*)
        case (None, None, Some(joinExpr)) => baseValid.joinType(DEFAULT_VALIDATION_JOIN_TYPE).joinExpr(joinExpr)
        case _ => throw new RuntimeException("Unexpected upstream validation join options, need to define join columns or expression")
      }
      val upstreamWithValidations = upstreamValidation.nested.map(nest =>
        nest.validations.flatMap(nestedValidation => {
          validationItemRequestToValidationBuilders(nestedValidation)
            .map(joinValidation.withValidation)
        })
      ).getOrElse(List())
      upstreamWithValidations
    })
    List()
  }

  private def groupByValidationMapping(baseValid: GroupByValidationBuilder, optNestedValidations: Option[ValidationItemRequests]) = {
    optNestedValidations.map(validationReqs => {
      validationReqs.validations.flatMap(validationReq => {
        // only column validations can be applied after group by
        validationReq.options.map(opts => {
          // check for aggType and aggCol
          (opts.get("aggType"), opts.get("aggCol")) match {
            case (Some(aggType), Some(aggCol)) =>
              val aggregateValidation = aggType match {
                case VALIDATION_MIN => baseValid.min(aggCol)
                case VALIDATION_MAX => baseValid.max(aggCol)
                case VALIDATION_COUNT => baseValid.count(aggCol)
                case VALIDATION_SUM => baseValid.sum(aggCol)
                case VALIDATION_AVERAGE => baseValid.avg(aggCol)
                case VALIDATION_STANDARD_DEVIATION => baseValid.stddev(aggCol)
                case _ => throw new RuntimeException(s"Unexpected aggregation type found in group by validation, aggregation-type=$aggType")
              }
              opts.filter(o => o._1 != "aggType" && o._1 != "aggCol")
                .map(opt => columnValidationMapping(aggregateValidation, opts, opt._2, opt))
                .toList
            case _ => throw new RuntimeException("Keys 'aggType' and 'aggCol' are expected when defining a group by validation")
          }
        }).getOrElse(List())
      })
    }).getOrElse(List())
  }

  private def columnNamesValidationMapping(baseValid: ColumnNamesValidationBuilder, opts: Map[String, String], opt: (String, String)) = {
    opt._1 match {
      case VALIDATION_COLUMN_NAMES_COUNT_EQUAL => baseValid.countEqual(opt._2.toInt)
      case VALIDATION_COLUMN_NAMES_COUNT_BETWEEN =>
        val min = opts(VALIDATION_MIN)
        val max = opts(VALIDATION_MAX)
        baseValid.countBetween(min.toInt, max.toInt)
      case VALIDATION_COLUMN_NAMES_MATCH_ORDER => baseValid.matchOrder(opt._2.split(VALIDATION_OPTION_DELIMITER): _*)
      case VALIDATION_COLUMN_NAMES_MATCH_SET => baseValid.matchSet(opt._2.split(VALIDATION_OPTION_DELIMITER): _*)
      case _ => baseValid.countEqual(1)
    }
  }

  private def columnValidationMapping(baseValid: ColumnValidationBuilder, opts: Map[String, String], colName: String, opt: (String, String)) = {
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
      case VALIDATION_IN => baseValid.in(opt._2.split(VALIDATION_OPTION_DELIMITER): _*)
      case VALIDATION_NOT_IN => baseValid.notIn(opt._2.split(VALIDATION_OPTION_DELIMITER): _*)
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
