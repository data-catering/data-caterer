package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.{ColumnNamesValidationBuilder, ColumnValidationBuilder, GroupByValidationBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.ui.model.{DataSourceRequest, ValidationItemRequest, ValidationItemRequests}
import org.apache.log4j.Logger

object ValidationMapper {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def validationMapping(dataSourceRequest: DataSourceRequest): List[ValidationBuilder] = {
    dataSourceRequest.validations
      .map(validations => validations.optValidations.map(v => v.flatMap(validationItemRequestToValidationBuilders)).getOrElse(List()))
      .getOrElse(List())
  }

  private def validationItemRequestToValidationBuilders(validateItem: ValidationItemRequest): List[ValidationBuilder] = {
    validateItem.`type` match {
      case VALIDATION_COLUMN =>
        //map type of column validation to builder method
        //each option is a new validation
        validateItem.options.map(opts => {
          val colName = opts(VALIDATION_FIELD)
          opts
            .filter(o => !VALIDATION_SUPPORTING_OPTIONS.contains(o._1))
            .map(opt => {
              val baseValid = validationWithDescriptionAndErrorThreshold(opts).col(colName)
              columnValidationMapping(baseValid, opts, colName, opt)
            })
            .toList
        }).getOrElse(List())
      case VALIDATION_COLUMN_NAMES =>
        validateItem.options.map(opts => {
          opts
            .filter(o => !VALIDATION_SUPPORTING_OPTIONS.contains(o._1))
            .map(opt => {
              val baseValid = validationWithDescriptionAndErrorThreshold(opts).columnNames
              columnNamesValidationMapping(baseValid, opts, opt)
            })
            .toList
        }).getOrElse(List())
      case VALIDATION_UPSTREAM =>
        // require upstream ConnectionTaskBuilder
        List()
      case VALIDATION_GROUP_BY =>
        validateItem.options.map(opts => {
          val groupByCols = opts(VALIDATION_GROUP_BY_COLUMNS).split(VALIDATION_OPTION_DELIMITER)
          val baseValid = validationWithDescriptionAndErrorThreshold(opts).groupBy(groupByCols: _*)
          groupByValidationMapping(baseValid, validateItem.nested)
        }).getOrElse(List())
      case _ => List()
    }
  }

  private def validationWithDescriptionAndErrorThreshold(options: Map[String, String]): ValidationBuilder = {
    val optDescription = options.get(VALIDATION_DESCRIPTION)
    val optErrorThreshold = options.get(VALIDATION_ERROR_THRESHOLD)
    val baseValidation = ValidationBuilder()
    val validWithDesc = optDescription.map(desc => baseValidation.description(desc)).getOrElse(baseValidation)
    optErrorThreshold.map(error => validWithDesc.errorThreshold(error.toDouble)).getOrElse(validWithDesc)
  }

  def connectionsWithUpstreamValidationMapping(connections: List[ConnectionTaskBuilder[_]], dataSources: List[DataSourceRequest]): List[ConnectionTaskBuilder[_]] = {
    val dataSourcesWithUpstreamValidation = dataSources
      .filter(ds => {
        ds.validations
          .map(_.optValidations.getOrElse(List())).getOrElse(List())
          .exists(_.`type` == VALIDATION_UPSTREAM)
      })
      .map(ds => (ds.taskName, ds.validations.map(_.optValidations.get).getOrElse(List())))
      .toMap

    connections.map(connection => {
      val connectionTaskName = connection.task.map(_.task.name).getOrElse("")
      val optDataSourceWithUpstreamValidation = dataSourcesWithUpstreamValidation.get(connectionTaskName)
      optDataSourceWithUpstreamValidation match {
        case Some(value) =>
          val upstreamValidations = value.filter(_.`type` == VALIDATION_UPSTREAM)
          val mappedValidations = upstreamValidationMapping(connections, upstreamValidations)
          val allValidations = connection.getValidations ++ mappedValidations
          connection.validations(allValidations: _*).enableDataValidation(allValidations.nonEmpty)
        case None =>
          LOGGER.debug(s"Task does not have any upstream validations defined, task-name=$connectionTaskName")
          connection
      }
    })
  }

  private def upstreamValidationMapping(connections: List[ConnectionTaskBuilder[_]], upstreamValidations: List[ValidationItemRequest]): List[ValidationBuilder] = {
    upstreamValidations.flatMap(upstreamValidation => {
      def getOption(k: String): Option[String] = upstreamValidation.options.flatMap(_.get(k))

      val upstreamTaskName = getOption(VALIDATION_UPSTREAM_TASK_NAME).getOrElse("")
      val upstreamConnection = connections.find(_.task.exists(_.task.name == upstreamTaskName))

      if (upstreamConnection.isDefined) {
        val baseValid = validationWithDescriptionAndErrorThreshold(upstreamValidation.options.getOrElse(Map())).upstreamData(upstreamConnection.get)

        // check for join options
        val joinValidation = (getOption(VALIDATION_UPSTREAM_JOIN_TYPE), getOption(VALIDATION_UPSTREAM_JOIN_COLUMNS), getOption(VALIDATION_UPSTREAM_JOIN_EXPR)) match {
          case (Some(joinType), Some(joinCols), _) => baseValid.joinType(joinType).joinColumns(joinCols.split(","): _*)
          case (Some(joinType), None, Some(joinExpr)) => baseValid.joinType(joinType).joinExpr(joinExpr)
          case (None, Some(joinCols), _) => baseValid.joinType(DEFAULT_VALIDATION_JOIN_TYPE).joinColumns(joinCols.split(","): _*)
          case (None, None, Some(joinExpr)) => baseValid.joinType(DEFAULT_VALIDATION_JOIN_TYPE).joinExpr(joinExpr)
          case _ => throw new IllegalArgumentException("Unexpected upstream validation join options, need to define join columns or expression")
        }
        val upstreamWithValidations = upstreamValidation.nested.map(nest =>
          nest.validations.flatMap(nestedValidation => {
            validationItemRequestToValidationBuilders(nestedValidation)
              .map(joinValidation.withValidation)
          })
        ).getOrElse(List())
        upstreamWithValidations
      } else {
        LOGGER.error(s"Validation upstream task name is not defined in task list, unable to execute upstream validations, " +
          s"validation-upstream-task-name=$upstreamTaskName")
        List()
      }
    })
  }

  private def groupByValidationMapping(baseValid: GroupByValidationBuilder, optNestedValidations: Option[ValidationItemRequests]) = {
    optNestedValidations.map(validationReqs => {
      validationReqs.validations.flatMap(validationReq => {
        // only column validations can be applied after group by
        validationReq.options.map(opts => {
          // check for aggType and aggCol
          (opts.get(VALIDATION_AGGREGATION_TYPE), opts.get(VALIDATION_AGGREGATION_COLUMN)) match {
            case (Some(aggType), Some(aggCol)) =>
              val aggregateValidation = aggType match {
                case VALIDATION_MIN => baseValid.min(aggCol)
                case VALIDATION_MAX => baseValid.max(aggCol)
                case VALIDATION_COUNT => baseValid.count(aggCol)
                case VALIDATION_SUM => baseValid.sum(aggCol)
                case VALIDATION_AVERAGE => baseValid.avg(aggCol)
                case VALIDATION_STANDARD_DEVIATION => baseValid.stddev(aggCol)
                case _ => throw new IllegalArgumentException(s"Unexpected aggregation type found in group by validation, aggregation-type=$aggType")
              }
              opts.filter(o => o._1 != VALIDATION_AGGREGATION_TYPE && o._1 != VALIDATION_AGGREGATION_COLUMN)
                .map(opt => columnValidationMapping(aggregateValidation, opts, opt._2, opt))
                .toList
            case _ => throw new IllegalArgumentException("Keys 'aggType' and 'aggCol' are expected when defining a group by validation")
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
      case _ =>
        LOGGER.warn("Unknown column name validation type, defaulting to column name count equal to 1")
        baseValid.countEqual(1)
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
      case VALIDATION_UNIQUE => validationWithDescriptionAndErrorThreshold(opts).unique(colName)
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
      case x => throw new IllegalArgumentException(s"Unsupported column validation, validation-key=$x")
    }
  }
}
