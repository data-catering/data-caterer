package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.{BasePlanRun, DataCatererConfigurationBuilder, PlanRun}
import io.github.datacatering.datacaterer.core.ui.mapper.ConfigurationMapper.configurationMapping
import io.github.datacatering.datacaterer.core.ui.mapper.ConnectionMapper.connectionMapping
import io.github.datacatering.datacaterer.core.ui.mapper.CountMapper.countMapping
import io.github.datacatering.datacaterer.core.ui.mapper.FieldMapper.fieldMapping
import io.github.datacatering.datacaterer.core.ui.mapper.ForeignKeyMapper.foreignKeyMapping
import io.github.datacatering.datacaterer.core.ui.mapper.ValidationMapper.{connectionsWithUpstreamValidationMapping, validationMapping}
import io.github.datacatering.datacaterer.core.ui.model.{DataSourceRequest, PlanRunRequest}

object UiMapper {

  def mapToPlanRun(planRunRequest: PlanRunRequest, installDirectory: String): PlanRun = {
    val plan = new BasePlanRun()
    val planBuilder = plan.plan.name(planRunRequest.name).runId(planRunRequest.id)
    val connections = planRunRequest.dataSources.map(dataSourceToConnection)
    val configuration = planRunRequest.configuration
      .map(c => configurationMapping(c, installDirectory, connections))
      .getOrElse(DataCatererConfigurationBuilder())
    val planBuilderWithForeignKeys = foreignKeyMapping(planRunRequest.foreignKeys, connections, planBuilder)
    // after initial connection mapping, need to get the upstream validations based on the connection mapping
    val connectionsWithUpstreamValidations = connectionsWithUpstreamValidationMapping(connections, planRunRequest.dataSources)

    if (connectionsWithUpstreamValidations.isEmpty) {
      throw new IllegalArgumentException("Have to define at least one data source to run the plan")
    }
    plan.execute(planBuilderWithForeignKeys, configuration, connectionsWithUpstreamValidations.head, connectionsWithUpstreamValidations.tail: _*)
    plan
  }

  private def dataSourceToConnection(dataSourceRequest: DataSourceRequest): ConnectionTaskBuilder[_] = {
    val baseConnection = connectionMapping(dataSourceRequest)
    val mappedFields = fieldMapping(dataSourceRequest)
    val countBuilder = countMapping(dataSourceRequest)
    val mappedValidations = validationMapping(dataSourceRequest)

    val baseConnectionWithSchema = mappedFields._1.map(baseConnection.schema)
      .getOrElse(mappedFields._2.map(f => baseConnection.schema(f: _*)).getOrElse(baseConnection))
    val enableDataGenFromOpts = dataSourceRequest.options.forall(opts => opts.getOrElse(ENABLE_DATA_GENERATION, "true").toBoolean)

    baseConnectionWithSchema
      .name(dataSourceRequest.taskName)
      .count(countBuilder)
      .validations(mappedValidations: _*)
      .enableDataGeneration(enableDataGenFromOpts || mappedFields._1.nonEmpty || mappedFields._2.nonEmpty)
  }

  def checkOptions(dataSourceName: String, requiredOptions: List[String], options: Map[String, String]): Unit = {
    requiredOptions.foreach(opt =>
      assert(
        options.contains(opt) && options(opt).nonEmpty,
        s"Data source missing required configuration or is empty, data-source-name=$dataSourceName, config=$opt"
      )
    )
  }
}
