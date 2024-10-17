package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.PlanBuilder
import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_STEP_NAME, VALIDATION_OPTION_DELIMITER}
import io.github.datacatering.datacaterer.api.model.ForeignKeyRelation
import io.github.datacatering.datacaterer.core.exception.MissingConnectionForForeignKeyException
import io.github.datacatering.datacaterer.core.generator.metadata.StepNameProvider
import io.github.datacatering.datacaterer.core.ui.model.{ForeignKeyRequest, ForeignKeyRequestItem}

object ForeignKeyMapper {

  def foreignKeyMapping(foreignKeyRequests: List[ForeignKeyRequest], connections: List[ConnectionTaskBuilder[_]], planBuilder: PlanBuilder): PlanBuilder = {
    val mappedWithConnections = foreignKeyRequests.map(fkr => {
      val sourceFk = mapToForeignKeyRelation(connections, fkr.source.get)
      val generationLinkConnections = mapForeignKeyLinksToRelations(connections, fkr.generationLinks)
      val deleteLinkConnections = mapForeignKeyLinksToRelations(connections, fkr.deleteLinks)

      (sourceFk, generationLinkConnections, deleteLinkConnections)
    })
    mappedWithConnections.foldLeft(planBuilder)((pb, fk) => pb.addForeignKeyRelationship(fk._1, fk._2, fk._3))
  }

  private def mapToForeignKeyRelation(connections: List[ConnectionTaskBuilder[_]], foreignKeyRequestItem: ForeignKeyRequestItem): ForeignKeyRelation = {
    val connection = getConnectionByTaskName(connections, foreignKeyRequestItem.taskName)
    val columns = foreignKeyRequestItem.columns.split(VALIDATION_OPTION_DELIMITER).toList
    val dataSourceName = connection.connectionConfigWithTaskBuilder.dataSourceName
    //if there are options defined in the foreign key request item, it needs to be used to define the step name
    //since if a metadata source is used to generate sub data sources, the options are used to define the step name
    //i.e. read schema from OpenAPI doc and step name becomes 'GET/my-path'
    val overrideOptions = foreignKeyRequestItem.options.getOrElse(Map())
    val baseOptions = connection.connectionConfigWithTaskBuilder.options
    val stepName = StepNameProvider.fromOptions(baseOptions ++ overrideOptions).getOrElse(connection.step.map(_.step.name).getOrElse(DEFAULT_STEP_NAME))
    ForeignKeyRelation(dataSourceName, stepName, columns)
  }

  private def mapForeignKeyLinksToRelations(connections: List[ConnectionTaskBuilder[_]], links: List[ForeignKeyRequestItem]): List[ForeignKeyRelation] = {
    links.map(link => mapToForeignKeyRelation(connections, link))
  }

  private def getConnectionByTaskName(connections: List[ConnectionTaskBuilder[_]], taskName: String): ConnectionTaskBuilder[_] = {
    val matchingConnection = connections.find(c => c.task.exists(taskBuilder => taskBuilder.task.name == taskName))
    matchingConnection match {
      case Some(value) => value
      case None => throw MissingConnectionForForeignKeyException(taskName)
    }
  }

}
