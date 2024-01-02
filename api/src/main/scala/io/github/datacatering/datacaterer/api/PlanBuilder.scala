package io.github.datacatering.datacaterer.api

import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api
import io.github.datacatering.datacaterer.api.converter.Converters.toScalaList
import io.github.datacatering.datacaterer.api.model.Constants.METADATA_SOURCE_TYPE
import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder
import io.github.datacatering.datacaterer.api.model.{ForeignKeyRelation, Plan}

import scala.annotation.varargs

case class PlanBuilder(plan: Plan = Plan(), tasks: List[TasksBuilder] = List()) {
  def this() = this(Plan(), List())

  def name(name: String): PlanBuilder =
    this.modify(_.plan.name).setTo(name)

  def description(desc: String): PlanBuilder =
    this.modify(_.plan.description).setTo(desc)

  def taskSummaries(taskSummaries: TaskSummaryBuilder*): PlanBuilder = {
    val tasksToAdd = taskSummaries.filter(_.task.isDefined)
      .map(x => TasksBuilder(List(x.task.get), x.taskSummary.dataSourceName))
      .toList
    this.modify(_.plan.tasks)(_ ++ taskSummaries.map(_.taskSummary))
      .modify(_.tasks)(_ ++ tasksToAdd)
  }

  def sinkOptions(sinkOptionsBuilder: SinkOptionsBuilder): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(sinkOptionsBuilder.sinkOptions))

  def seed(seed: Long): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.seed(seed).sinkOptions))

  def locale(locale: String): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.locale(locale).sinkOptions))

  @varargs def addForeignKeyRelationship(foreignKey: ForeignKeyRelation, relations: ForeignKeyRelation*): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.foreignKey(foreignKey, relations.toList).sinkOptions))

  def addForeignKeyRelationship(connectionTaskBuilder: ConnectionTaskBuilder[_], columns: List[String],
                                relations: List[(ConnectionTaskBuilder[_], List[String])]): PlanBuilder = {
    val baseRelation = toForeignKeyRelation(connectionTaskBuilder, columns)
    val otherRelations = relations.map(r => toForeignKeyRelation(r._1, r._2))
    addForeignKeyRelationship(baseRelation, otherRelations: _*)
  }

  def addForeignKeyRelationship(connectionTaskBuilder: ConnectionTaskBuilder[_], columns: java.util.List[String],
                                relations: java.util.List[java.util.Map.Entry[ConnectionTaskBuilder[_], java.util.List[String]]]): PlanBuilder = {
    val scalaListRelations = toScalaList(relations)
    val mappedRelations = scalaListRelations.map(r => (r.getKey, toScalaList(r.getValue)))
    addForeignKeyRelationship(connectionTaskBuilder, toScalaList(columns), mappedRelations)
  }

  def addForeignKeyRelationship(connectionTaskBuilder: ConnectionTaskBuilder[_], column: String,
                                relations: List[(ConnectionTaskBuilder[_], String)]): PlanBuilder =
    addForeignKeyRelationship(connectionTaskBuilder, List(column), relations.map(r => (r._1, List(r._2))))

  def addForeignKeyRelationship(connectionTaskBuilder: ConnectionTaskBuilder[_], column: String,
                                relations: java.util.List[java.util.Map.Entry[ConnectionTaskBuilder[_], String]]): PlanBuilder = {
    val scalaListRelations = toScalaList(relations)
    val mappedRelations = scalaListRelations.map(r => (r.getKey, List(r.getValue)))
    addForeignKeyRelationship(connectionTaskBuilder, List(column), mappedRelations)
  }

  def addForeignKeyRelationships(connectionTaskBuilder: ConnectionTaskBuilder[_], columns: List[String],
                                 relations: List[ForeignKeyRelation]): PlanBuilder = {
    val baseRelation = toForeignKeyRelation(connectionTaskBuilder, columns)
    addForeignKeyRelationship(baseRelation, relations: _*)
  }

  def addForeignKeyRelationships(connectionTaskBuilder: ConnectionTaskBuilder[_], columns: java.util.List[String],
                                 relations: java.util.List[ForeignKeyRelation]): PlanBuilder =
    addForeignKeyRelationships(connectionTaskBuilder, toScalaList(columns), toScalaList(relations))

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation,
                                relations: List[(ConnectionTaskBuilder[_], List[String])]): PlanBuilder =
    addForeignKeyRelationship(foreignKey, relations.map(r => toForeignKeyRelation(r._1, r._2)): _*)

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation,
                                relations: java.util.List[(ConnectionTaskBuilder[_], java.util.List[String])]): PlanBuilder =
    addForeignKeyRelationship(foreignKey, toScalaList(relations).map(r => toForeignKeyRelation(r._1, toScalaList(r._2))): _*)

  private def toForeignKeyRelation(connectionTaskBuilder: ConnectionTaskBuilder[_], columns: List[String]) = {
    val dataSource = connectionTaskBuilder.connectionConfigWithTaskBuilder.dataSourceName
    val colNames = columns.mkString(",")
    connectionTaskBuilder.step match {
      case Some(value) =>
        val fields = value.step.schema.fields.getOrElse(List())
        val hasColumns = columns.forall(c => fields.exists(_.name == c))
        if (!hasColumns && !value.step.options.contains(METADATA_SOURCE_TYPE)) {
          throw new RuntimeException(s"Column name defined in foreign key relationship does not exist, data-source=$dataSource, column-name=$colNames")
        }
        ForeignKeyRelation(dataSource, value.step.name, columns)
      case None =>
        throw new RuntimeException(s"No schema defined for data source. Cannot create foreign key relationship, data-source=$dataSource, column-name=$colNames")
    }
  }

  private def getSinkOpt: SinkOptionsBuilder = {
    plan.sinkOptions match {
      case Some(value) => api.SinkOptionsBuilder(value)
      case None => SinkOptionsBuilder()
    }
  }
}
