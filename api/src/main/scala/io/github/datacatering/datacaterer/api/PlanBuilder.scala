package io.github.datacatering.datacaterer.api

import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api
import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder
import io.github.datacatering.datacaterer.api.converter.Converters.toScalaList
import io.github.datacatering.datacaterer.api.model.Constants.{ENABLE_REFERENCE_MODE, METADATA_SOURCE_TYPE, YAML_PLAN}
import io.github.datacatering.datacaterer.api.model.{ForeignKeyRelation, Plan, YamlPlanSource}

import scala.annotation.varargs

case class PlanBuilder(plan: Plan = Plan(), tasks: List[TasksBuilder] = List()) {
  def this() = this(Plan(), List())

  def name(name: String): PlanBuilder =
    this.modify(_.plan.name).setTo(name)

  def description(desc: String): PlanBuilder =
    this.modify(_.plan.description).setTo(desc)

  def runId(runId: String): PlanBuilder =
    this.modify(_.plan.runId).setTo(Some(runId))

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

  @varargs def addForeignKeyRelationship(foreignKey: ForeignKeyRelation, generationLinks: ForeignKeyRelation*): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.foreignKey(foreignKey, generationLinks.toList).sinkOptions))

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation, generationLinks: List[ForeignKeyRelation], deleteLinks: List[ForeignKeyRelation]): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.foreignKey(foreignKey, generationLinks, deleteLinks).sinkOptions))

  def addForeignKeyRelationship(connectionTaskBuilder: ConnectionTaskBuilder[_], fields: List[String],
                                generationLinks: List[(ConnectionTaskBuilder[_], List[String])]): PlanBuilder = {
    val baseRelation = toForeignKeyRelation(connectionTaskBuilder, fields)
    val otherRelations = generationLinks.map(r => toForeignKeyRelation(r._1, r._2))
    addForeignKeyRelationship(baseRelation, otherRelations: _*)
  }

  def addForeignKeyRelationship(connectionTaskBuilder: ConnectionTaskBuilder[_], fields: List[String],
                                generationLinks: List[(ConnectionTaskBuilder[_], List[String])],
                                deleteLinks: List[(ConnectionTaskBuilder[_], List[String])],
                               ): PlanBuilder = {
    val baseRelation = toForeignKeyRelation(connectionTaskBuilder, fields)
    val mappedGeneration = generationLinks.map(r => toForeignKeyRelation(r._1, r._2))
    val mappedDelete = deleteLinks.map(r => toForeignKeyRelation(r._1, r._2, true))
    addForeignKeyRelationship(baseRelation, mappedGeneration, mappedDelete)
  }

  def addForeignKeyRelationship(connectionTaskBuilder: ConnectionTaskBuilder[_], fields: java.util.List[String],
                                relations: java.util.List[java.util.Map.Entry[ConnectionTaskBuilder[_], java.util.List[String]]]): PlanBuilder = {
    val scalaListRelations = toScalaList(relations)
    val mappedRelations = scalaListRelations.map(r => (r.getKey, toScalaList(r.getValue)))
    addForeignKeyRelationship(connectionTaskBuilder, toScalaList(fields), mappedRelations)
  }

  def addForeignKeyRelationship(connectionTaskBuilder: ConnectionTaskBuilder[_], field: String,
                                relations: List[(ConnectionTaskBuilder[_], String)]): PlanBuilder =
    addForeignKeyRelationship(connectionTaskBuilder, List(field), relations.map(r => (r._1, List(r._2))))

  def addForeignKeyRelationship(connectionTaskBuilder: ConnectionTaskBuilder[_], field: String,
                                relations: java.util.List[java.util.Map.Entry[ConnectionTaskBuilder[_], String]]): PlanBuilder = {
    val scalaListRelations = toScalaList(relations)
    val mappedRelations = scalaListRelations.map(r => (r.getKey, List(r.getValue)))
    addForeignKeyRelationship(connectionTaskBuilder, List(field), mappedRelations)
  }

  def addForeignKeyRelationships(connectionTaskBuilder: ConnectionTaskBuilder[_], fields: List[String],
                                 relations: List[ForeignKeyRelation]): PlanBuilder = {
    val baseRelation = toForeignKeyRelation(connectionTaskBuilder, fields)
    addForeignKeyRelationship(baseRelation, relations: _*)
  }

  def addForeignKeyRelationships(connectionTaskBuilder: ConnectionTaskBuilder[_], fields: java.util.List[String],
                                 relations: java.util.List[ForeignKeyRelation]): PlanBuilder =
    addForeignKeyRelationships(connectionTaskBuilder, toScalaList(fields), toScalaList(relations))

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation,
                                relations: List[(ConnectionTaskBuilder[_], List[String])]): PlanBuilder =
    addForeignKeyRelationship(foreignKey, relations.map(r => toForeignKeyRelation(r._1, r._2)): _*)

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation,
                                relations: java.util.List[(ConnectionTaskBuilder[_], java.util.List[String])]): PlanBuilder =
    addForeignKeyRelationship(foreignKey, toScalaList(relations).map(r => toForeignKeyRelation(r._1, toScalaList(r._2))): _*)

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation,
                                generationLinks: List[ForeignKeyRelation],
                                cardinality: CardinalityConfigBuilder): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.foreignKey(foreignKey, generationLinks, cardinality).sinkOptions))

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation,
                                generationLinks: List[ForeignKeyRelation],
                                nullability: NullabilityConfigBuilder): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.foreignKey(foreignKey, generationLinks, nullability).sinkOptions))

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation,
                                generationLinks: List[ForeignKeyRelation],
                                cardinality: CardinalityConfigBuilder,
                                nullability: NullabilityConfigBuilder): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.foreignKey(foreignKey, generationLinks, cardinality, nullability).sinkOptions))

  private def toForeignKeyRelation(connectionTaskBuilder: ConnectionTaskBuilder[_], fields: List[String], isDeleteFk: Boolean = false): ForeignKeyRelation = {
    val dataSource = connectionTaskBuilder.connectionConfigWithTaskBuilder.dataSourceName
    val fieldNames = fields.mkString(",")
    val referenceModeEnabled = isReferenceModeEnabled(connectionTaskBuilder)

    connectionTaskBuilder.step match {
      case Some(value) =>
        val schemaFields = value.step.fields
        // Check if the field exists in the schema
        val hasFields = isForeignKeyInSchema(fields, schemaFields)
        if (!hasFields && !value.step.options.contains(METADATA_SOURCE_TYPE) && !isDeleteFk && !referenceModeEnabled) {
          throw new RuntimeException(s"Field name defined in foreign key relationship does not exist, data-source=$dataSource, field-name=$fieldNames")
        }
        ForeignKeyRelation(dataSource, value.step.name, fields)
      case None =>
        throw new RuntimeException(s"No fields defined for data source. Cannot create foreign key relationship, data-source=$dataSource, field-name=$fieldNames")
    }
  }

  private def getSinkOpt: SinkOptionsBuilder = {
    plan.sinkOptions match {
      case Some(value) => api.SinkOptionsBuilder(value)
      case None => SinkOptionsBuilder()
    }
  }

  private def isReferenceModeEnabled(connectionTaskBuilder: ConnectionTaskBuilder[_]): Boolean = {
    val configTaskReferenceMode = connectionTaskBuilder.connectionConfigWithTaskBuilder.options.getOrElse(ENABLE_REFERENCE_MODE, "false").toBoolean
    val stepReferenceMode = connectionTaskBuilder.step.get.step.options.getOrElse(ENABLE_REFERENCE_MODE, "false").toBoolean
    configTaskReferenceMode || stepReferenceMode
  }

  private def isForeignKeyInSchema(fields: List[String], schemaFields: List[api.model.Field]): Boolean = {
    // For all fields defined in the foreign key relationship, check if all fields exist in the schema
    fields.forall(f => {
      val fieldParts = f.split("\\.")
      // If field is a dot separated string, recursively check if all parts exist in the field's parent
      if (fieldParts.length > 1) {
        val parentField = schemaFields.find(_.name == fieldParts.head)
        if (parentField.isDefined) {
          isForeignKeyInSchema(List(fieldParts.tail.mkString(".")), parentField.get.fields)
        } else {
          false
        }
      } else {
        schemaFields.exists(_.name == fieldParts.head)
      }
    })
  }

  /**
   * Create a PlanBuilder that loads base configuration from a YAML file.
   * This allows referencing existing YAML plan definitions while still being able to override
   * specific configurations using the builder pattern.
   *
   * @param yamlConfig Configuration specifying which YAML file to load
   * @return PlanBuilder with YAML plan as base configuration
   */
  def fromYaml(yamlConfig: YamlConfig): PlanBuilder = {
    // Add YAML source as metadata source type for later processing
    val yamlSource = YamlPlanSource(yamlConfig.toOptionsMap + (METADATA_SOURCE_TYPE -> YAML_PLAN))
    // Return current builder with special marker to indicate YAML loading
    // The actual YAML loading will happen during execution when the plan is processed
    this.modify(_.plan.description).setTo(s"${plan.description} [YAML: ${yamlConfig.planFile.getOrElse("unknown")}]")
  }
}
