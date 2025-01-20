package io.github.datacatering.datacaterer.api.connection

import io.github.datacatering.datacaterer.api.HttpMethodEnum.HttpMethodEnum
import io.github.datacatering.datacaterer.api.model.Constants.{ALL_COMBINATIONS, ENABLE_DATA_VALIDATION, FORMAT, VALIDATION_IDENTIFIER}
import io.github.datacatering.datacaterer.api.model.{Step, Task}
import io.github.datacatering.datacaterer.api.{ConnectionConfigWithTaskBuilder, CountBuilder, FieldBuilder, GeneratorBuilder, HttpMethodEnum, MetadataSourceBuilder, StepBuilder, TaskBuilder, TasksBuilder, ValidationBuilder, WaitConditionBuilder}

import scala.annotation.varargs

trait ConnectionTaskBuilder[T] {
  var connectionConfigWithTaskBuilder: ConnectionConfigWithTaskBuilder = ConnectionConfigWithTaskBuilder()
  var task: Option[TaskBuilder] = None
  var step: Option[StepBuilder] = None

  def apply(builder: ConnectionConfigWithTaskBuilder, optTask: Option[Task], optStep: Option[Step]): ConnectionTaskBuilder[T] = {
    this.connectionConfigWithTaskBuilder = builder
    this.task = optTask.map(TaskBuilder)
    this.step = optStep.map(s => StepBuilder(s))
    this
  }

  def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[T]): T

  def name(name: String): ConnectionTaskBuilder[T] = {
    this.task = Some(getTask.name(name))
    this
  }

  def step(stepBuilder: StepBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(stepBuilder)
    this
  }

  @varargs def fields(fields: FieldBuilder*): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.fields(fields: _*))
    this
  }

  def fields(fields: List[FieldBuilder]): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.fields(fields))
    this
  }

  def fields(metadataSourceBuilder: MetadataSourceBuilder): ConnectionTaskBuilder[T] = {
    this.connectionConfigWithTaskBuilder = this.connectionConfigWithTaskBuilder.metadataSource(metadataSourceBuilder)
    this.step = Some(getStep.options(metadataSourceBuilder.metadataSource.allOptions))
    this
  }

  def count(countBuilder: CountBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.count(countBuilder))
    this
  }

  def count(generatorBuilder: GeneratorBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.count(generatorBuilder))
    this
  }

  def allCombinations(enable: Boolean): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.option(ALL_COMBINATIONS, enable.toString))
    this
  }

  def numPartitions(numPartitions: Int): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.numPartitions(numPartitions))
    this
  }

  @varargs def validations(validationBuilders: ValidationBuilder*): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.validations(validationBuilders: _*))
    this
  }

  def validations(metadataSourceBuilder: MetadataSourceBuilder): ConnectionTaskBuilder[T] = {
    this.connectionConfigWithTaskBuilder = this.connectionConfigWithTaskBuilder.metadataSource(metadataSourceBuilder)
    this
  }

  def getValidations: List[ValidationBuilder] = {
    getStep.optValidation.map(_.dataSourceValidation.validations).getOrElse(List())
  }

  def validationWait(waitConditionBuilder: WaitConditionBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.wait(waitConditionBuilder))
    this
  }

  def validationWaitDataExists(expr: String): ConnectionTaskBuilder[T] = {
    val waitConditionBuilder = new WaitConditionBuilder()
      .dataExists(this.connectionConfigWithTaskBuilder.dataSourceName, this.connectionConfigWithTaskBuilder.options, expr)
    this.step = Some(getStep.wait(waitConditionBuilder))
    this
  }

  def enableDataGeneration(enable: Boolean): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.enableDataGeneration(enable))
    this
  }

  def enableDataValidation(enable: Boolean): ConnectionTaskBuilder[T] = {
    this.connectionConfigWithTaskBuilder = connectionConfigWithTaskBuilder.options(Map(ENABLE_DATA_VALIDATION -> enable.toString))
    this
  }

  def task(taskBuilder: TaskBuilder): ConnectionTaskBuilder[T] = {
    this.task = Some(taskBuilder)
    this
  }

  @varargs def task(stepBuilders: StepBuilder*): ConnectionTaskBuilder[T] = {
    this.task = Some(getTask.steps(stepBuilders: _*))
    this
  }

  def toTasksBuilder: Option[TasksBuilder] = {
    val dataSourceName = connectionConfigWithTaskBuilder.dataSourceName
    val format = connectionConfigWithTaskBuilder.options(FORMAT)
    val optBaseTask = (task, step) match {
      case (Some(task), Some(step)) => Some(task.steps(step.`type`(format)))
      case (Some(task), None) => Some(task)
      case (None, Some(step)) => Some(TaskBuilder().steps(step.`type`(format)))
      case _ => None
    }

    optBaseTask.map(TasksBuilder().addTasks(dataSourceName, _))
  }

  def getStep: StepBuilder = step match {
    case Some(value) => value
    case None =>
      val baseStep = StepBuilder()
      this.connectionConfigWithTaskBuilder = this.connectionConfigWithTaskBuilder.option(VALIDATION_IDENTIFIER -> baseStep.step.name)
      baseStep.option(VALIDATION_IDENTIFIER -> baseStep.step.name)
      baseStep
  }

  protected def getTask: TaskBuilder = task match {
    case Some(value) => value
    case None => TaskBuilder()
  }
}

case class NoopBuilder() extends ConnectionTaskBuilder[NoopBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[NoopBuilder]): NoopBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }
}

case class FileBuilder() extends ConnectionTaskBuilder[FileBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[FileBuilder]): FileBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  @varargs def partitionBy(partitionsBy: String*): FileBuilder = {
    this.step = Some(getStep.partitionBy(partitionsBy: _*))
    this
  }
}

trait JdbcBuilder[T] extends ConnectionTaskBuilder[T] {

  def table(table: String): JdbcBuilder[T] = {
    this.step = Some(getStep.jdbcTable(table))
    this
  }

  def table(schema: String, table: String): JdbcBuilder[T] = {
    this.step = Some(getStep.jdbcTable(schema, table))
    this
  }
}

case class PostgresBuilder() extends JdbcBuilder[PostgresBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[PostgresBuilder]): PostgresBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }
}

case class MySqlBuilder() extends JdbcBuilder[MySqlBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[MySqlBuilder]): MySqlBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }
}

case class CassandraBuilder() extends ConnectionTaskBuilder[CassandraBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[CassandraBuilder]): CassandraBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  def table(keyspace: String, table: String): CassandraBuilder = {
    this.step = Some(getStep.cassandraTable(keyspace, table))
    this
  }

}

case class SolaceBuilder() extends ConnectionTaskBuilder[SolaceBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[SolaceBuilder]): SolaceBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  def destination(destination: String): SolaceBuilder = {
    this.step = Some(getStep.jmsDestination(destination))
    this
  }

}

case class KafkaBuilder() extends ConnectionTaskBuilder[KafkaBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[KafkaBuilder]): KafkaBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  def topic(topic: String): KafkaBuilder = {
    this.step = Some(getStep.kafkaTopic(topic))
    this
  }

}

case class HttpBuilder() extends ConnectionTaskBuilder[HttpBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[HttpBuilder]): HttpBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  def url(
           url: String,
           method: HttpMethodEnum = HttpMethodEnum.GET,
           pathParams: List[FieldBuilder] = List(),
           queryParams: List[FieldBuilder] = List()
         ): HttpBuilder = {
    val httpFields = FieldBuilder().httpUrl(url, method, pathParams, queryParams)
    this.step = Some(getStep.fields(httpFields: _*))
    this
  }
}
