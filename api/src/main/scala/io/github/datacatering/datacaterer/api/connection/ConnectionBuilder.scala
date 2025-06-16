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

  /**
   * Use same connection configuration and task configuration from another builder.
   *
   * @param connectionTaskBuilder The builder to copy from.
   * @return
   */
  def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[T]): T

  /**
   * Set the connection name. Should be unique within same PlanRun
   *
   * @param name The connection name.
   * @return The connection task builder.
   */
  def name(name: String): ConnectionTaskBuilder[T] = {
    this.task = Some(getTask.name(name))
    this
  }

  /**
   * Set new step within connection. A step is comprised of a single data source and its associated options.
   *
   * @param stepBuilder The step builder.
   * @return The connection task builder.
   */
  def step(stepBuilder: StepBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(stepBuilder)
    this
  }

  /**
   * Set the fields for the data source. Contains all the metadata required to generate data.
   *
   * @param fields The fields to set.
   * @return The connection task builder.
   */
  @varargs def fields(fields: FieldBuilder*): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.fields(fields: _*))
    this
  }

  /**
   * Set the fields for the data source. Contains all the metadata required to generate data.
   *
   * @param fields The fields to set.
   * @return The connection task builder.
   */
  def fields(fields: List[FieldBuilder]): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.fields(fields))
    this
  }

  /**
   * Set the fields for the data source. Contains all the metadata required to generate data from an external source.
   *
   * @param metadataSourceBuilder The metadata source builder.
   * @return The connection task builder.
   */
  def fields(metadataSourceBuilder: MetadataSourceBuilder): ConnectionTaskBuilder[T] = {
    this.connectionConfigWithTaskBuilder = this.connectionConfigWithTaskBuilder.metadataSource(metadataSourceBuilder)
    this.step = Some(getStep.options(metadataSourceBuilder.metadataSource.allOptions))
    this
  }

  /**
   * Set the number of records to generate. Can be used to generate a fixed number of records, random range of records or per set of fields.
   *
   * @param countBuilder The count builder.
   * @return The connection task builder.
   */
  def count(countBuilder: CountBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.count(countBuilder))
    this
  }

  /**
   * Set the number of records to generate. Used to generate a random number of records according to the metadata.
   *
   * @param generatorBuilder The generator builder.
   * @return The connection task builder.
   */
  def count(generatorBuilder: GeneratorBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.count(generatorBuilder))
    this
  }

  /**
   * Generate all possible combinations of the fields. Fields with `oneOf` will be used to generate all possible combinations.
   *
   * @param enable Whether to enable all combinations.
   * @return The connection task builder.
   */
  def allCombinations(enable: Boolean): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.option(ALL_COMBINATIONS, enable.toString))
    this
  }

  /**
   * Set the number of partitions to generate. Used to generate a fixed number of partitions.
   *
   * @param numPartitions The number of partitions.
   * @return The connection task builder.
   */
  def numPartitions(numPartitions: Int): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.numPartitions(numPartitions))
    this
  }

  /**
   * Define data validations to be performed on the data source.
   *
   * @param validationBuilders The validation builders.
   * @return The connection task builder.
   */
  @varargs def validations(validationBuilders: ValidationBuilder*): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.validations(validationBuilders: _*))
    this
  }

  /**
   * Define data validations to be performed on the data source. Validations sourced from external metadata.
   *
   * @param metadataSourceBuilder The metadata source builder.
   * @return The connection task builder.
   */
  def validations(metadataSourceBuilder: MetadataSourceBuilder): ConnectionTaskBuilder[T] = {
    this.connectionConfigWithTaskBuilder = this.connectionConfigWithTaskBuilder.metadataSource(metadataSourceBuilder)
    this
  }

  def getValidations: List[ValidationBuilder] = {
    getStep.optValidation.map(_.dataSourceValidation.validations).getOrElse(List())
  }

  /**
   * Define a wait condition before running data validations. Useful when waiting for data to be consumed by a job or service.
   *
   * @param waitConditionBuilder The wait condition builder.
   * @return The connection task builder.
   */
  def validationWait(waitConditionBuilder: WaitConditionBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.wait(waitConditionBuilder))
    this
  }

  /**
   * Wait for data to exist before running data validations. Useful when waiting for data to be consumed by a job or service.
   *
   * @param expr The SQL expression to evaluate on the data source to know when the correct data exists.
   * @return The connection task builder.
   */
  def validationWaitDataExists(expr: String): ConnectionTaskBuilder[T] = {
    val waitConditionBuilder = new WaitConditionBuilder()
      .dataExists(this.connectionConfigWithTaskBuilder.dataSourceName, this.connectionConfigWithTaskBuilder.options, expr)
    this.step = Some(getStep.wait(waitConditionBuilder))
    this
  }

  /**
   * Enable data generation. Used to generate data from the data source.
   *
   * @param enable Whether to enable data generation.
   * @return The connection task builder.
   */
  def enableDataGeneration(enable: Boolean): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.enableDataGeneration(enable))
    this
  }

  /**
   * Enable data validation. Used to validate data from the data source.
   *
   * @param enable Whether to enable data validation.
   * @return The connection task builder.
   */
  def enableDataValidation(enable: Boolean): ConnectionTaskBuilder[T] = {
    this.connectionConfigWithTaskBuilder = connectionConfigWithTaskBuilder.options(Map(ENABLE_DATA_VALIDATION -> enable.toString))
    this
  }

  /**
   * Define a task for data generation.
   *
   * @param taskBuilder The task builder.
   * @return The connection task builder.
   */
  def task(taskBuilder: TaskBuilder): ConnectionTaskBuilder[T] = {
    this.task = Some(taskBuilder)
    this
  }

  /**
   * Define a task for data generation.
   *
   * @param stepBuilders The step builders.
   * @return The connection task builder.
   */
  @varargs def task(stepBuilders: StepBuilder*): ConnectionTaskBuilder[T] = {
    this.task = Some(getTask.steps(stepBuilders: _*))
    this
  }

  /**
   * Include only specific fields in data generation. Supports dot notation for nested fields.
   * Example: includeFields("name", "address.city", "account.balance")
   *
   * @param fields Field names to include
   * @return The connection task builder
   */
  @varargs def includeFields(fields: String*): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.includeFields(fields: _*))
    this
  }

  /**
   * Exclude specific fields from data generation. Supports dot notation for nested fields.
   * Example: excludeFields("internal_id", "metadata.created_by")
   *
   * @param fields Field names to exclude
   * @return The connection task builder
   */
  @varargs def excludeFields(fields: String*): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.excludeFields(fields: _*))
    this
  }

  /**
   * Include fields matching regex patterns. Supports dot notation for nested fields.
   * Example: includeFieldPatterns("user_.*", "account_.*")
   *
   * @param patterns Regex patterns for field names to include
   * @return The connection task builder
   */
  @varargs def includeFieldPatterns(patterns: String*): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.includeFieldPatterns(patterns: _*))
    this
  }

  /**
   * Exclude fields matching regex patterns. Supports dot notation for nested fields.
   * Example: excludeFieldPatterns("internal_.*", "temp_.*")
   *
   * @param patterns Regex patterns for field names to exclude
   * @return The connection task builder
   */
  @varargs def excludeFieldPatterns(patterns: String*): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.excludeFieldPatterns(patterns: _*))
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
      val optValidationIdentifier = this.connectionConfigWithTaskBuilder.options.get(VALIDATION_IDENTIFIER)
      if (optValidationIdentifier.isDefined) {
        baseStep.option(VALIDATION_IDENTIFIER -> optValidationIdentifier.get)
      } else {
        this.connectionConfigWithTaskBuilder = this.connectionConfigWithTaskBuilder.option(VALIDATION_IDENTIFIER -> baseStep.step.name)
        baseStep.option(VALIDATION_IDENTIFIER -> baseStep.step.name)
      }
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

  /**
   * Set the file partitioning fields. For example, `year`, `month`, `day` maps to `2021`, `01`, `01` partitions.
   *
   * @param partitionsBy The fields to partition by.
   * @return
   */
  @varargs def partitionBy(partitionsBy: String*): FileBuilder = {
    this.step = Some(getStep.partitionBy(partitionsBy: _*))
    this
  }
}

trait JdbcBuilder[T] extends ConnectionTaskBuilder[T] {

  /**
   * Set the JDBC table name. For example, `schema.table`.
   *
   * @param table The JDBC table name.
   * @return
   */
  def table(table: String): JdbcBuilder[T] = {
    this.step = Some(getStep.jdbcTable(table))
    this
  }

  /**
   * Set the JDBC schema and table name. For example, `schema.table`.
   *
   * @param schema The schema name.
   * @param table  The table name within the schema.
   * @return
   */
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

  /**
   * Set the Cassandra keyspace and table.
   *
   * @param keyspace Keyspace name.
   * @param table    Table name within keyspace.
   * @return
   */
  def table(keyspace: String, table: String): CassandraBuilder = {
    this.step = Some(getStep.cassandraTable(keyspace, table))
    this
  }
}

case class BigQueryBuilder() extends ConnectionTaskBuilder[BigQueryBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[BigQueryBuilder]): BigQueryBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  /**
   * Set the BigQuery table name. For example, `project.dataset.table`.
   *
   * @param table The BigQuery table name.
   * @return
   */
  def table(table: String): BigQueryBuilder = {
    this.step = Some(getStep.table(table))
    this
  }
}

case class RabbitmqBuilder() extends ConnectionTaskBuilder[RabbitmqBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[RabbitmqBuilder]): RabbitmqBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  /**
   * Set the RabbitMQ destination. For example, `queue-name`.
   *
   * @param destination The RabbitMQ destination.
   * @return The connection task builder.
   */
  def destination(destination: String): RabbitmqBuilder = {
    this.step = Some(getStep.jmsDestination(destination))
    this
  }

}

case class SolaceBuilder() extends ConnectionTaskBuilder[SolaceBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[SolaceBuilder]): SolaceBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  /**
   * Set the Solace destination. For example, `queue-name` or `JNDI/Q/queue-name`.
   *
   * @param destination The Solace destination.
   * @return The connection task builder.
   */
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

  /**
   * Set the Kafka topic.
   *
   * @param topic The Kafka topic.
   * @return The connection task builder.
   */
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

  /**
   * Set the HTTP URL, method, path parameters and query parameters.
   *
   * @param url         The HTTP URL.
   * @param method      The HTTP method.
   * @param pathParams  The HTTP path parameters.
   * @param queryParams The HTTP query parameters.
   * @return
   */
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
