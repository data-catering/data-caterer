package io.github.datacatering.datacaterer.api

import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api.HttpMethodEnum.HttpMethodEnum
import io.github.datacatering.datacaterer.api.HttpQueryParameterStyleEnum.HttpQueryParameterStyleEnum
import io.github.datacatering.datacaterer.api.converter.Converters.{toScalaList, toScalaMap}
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{ArrayType, Count, DataType, Field, HeaderType, PerFieldCount, Step, StringType, Task, TaskSummary}

import scala.annotation.varargs

/**
 * Builds a `TaskSummary` with optional `Task` information.
 *
 * @param taskSummary the `TaskSummary` to build, with a default name and data source
 * @param task        an optional `Task` to include in the summary
 */
case class TaskSummaryBuilder(
                               taskSummary: TaskSummary = TaskSummary(DEFAULT_TASK_NAME, "myDefaultDataSource"),
                               task: Option[Task] = None
                             ) {
  /**
   * Constructs a new `TaskBuilder` instance with the default task name and data source name.
   */
  def this() = this(TaskSummary(DEFAULT_TASK_NAME, DEFAULT_DATA_SOURCE_NAME), None)

  /**
   * Sets the name of the task.
   *
   * @param name the name to set for the task
   * @return the updated `TaskSummaryBuilder` instance
   */
  def name(name: String): TaskSummaryBuilder = {
    if (task.isEmpty) this.modify(_.taskSummary.name).setTo(name) else this
  }

  /**
   * Builds a `TaskSummaryBuilder` by modifying the `TaskSummary` with the provided `TaskBuilder`.
   *
   * @param taskBuilder the `TaskBuilder` to use for modifying the `TaskSummary`
   * @return a `TaskSummaryBuilder` with the `TaskSummary` modified according to the provided `TaskBuilder`
   */
  def task(taskBuilder: TaskBuilder): TaskSummaryBuilder = {
    this.modify(_.taskSummary.name).setTo(taskBuilder.task.name)
      .modify(_.task).setTo(Some(taskBuilder.task))
  }

  /**
   * Builds a `TaskSummaryBuilder` with the specified `Task` instance.
   *
   * @param task the `Task` instance to be associated with the `TaskSummaryBuilder`
   * @return a `TaskSummaryBuilder` with the specified `Task` instance
   */
  def task(task: Task): TaskSummaryBuilder = {
    this.modify(_.taskSummary.name).setTo(task.name)
      .modify(_.task).setTo(Some(task))
  }

  /**
   * Sets the data source name for the task summary.
   *
   * @param name the name of the data source
   * @return a new `TaskSummaryBuilder` with the updated data source name
   */
  def dataSource(name: String): TaskSummaryBuilder =
    this.modify(_.taskSummary.dataSourceName).setTo(name)

  /**
   * Sets whether the task is enabled or disabled.
   *
   * @param enabled `true` to enable the task, `false` to disable it
   * @return a new `TaskSummaryBuilder` with the updated enabled state
   */
  def enabled(enabled: Boolean): TaskSummaryBuilder =
    this.modify(_.taskSummary.enabled).setTo(enabled)

}

/**
 * Builds a list of `Task` instances with a specified data source name.
 *
 * @param tasks          the list of `Task` instances to build, defaults to an empty list
 * @param dataSourceName the name of the data source to use, defaults to `DEFAULT_DATA_SOURCE_NAME`
 */
case class TasksBuilder(tasks: List[Task] = List(), dataSourceName: String = DEFAULT_DATA_SOURCE_NAME) {
  def this() = this(List(), DEFAULT_DATA_SOURCE_NAME)

  /**
   * Adds one or more tasks to the `TasksBuilder` instance.
   *
   * @param dataSourceName the name of the data source associated with the tasks
   * @param taskBuilders   the `TaskBuilder` instances representing the tasks to add
   * @return the updated `TasksBuilder` instance
   */
  @varargs def addTasks(dataSourceName: String, taskBuilders: TaskBuilder*): TasksBuilder =
    this.modify(_.tasks)(_ ++ taskBuilders.map(_.task))
      .modify(_.dataSourceName).setTo(dataSourceName)

  /**
   * Adds a new task to the TasksBuilder.
   *
   * @param name           The name of the task.
   * @param dataSourceName The name of the data source for the task.
   * @param stepBuilders   The step builders for the task.
   * @return The updated TasksBuilder.
   */
  @varargs def addTask(name: String, dataSourceName: String, stepBuilders: StepBuilder*): TasksBuilder =
    this.modify(_.tasks)(_ ++ List(TaskBuilder(Task(name, stepBuilders.map(_.step).toList)).task))
      .modify(_.dataSourceName).setTo(dataSourceName)

  /**
   * Adds a new task to the TasksBuilder.
   *
   * @param name           The name of the task.
   * @param dataSourceName The name of the data source for the task.
   * @param steps          The list of steps for the task.
   * @return The updated TasksBuilder instance.
   */
  def addTask(name: String, dataSourceName: String, steps: List[Step]): TasksBuilder =
    this.modify(_.tasks)(_ ++ List(TaskBuilder(Task(name, steps)).task))
      .modify(_.dataSourceName).setTo(dataSourceName)
}

/**
 * A task can be seen as a representation of a data source.
 * A task can contain steps which represent sub data sources within it.<br>
 * For example, you can define a Postgres task for database 'customer' with steps to generate data for
 * tables 'public.account' and 'public.transactions' within it.
 */
case class TaskBuilder(task: Task = Task()) {
  def this() = this(Task())

  /**
   * Sets the name of the task.
   *
   * @param name the name to set for the task
   * @return the updated `TaskBuilder` instance
   */
  def name(name: String): TaskBuilder = this.modify(_.task.name).setTo(name)

  /**
   * Adds the given `StepBuilder` instances as steps to the task.
   *
   * @param steps the `StepBuilder` instances to add as steps
   * @return the updated `TaskBuilder` instance
   */
  @varargs def steps(steps: StepBuilder*): TaskBuilder = this.modify(_.task.steps)(_ ++ steps.map(_.step))
}

case class StepBuilder(step: Step = Step(), optValidation: Option[DataSourceValidationBuilder] = None) {
  def this() = this(Step(), None)

  /**
   * Define name of step.
   * Used as part of foreign key definitions
   *
   * @param name Step name
   * @return StepBuilder
   */
  def name(name: String): StepBuilder =
    this.modify(_.step.name).setTo(name)

  /**
   * Define type of step. For example, csv, json, parquet.
   * Used to determine how to save the generated data
   *
   * @param type Can be one of the supported types
   * @return StepBuilder
   */
  def `type`(`type`: String): StepBuilder =
    this.modify(_.step.`type`).setTo(`type`)

  /**
   * Enable/disable the step
   *
   * @param enabled Boolean flag
   * @return StepBuilder
   */
  def enabled(enabled: Boolean): StepBuilder =
    this.modify(_.step.enabled).setTo(enabled)

  /**
   * Add in generic option to the step.
   * This can be used to configure the sub data source details such as table, topic, and file path.
   * It is used as part of the options passed to Spark when connecting to the data source.
   * Can also be used for attaching metadata to the step
   *
   * @param option Key and value of the data used for retrieval
   * @return StepBuilder
   */
  def option(option: (String, String)): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(option))

  /**
   * Map of configurations used by Spark to connect to the data source
   *
   * @param options Map of key value pairs to connect to data source
   * @return StepBuilder
   */
  def options(options: Map[String, String]): StepBuilder =
    this.modify(_.step.options)(_ ++ options)

  /**
   * Wrapper for Java Map
   *
   * @param options Map of key value pairs to connect to data source
   * @return StepBuilder
   */
  def options(options: java.util.Map[String, String]): StepBuilder =
    this.options(toScalaMap(options))

  /**
   * Define table name to connect for JDBC data source.
   *
   * @param table Table name
   * @return StepBuilder
   */
  def jdbcTable(table: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(JDBC_TABLE -> table))

  /**
   * Define schema and table name for JDBC data source.
   *
   * @param schema Schema name
   * @param table  Table name
   * @return StepBuilder
   */
  def jdbcTable(schema: String, table: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(JDBC_TABLE -> s"$schema.$table"))

  /**
   * Keyspace and table name for Cassandra data source
   *
   * @param keyspace Keyspace name
   * @param table    Table name
   * @return StepBuilder
   */
  def cassandraTable(keyspace: String, table: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(CASSANDRA_KEYSPACE -> keyspace, CASSANDRA_TABLE -> table))

  /**
   * The queue/topic name for a JMS data source.
   * This is used as part of connecting to a JMS destination as a JNDI resource
   *
   * @param destination Destination name
   * @return StepBuilder
   */
  def jmsDestination(destination: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(JMS_DESTINATION_NAME -> destination))

  /**
   * Kafka topic to push data to for Kafka data source
   *
   * @param topic Topic name
   * @return StepBuilder
   */
  def kafkaTopic(topic: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(KAFKA_TOPIC -> topic))

  /**
   * File pathway used for file data source.
   * Can be defined as a local file system path or cloud based path (i.e. s3a://my-bucket/file/path)
   *
   * @param path File path
   * @return StepBuilder
   */
  def path(path: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(PATH -> path))

  /**
   * The fields within the generated data to use as partitions for a file data source.
   * Order of partition fields defined is used to define order of partitions.<br>
   * For example, {{{partitionBy("year", "account_id")}}}
   * will ensure that `year` is used as the top level partition
   * before `account_id`.
   *
   * @param partitionsBy Partition field names in order
   * @return StepBuilder
   */
  @varargs def partitionBy(partitionsBy: String*): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(PARTITION_BY -> partitionsBy.map(_.trim).mkString(",")))

  /**
   * Number of partitions to use when saving data to the data source.
   * This can be used to help fine tune performance depending on your data source.<br>
   * For example, if you are facing timeout errors when saving to your database, you can reduce the number of
   * partitions to help reduce the number of concurrent saves to your database.
   *
   * @param partitions Number of partitions when saving data to data source
   * @return StepBuilder
   */
  def numPartitions(partitions: Int): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(PARTITIONS -> partitions.toString))

  /**
   * Number of rows pushed to data source per second.
   * Only used for real time data sources such as JMS, Kafka and HTTP.<br>
   * If you see that the number of rows per second is not reaching as high as expected, it may be due to the number
   * of partitions used when saving data. You will also need to increase the number of partitions via<br>
   * {{{.numPartitions(20)}}} or some higher number
   *
   * @param rowsPerSecond Number of rows per second to generate
   * @return StepBuilder
   */
  def rowsPerSecond(rowsPerSecond: Int): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(ROWS_PER_SECOND -> rowsPerSecond.toString))

  /**
   * Define number of records to be generated for the sub data source via CountBuilder
   *
   * @param countBuilder Configure number of records to generate
   * @return StepBuilder
   */
  def count(countBuilder: CountBuilder): StepBuilder =
    this.modify(_.step.count).setTo(countBuilder.count)

  /**
   * Define number of records to be generated.
   * If you also have defined a per field count, this value will not represent the full number of records generated.
   *
   * @param records Number of records to generate
   * @return StepBuilder
   * @see <a href=https://data.catering/setup/generator/count/>Count definition</a> for details
   */
  def count(records: Long): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().records(records).count)

  /**
   * Define a generator to be used for determining the number of records to generate.
   * If you also have defined a per field count, the value generated will be combined with the per field count to
   * determine the total number of records
   *
   * @param generator Generator builder for determining number of records to generate
   * @return StepBuilder
   * @see <a href=https://data.catering/setup/generator/count/>Count definition</a> for details
   */
  def count(generator: GeneratorBuilder): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().generator(generator).count)

  /**
   * Define the number of records to generate based off certain fields.<br>
   * For example, if you had a data set with fields account_id and amount, you can set that 10 records to be generated
   * per account_id via {{{.count(new PerFieldCountBuilder().total(10, "account_id")}}}.
   * The total number of records generated is also influenced by other count configurations.
   *
   * @param perFieldCountBuilder Per field count builder
   * @return StepBuilder
   * @see <a href=https://data.catering/setup/generator/count/>Count definition</a> for details
   */
  def count(perFieldCountBuilder: PerFieldCountBuilder): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().perField(perFieldCountBuilder).count)

  /**
   * Define fields of the schema of the data source to use when generating data.
   *
   * @param fields Fields of the schema
   * @return StepBuilder
   */
  @varargs def fields(fields: FieldBuilder*): StepBuilder =
    this.modify(_.step.fields).setTo(step.fields ++ fields.map(_.field))

  /**
   * Define fields of the schema of the data source to use when generating data.
   *
   * @param fields Fields of the schema
   * @return StepBuilder
   */
  def fields(fields: List[FieldBuilder]): StepBuilder =
    this.modify(_.step.fields).setTo(step.fields ++ fields.map(_.field))

  /**
   * Define data validations once data has been generated. The result of the validations is logged out and included
   * as part of the HTML report.
   *
   * @param validations All validations
   * @return StepBuilder
   */
  @varargs def validations(validations: ValidationBuilder*): StepBuilder =
    this.modify(_.optValidation).setTo(Some(getValidation.validations(validations: _*)))

  /**
   * Define a wait condition that is used before executing validations on the data source
   *
   * @param waitConditionBuilder Builder for wait condition
   * @return StepBuilder
   */
  def wait(waitConditionBuilder: WaitConditionBuilder): StepBuilder =
    this.modify(_.optValidation).setTo(Some(getValidation.wait(waitConditionBuilder)))

  /**
   * Enable/disable data generation for this step. By default, it follows what is defined at configuration level
   * {{configuration.enableGenerateData}}. Enabled by default.
   *
   * @param enable Enable data generation
   * @return
   */
  def enableDataGeneration(enable: Boolean): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(ENABLE_DATA_GENERATION -> enable.toString))

  private def getValidation: DataSourceValidationBuilder = optValidation.getOrElse(DataSourceValidationBuilder())
}

/**
 * Builds a `Count` instance with the specified count value.
 *
 * @param count the count value to use for the `Count` instance
 */
case class CountBuilder(count: Count = Count()) {
  def this() = this(Count())

  /**
   * Sets the number of records to be processed by the task.
   *
   * @param records the number of records to be processed
   * @return a new `CountBuilder` instance with the records count set
   */
  def records(records: Long): CountBuilder =
    this.modify(_.count.records).setTo(Some(records))

  /**
   * Sets the generator for the count builder and clears the records.
   *
   * @param generator the generator to set for the count builder
   * @return the modified count builder
   */
  def generator(generator: GeneratorBuilder): CountBuilder =
    this.modify(_.count.options).setTo(generator.options)
      .modify(_.count.records).setTo(None)

  /**
   * Sets the per-field count for the task builder.
   *
   * @param perFieldCountBuilder the builder for the per-field count
   * @return the updated task builder
   */
  def perField(perFieldCountBuilder: PerFieldCountBuilder): CountBuilder =
    this.modify(_.count.perField).setTo(Some(perFieldCountBuilder.perFieldCount))

  /**
   * Sets the number of records per field for the task builder.
   *
   * @param records the number of records per field
   * @param fields  the field names to apply the records per field setting to
   * @return the updated task builder
   */
  @varargs def recordsPerField(records: Long, fields: String*): CountBuilder =
    this.modify(_.count.perField).setTo(Some(perFieldCount.records(records, fields: _*).perFieldCount))

  /**
   * Generates a `CountBuilder` that records the number of records per field.
   *
   * @param generator The `GeneratorBuilder` to use for generating the per-field counts.
   * @param fields    The field names to generate per-field counts for.
   * @return A `CountBuilder` that records the number of records per field.
   */
  @varargs def recordsPerFieldGenerator(generator: GeneratorBuilder, fields: String*): CountBuilder =
    this.modify(_.count.perField).setTo(Some(perFieldCount.generator(generator, fields: _*).perFieldCount))

  /**
   * Generates a `CountBuilder` with the specified number of records and a generator for the per-field counts.
   *
   * @param records   the total number of records to generate
   * @param generator the `GeneratorBuilder` to use for generating the per-field counts
   * @param fields    the names of the fields to generate counts for
   * @return a `CountBuilder` with the specified record and per-field count settings
   */
  @varargs def recordsPerFieldGenerator(records: Long, generator: GeneratorBuilder, fields: String*): CountBuilder =
    this.modify(_.count.records).setTo(Some(records))
      .modify(_.count.perField).setTo(Some(perFieldCount.generator(generator, fields: _*).perFieldCount))

  /**
   * Generates a normal distribution of records per field for the specified fields.
   *
   * @param min    the minimum number of records per field
   * @param max    the maximum number of records per field
   * @param fields the fields to generate the normal distribution for
   * @return a `CountBuilder` instance with the normal distribution configuration applied
   */
  @varargs def recordsPerFieldNormalDistribution(min: Long, max: Long, fields: String*): CountBuilder = {
    val generator = GeneratorBuilder().min(min).max(max).normalDistribution()
    this.modify(_.count.perField).setTo(Some(perFieldCount.generator(generator, fields: _*).perFieldCount))
  }

  /**
   * Configures the task builder to generate records per field using an exponential distribution.
   *
   * @param min           the minimum number of records per field
   * @param max           the maximum number of records per field
   * @param rateParameter the rate parameter for the exponential distribution
   * @param fields        the fields to apply the distribution to
   * @return the modified task builder
   */
  @varargs def recordsPerFieldExponentialDistribution(min: Long, max: Long, rateParameter: Double, fields: String*): CountBuilder = {
    val generator = GeneratorBuilder().min(min).max(max).exponentialDistribution(rateParameter)
    this.modify(_.count.perField).setTo(Some(perFieldCount.generator(generator, fields: _*).perFieldCount))
  }

  /**
   * Generates a list of records per field using an exponential distribution.
   *
   * @param rateParameter the rate parameter for the exponential distribution
   * @param fields        the fields to generate records for
   * @return a [[CountBuilder]] that can be used to build the records
   */
  @varargs def recordsPerFieldExponentialDistribution(rateParameter: Double, fields: String*): CountBuilder =
    recordsPerFieldExponentialDistribution(0, 100, rateParameter, fields: _*)

  private def perFieldCount: PerFieldCountBuilder = {
    count.perField match {
      case Some(value) => PerFieldCountBuilder(value)
      case None => PerFieldCountBuilder()
    }
  }
}

/**
 * Define number of records to generate based on certain field values. This is used in situations where
 * you want to generate multiple records for a given set of field values to closer represent the real production
 * data setting. For example, you may have a data set containing bank transactions where you want to generate
 * multiple transactions per account.
 */
case class PerFieldCountBuilder(perFieldCount: PerFieldCount = PerFieldCount()) {

  /**
   * Define the set of fields that should have multiple records generated for.
   *
   * @param fieldNames Field names
   * @return PerFieldCountBuilder
   */
  @varargs def fieldNames(fieldNames: String*): PerFieldCountBuilder =
    this.modify(_.perFieldCount.fieldNames).setTo(fieldNames.toList)

  /**
   * Number of records to generate per set of field values defined
   *
   * @param records Number of records
   * @param fields  Field names
   * @return PerFieldCountBuilder
   */
  @varargs def records(records: Long, fields: String*): PerFieldCountBuilder =
    fieldNames(fields: _*).modify(_.perFieldCount.count).setTo(Some(records))

  /**
   * Define a generator to determine the number of records to generate per set of field value defined
   *
   * @param generator Generator for number of records
   * @param fields    Field names
   * @return PerFieldCountBuilder
   */
  @varargs def generator(generator: GeneratorBuilder, fields: String*): PerFieldCountBuilder =
    fieldNames(fields: _*).modify(_.perFieldCount.options).setTo(generator.options)
}

/**
 * Builds a `Field` instance with optional configuration.
 *
 * @param field the initial `Field` instance to build upon, defaults to a new `Field` instance
 */
case class FieldBuilder(field: Field = Field()) {
  def this() = this(Field())

  /**
   * Sets the name of the field.
   *
   * @param name the name of the field
   * @return the updated `FieldBuilder` instance
   */
  def name(name: String): FieldBuilder =
    this.modify(_.field.name).setTo(name)

  /**
   * Sets the data type of the field being built.
   *
   * @param `type` the data type to set for the field
   * @return the updated `FieldBuilder` instance
   */
  def `type`(`type`: DataType): FieldBuilder =
    this.modify(_.field.`type`).setTo(Some(`type`.toString))

  /**
   * Adds the specified fields to the schema of this `FieldBuilder`.
   *
   * @param fields the fields to add to the schema
   * @return a new `FieldBuilder` with the updated schema
   */
  @varargs def fields(fields: FieldBuilder*): FieldBuilder =
    this.modify(_.field.fields).setTo(field.fields ++ fields.map(_.field))

  /**
   * Adds the specified fields to the schema of this `FieldBuilder`.
   *
   * @param fields the fields to add to the schema
   * @return a new `FieldBuilder` with the updated schema
   */
  def fields(fields: List[FieldBuilder]): FieldBuilder =
    this.modify(_.field.fields).setTo(field.fields ++ fields.map(_.field))

  /**
   * Sets the field generator for the `TaskBuilder` instance, using the options from the provided `MetadataSourceBuilder`.
   *
   * @param metadataSourceBuilder the `MetadataSourceBuilder` instance to use for the field generator options
   * @return the updated `FieldBuilder` instance
   */
  def fields(metadataSourceBuilder: MetadataSourceBuilder): FieldBuilder =
    this.modify(_.field.options).setTo(metadataSourceBuilder.metadataSource.allOptions)

  /**
   * Sets whether the field is nullable or not.
   *
   * @param nullable `true` if the field should be nullable, `false` otherwise.
   * @return the updated `FieldBuilder` instance.
   */
  def nullable(nullable: Boolean): FieldBuilder =
    this.modify(_.field.nullable).setTo(nullable)

  /**
   * Sets the generator for the current field builder.
   *
   * @param generator the generator to set for the field
   * @return the updated field builder
   */
  def generator(generator: GeneratorBuilder): FieldBuilder =
    this.modify(_.field.options).setTo(generator.options)

  /**
   * Sets the SQL query to be used for this field.
   *
   * @param sql the SQL query to use for this field
   * @return a new `FieldBuilder` instance with the SQL query set
   */
  def sql(sql: String): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.sql(sql).options)

  /**
   * Sets the regular expression pattern to be used for the field generator.
   *
   * @param regex the regular expression pattern to use for the field generator
   * @return the updated `FieldBuilder` instance
   */
  def regex(regex: String): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.regex(regex).options)

  /**
   * Builds a field that can take on one of the provided values.
   *
   * @param values The values that the field can take on.
   * @return A FieldBuilder that has been modified to use the provided values.
   */
  @varargs def oneOf(values: Any*): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.oneOf(values: _*).options)
      .modify(_.field.`type`)
      .setTo(
        values match {
          case Seq(_: Double, _*) => Some("double")
          case Seq(_: String, _*) => Some("string")
          case Seq(_: Int, _*) => Some("integer")
          case Seq(_: Long, _*) => Some("long")
          case Seq(_: Boolean, _*) => Some("boolean")
          case _ => None
        }
      )

  /**
   * Sets the options for the field generator.
   *
   * @param options a map of options to configure the field generator
   * @return the updated FieldBuilder instance
   */
  def options(options: Map[String, Any]): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.options(options).options)

  /**
   * Adds an option to the field generator.
   *
   * @param option a tuple containing the option name and value
   * @return the updated `FieldBuilder` instance
   */
  def option(option: (String, Any)): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.option(option).options)

  /**
   * Sets the seed for the field generator.
   *
   * @param seed the seed value to use for the field generator
   * @return the updated `FieldBuilder` instance
   */
  def seed(seed: Long): FieldBuilder = this.modify(_.field.options).setTo(getGenBuilder.seed(seed).options)

  /**
   * Enables or disables null values for the field.
   *
   * @param enable `true` to enable null values, `false` to disable
   * @return the updated `FieldBuilder` instance
   */
  def enableNull(enable: Boolean): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.enableNull(enable).options)

  /**
   * Sets the null probability for the field generator.
   *
   * @param probability The probability of generating a null value, between 0 and 1.
   * @return The updated `FieldBuilder` instance.
   */
  def nullProbability(probability: Double): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.nullProbability(probability).options)

  /**
   * Enables or disables edge cases for the field generator.
   *
   * @param enable `true` to enable edge cases, `false` to disable them
   * @return the updated `FieldBuilder` instance
   */
  def enableEdgeCases(enable: Boolean): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.enableEdgeCases(enable).options)

  /**
   * Sets the edge case probability for the field generator.
   *
   * @param probability The probability of an edge case occurring, between 0 and 1.
   * @return The updated `FieldBuilder` instance.
   */
  def edgeCaseProbability(probability: Double): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.edgeCaseProbability(probability).options)

  /**
   * Creates a `FieldBuilder` with a static value generator.
   *
   * @param value The static value to use for the field.
   * @return A `FieldBuilder` with the static value generator set.
   */
  def static(value: Any): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.static(value).options)

  /**
   * Constructs a `FieldBuilder` with a static value.
   *
   * @param value the static value to set on the `FieldBuilder`
   * @return a `FieldBuilder` with the static value set
   */
  def staticValue(value: Any): FieldBuilder = static(value)

  /**
   * Sets the field generator to be unique or not unique.
   *
   * @param isUnique `true` to make the field generator unique, `false` otherwise.
   * @return the updated `FieldBuilder` instance.
   */
  def unique(isUnique: Boolean): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.unique(isUnique).options)

  /**
   * Sets the field generator to an array type with the specified element type.
   *
   * @param `type` the type of the array elements
   * @return a new `FieldBuilder` instance with the array type set
   */
  def arrayType(`type`: String): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.arrayType(`type`).options)

  /**
   * Sets the faker expression for the field generator.
   *
   * @param expr the faker expression to set for the field generator
   * @return the updated `FieldBuilder` instance
   */
  def expression(expr: String): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.expression(expr).options)

  /**
   * Sets the field generator to use an average length generator with the specified length.
   *
   * @param length the length to use for the average length generator
   * @return the updated `FieldBuilder` instance
   */
  def avgLength(length: Int): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.avgLength(length).options)

  /**
   * Sets the minimum value for the field.
   *
   * @param min The minimum value for the field.
   * @return The updated `FieldBuilder` instance.
   */
  def min(min: Any): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.min(min).options)

  /**
   * Sets the minimum length of the field value.
   *
   * @param length the minimum length of the field value
   * @return the updated `FieldBuilder` instance
   */
  def minLength(length: Int): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.minLength(length).options)

  /**
   * Sets the minimum length for the array generated by this `FieldBuilder`.
   *
   * @param length the minimum length for the generated array
   * @return the updated `FieldBuilder` instance
   */
  def arrayMinLength(length: Int): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.arrayMinLength(length).options)

  /**
   * Sets the maximum value for the field generator.
   *
   * @param max The maximum value to set for the field generator.
   * @return The updated `FieldBuilder` instance.
   */
  def max(max: Any): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.max(max).options)

  /**
   * Sets the maximum length of the field.
   *
   * @param length the maximum length of the field
   * @return the updated `FieldBuilder` instance
   */
  def maxLength(length: Int): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.maxLength(length).options)

  /**
   * Sets the maximum length of the array generated by the field's generator.
   *
   * @param length the maximum length of the generated array
   * @return the updated `FieldBuilder` instance
   */
  def arrayMaxLength(length: Int): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.arrayMaxLength(length).options)

  /**
   * Sets the numeric precision for the field.
   *
   * @param precision The numeric precision to set for the field.
   * @return The updated `FieldBuilder` instance.
   */
  def numericPrecision(precision: Int): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.numericPrecision(precision).options)

  /**
   * Sets the numeric scale for the field.
   *
   * @param scale the numeric scale to set for the field
   * @return the updated `FieldBuilder` instance
   */
  def numericScale(scale: Int): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.numericScale(scale).options)

  /**
   * Sets the rounding for the field.
   *
   * @param round Number of decimal places to round to
   * @return the updated `FieldBuilder` instance
   */
  def round(round: Int): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.round(round).options)

  /**
   * Sets whether the field should be omitted from the generated output.
   *
   * @param omit `true` to omit the field, `false` to include it.
   * @return a new `FieldBuilder` instance with the updated omit setting.
   */
  def omit(omit: Boolean): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.omit(omit).options)

  /**
   * Sets the primary key flag for the current field.
   *
   * @param isPrimaryKey `true` to mark the field as a primary key, `false` otherwise.
   * @return The updated `FieldBuilder` instance.
   */
  def primaryKey(isPrimaryKey: Boolean): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.primaryKey(isPrimaryKey).options)

  /**
   * Sets the primary key position for the field being built.
   *
   * @param position the position of the primary key
   * @return the updated FieldBuilder instance
   */
  def primaryKeyPosition(position: Int): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.primaryKeyPosition(position).options)

  /**
   * Sets the clustering position for the field generator.
   *
   * @param position the position of the field in the clustering order
   * @return the updated `FieldBuilder` instance
   */
  def clusteringPosition(position: Int): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.clusteringPosition(position).options)

  /**
   * Sets the standard deviation of the field generator.
   *
   * @param stddev the standard deviation to use for the field generator
   * @return the updated `FieldBuilder` instance
   */
  def standardDeviation(stddev: Double): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.standardDeviation(stddev).options)

  /**
   * Sets the mean value for the field generator.
   *
   * @param mean the mean value to set for the field generator
   * @return the updated `FieldBuilder` instance
   */
  def mean(mean: Double): FieldBuilder =
    this.modify(_.field.options).setTo(getGenBuilder.mean(mean).options)

  /**
   * Create message header fields. Can be used for data sources such as Kafka or Solace.
   *
   * @param messageHeader Headers of the message
   * @return FieldBuilder
   */
  @varargs def messageHeaders(messageHeader: FieldBuilder*): FieldBuilder = {
    val combinedSql = messageHeader.map(f => f.field.options(SQL_GENERATOR)).mkString(",")
    this.name(REAL_TIME_HEADERS_FIELD).sql(s"ARRAY($combinedSql)").`type`(HeaderType.getType)
  }

  /**
   * Create message header field. Can be used for data sources such as Kafka or Solace.
   * Use with `messageHeaders(...)`.
   *
   * @param key   Key of the header
   * @param value Value of the header in SQL
   * @return FieldBuilder
   */
  def messageHeader(key: String, value: String): FieldBuilder =
    this.modify(_.field.options)
      .setTo(getGenBuilder.sql(s"NAMED_STRUCT('key', '$key', 'value', TO_BINARY($value, 'utf-8'))").options)

  /**
   * Create a message body field. Can be used for data sources such as Kafka or Solace.
   *
   * @param fields Fields defined in the JSON body of the message
   * @return List of fields
   */
  @varargs def messageBody(fields: FieldBuilder*): List[FieldBuilder] = {
    realTimeBody(fields: _*)
  }

  /**
   * Create a HTTP header field. Used for HTTP data source.
   *
   * @param name Name of the header field
   * @return FieldBuilder
   */
  def httpHeader(name: String): FieldBuilder = {
    val cleanFieldName = name.replaceAll("-", "_")
    val contentLengthMap = if (name == "Content-Length") {
      Map(
        SQL_GENERATOR -> s"LENGTH($REAL_TIME_BODY_FIELD)",
        HTTP_HEADER_FIELD_PREFIX -> name
      )
    } else Map(HTTP_HEADER_FIELD_PREFIX -> name)
    this.name(HTTP_HEADER_FIELD_PREFIX + cleanFieldName)
      .options(contentLengthMap)
  }

  /**
   * Create a HTTP path parameter field. Used for HTTP data source path parameters in URL.
   * For example, url=http://localhost:8080/user/{id} has path parameter `id`
   *
   * @param name Name of the path parameter
   * @return FieldBuilder
   */
  def httpPathParam(name: String): FieldBuilder = {
    this.name(HTTP_PATH_PARAM_FIELD_PREFIX + name).nullable(false).enableNull(false)
  }

  /**
   * Create a HTTP query parameter field. Used for HTTP data source query parameters in URL.
   * Follows the OpenAPI standard (https://swagger.io/docs/specification/v3_0/serialization/#query-parameters).
   *
   * @param name Name of the path parameter
   * @return FieldBuilder
   */
  def httpQueryParam(
                      name: String,
                      `type`: DataType = StringType,
                      style: HttpQueryParameterStyleEnum = HttpQueryParameterStyleEnum.FORM,
                      explode: Boolean = true,
                    ): FieldBuilder = {
    val fieldName = s"$HTTP_QUERY_PARAM_FIELD_PREFIX$name"
    val sqlGenerator = `type` match {
      case ArrayType =>
        val delimiter = (style, explode) match {
          case (HttpQueryParameterStyleEnum.FORM, false) => ","
          case (HttpQueryParameterStyleEnum.SPACE_DELIMITED, false) => "%20"
          case (HttpQueryParameterStyleEnum.PIPE_DELIMITED, false) => "|"
          case _ => s"&$name="
        }
        s"""CASE WHEN ARRAY_SIZE($fieldName) > 0 THEN CONCAT('$name=', ARRAY_JOIN($fieldName, '$delimiter')) ELSE null END"""
      case _ => s"CONCAT('$name=', $fieldName)"
    }
    this.name(fieldName).option(POST_SQL_EXPRESSION -> sqlGenerator)
  }

  /**
   * Create a HTTP query parameter field of type string. Used for HTTP data source query parameters in URL.
   *
   * @param name Name of query parameter
   * @return FieldBuilder
   */
  def httpQueryParam(name: String): FieldBuilder = httpQueryParam(name, StringType)

  /**
   * Create a HTTP URL field. It will build the URL based on the path and query parameters defined.
   *
   * @param url         URL of the HTTP endpoint
   * @param method      HTTP method to call endpoint (i.e. GET, POST, PUT, etc.)
   * @param pathParams  Definition of path parameter field generation
   * @param queryParams Definition of query parameter field generation
   * @return List of FieldBuilder
   */
  def httpUrl(
               url: String,
               method: HttpMethodEnum = HttpMethodEnum.GET,
               pathParams: List[FieldBuilder] = List(),
               queryParams: List[FieldBuilder] = List()
             ): List[FieldBuilder] = {
    val urlWithPathParamReplace = pathParams.foldLeft(s"'$url'")((url, pathParam) => {
      val fieldName = pathParam.field.name
      val fieldNameWithoutPrefix = fieldName.replaceFirst(HTTP_PATH_PARAM_FIELD_PREFIX, "")
      val replaceValue = pathParam.field.options.getOrElse(POST_SQL_EXPRESSION, s"`$fieldName`")
      s"REPLACE($url, '{$fieldNameWithoutPrefix}', URL_ENCODE($replaceValue))"
    })
    val urlWithPathAndQuery = if (queryParams.nonEmpty) s"CONCAT($urlWithPathParamReplace, '?')" else urlWithPathParamReplace
    val combinedQueryParams = queryParams.map(q => s"CAST(${q.field.options.getOrElse(POST_SQL_EXPRESSION, s"`${q.field.name}`")} AS STRING)").mkString(",")
    val combinedQuerySql = s"ARRAY_JOIN(ARRAY($combinedQueryParams), '&')"
    val urlSql = s"CONCAT($urlWithPathAndQuery, $combinedQuerySql)"
    val urlField = FieldBuilder().name(REAL_TIME_URL_FIELD).sql(urlSql)

    List(
      urlField,
      httpMethod(method)
    ) ++ pathParams ++ queryParams
  }

  /**
   * Create a HTTP URL field for Java.
   *
   * @param url         URL of the HTTP endpoint
   * @param method      HTTP method to call endpoint (i.e. GET, POST, PUT, etc.)
   * @param pathParams  Definition of path parameter field generation
   * @param queryParams Definition of query parameter field generation
   * @return List of FieldBuilder
   */
  def httpUrl(url: String, method: HttpMethodEnum, pathParams: java.util.List[FieldBuilder], queryParams: java.util.List[FieldBuilder]): List[FieldBuilder] =
    httpUrl(url, method, toScalaList(pathParams), toScalaList(queryParams))

  /**
   * Create a HTTP URL field with method.
   *
   * @param url    URL of the HTTP endpoint
   * @param method HTTP method to call endpoint (i.e. GET, POST, PUT, etc.)
   * @return List of FieldBuilder
   */
  def httpUrl(url: String, method: HttpMethodEnum): List[FieldBuilder] = httpUrl(url, method, List())

  /**
   * Create a HTTP URL field.
   *
   * @param url URL of the HTTP endpoint
   * @return List of FieldBuilder
   */
  def httpUrl(url: String): List[FieldBuilder] = httpUrl(url, HttpMethodEnum.GET)

  /**
   * Create a HTTP method field.
   *
   * @param method HTTP method to call endpoint (i.e. GET, POST, PUT, etc.)
   * @return FieldBuilder
   */
  def httpMethod(method: HttpMethodEnum): FieldBuilder = this.name(REAL_TIME_METHOD_FIELD).static(method.toString)

  /**
   * Create a HTTP body field. Used for HTTP data source.
   *
   * @param fields Fields defined in the JSON body of the message
   * @return List of fields
   */
  @varargs def httpBody(fields: FieldBuilder*): List[FieldBuilder] = {
    realTimeBody(fields: _*)
  }

  private def realTimeBody(fields: FieldBuilder*): List[FieldBuilder] = {
    val jsonContent = this.name(REAL_TIME_BODY_FIELD).sql(s"TO_JSON($REAL_TIME_BODY_CONTENT_FIELD)")
    val bodyContent = FieldBuilder().name(REAL_TIME_BODY_CONTENT_FIELD).fields(fields: _*)
    List(jsonContent, bodyContent)
  }

  private def getGenBuilder: GeneratorBuilder = {
    GeneratorBuilder(field.options)
  }
}

/**
 * Data generator contains all the metadata, related to either a field or count generation, required to create new data.
 */
case class GeneratorBuilder(options: Map[String, Any] = Map()) {
  def this() = this(Map())

  /**
   * Create a SQL based generator. You can reference other fields and SQL functions to generate data. The output data
   * type from the SQL expression should also match the data type defined otherwise a runtime error will be thrown
   *
   * @param sql SQL expression
   * @return GeneratorBuilder
   */
  def sql(sql: String): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(SQL_GENERATOR -> sql))

  /**
   * Create a generator based on a particular regex
   *
   * @param regex Regex data should adhere to
   * @return GeneratorBuilder
   */
  def regex(regex: String): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(REGEX_GENERATOR -> regex))

  /**
   * Create a generator that can only generate values from a set of values defined.
   *
   * @param values Set of valid values
   * @return GeneratorBuilder
   */
  @varargs def oneOf(values: Any*): GeneratorBuilder = this.modify(_.options)(_ ++ Map(ONE_OF_GENERATOR -> values))

  /**
   * Define metadata map for your generator. Add/overwrites existing metadata
   *
   * @param options Metadata map
   * @return GeneratorBuilder
   */
  def options(options: Map[String, Any]): GeneratorBuilder =
    this.modify(_.options)(_ ++ options)

  /**
   * Wrapper for Java Map
   *
   * @param options Metadata map
   * @return
   */
  def options(options: java.util.Map[String, Any]): GeneratorBuilder =
    this.options(toScalaMap(options))

  /**
   * Define metadata for your generator. Add/overwrites existing metadata
   *
   * @param option Key and value for metadata
   * @return GeneratorBuilder
   */
  def option(option: (String, Any)): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(option))

  /**
   * Seed to use for random generator. If you want to generate a consistent set of values, use this method
   *
   * @param seed Random seed
   * @return GeneratorBuilder
   */
  def seed(seed: Long): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(RANDOM_SEED -> seed.toString))

  /**
   * Enable/disable null values to be generated for this field
   *
   * @param enable Enable/disable null values
   * @return GeneratorBuilder
   */
  def enableNull(enable: Boolean): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(ENABLED_NULL -> enable.toString))

  /**
   * If [[enableNull]] is enabled, the generator will generate null values with the probability defined.
   * Value needs to be between 0.0 and 1.0.
   *
   * @param probability Probability of null values generated
   * @return GeneratorBuilder
   */
  def nullProbability(probability: Double): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(PROBABILITY_OF_NULL -> probability.toString))

  /**
   * Enable/disable edge case values to be generated. The edge cases are based on the data type defined.
   *
   * @param enable Enable/disable edge case values
   * @return GeneratorBuilder
   * @see <a href="https://data.catering/setup/generator/data-generator/">Generator</a> details here
   */
  def enableEdgeCases(enable: Boolean): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(ENABLED_EDGE_CASE -> enable.toString))


  /**
   * If [[enableEdgeCases]] is enabled, the generator will generate edge case values with the probability
   * defined. Value needs to be between 0.0 and 1.0.
   *
   * @param probability Probability of edge case values generated
   * @return GeneratorBuilder
   */
  def edgeCaseProbability(probability: Double): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(PROBABILITY_OF_EDGE_CASE -> probability.toString))

  /**
   * Generator will always give back the static value, ignoring all other metadata defined
   *
   * @param value Always generate this value
   * @return GeneratorBuilder
   */
  def static(value: Any): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(STATIC -> value.toString))

  /**
   * Wrapper for Java given `static` is a keyword
   *
   * @param value Always generate this value
   * @return GeneratorBuilder
   */
  def staticValue(value: Any): GeneratorBuilder = static(value)

  /**
   * Unique values within the generated data will be generated. This does not take into account values already existing
   * in the data source defined. It also requires the flag
   * [[DataCatererConfigurationBuilder.enableUniqueCheck]]
   * to be enabled (disabled by default as it is an expensive operation).
   *
   * @param isUnique Enable/disable generating unique values
   * @return GeneratorBuilder
   */
  def unique(isUnique: Boolean): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(IS_UNIQUE -> isUnique.toString))

  /**
   * If data type is array, define the inner data type of the array
   *
   * @param type Type of array
   * @return GeneratorBuilder
   */
  def arrayType(`type`: String): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(ARRAY_TYPE -> `type`))

  /**
   * Use a DataFaker expression to generate data. If you want to know what is possible to use as an expression, follow
   * the below link.
   *
   * @param expr DataFaker expression
   * @return GeneratorBuilder
   * @see <a href="https://data.catering/setup/generator/data-generator/#string">Expression</a> details
   */
  def expression(expr: String): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(EXPRESSION -> expr))

  /**
   * Average length of data generated. Length is specifically used for String data type and is ignored for other data types
   *
   * @param length Average length
   * @return GeneratorBuilder
   */
  def avgLength(length: Int): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(AVERAGE_LENGTH -> length.toString))

  /**
   * Minimum value to be generated. This can be used for any data type except for Struct and Array.
   *
   * @param min Minimum value
   * @return GeneratorBuilder
   */
  def min(min: Any): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(MINIMUM -> min.toString))

  /**
   * Minimum length of data generated. Length is specifically used for String data type and is ignored for other data types
   *
   * @param length Minimum length
   * @return GeneratorBuilder
   */
  def minLength(length: Int): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(MINIMUM_LENGTH -> length.toString))

  /**
   * Minimum length of array generated. Only used when data type is Array
   *
   * @param length Minimum length of array
   * @return GeneratorBuilder
   */
  def arrayMinLength(length: Int): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(ARRAY_MINIMUM_LENGTH -> length.toString))

  /**
   * Maximum value to be generated. This can be used for any data type except for Struct and Array. Can be ignored in
   * scenario where database field is auto increment where values generated start from the max value.
   *
   * @param max Maximum value
   * @return GeneratorBuilder
   */
  def max(max: Any): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(MAXIMUM -> max.toString))

  /**
   * Maximum length of data generated. Length is specifically used for String data type and is ignored for other data types
   *
   * @param length Maximum length
   * @return GeneratorBuilder
   */
  def maxLength(length: Int): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(MAXIMUM_LENGTH -> length.toString))

  /**
   * Maximum length of array generated. Only used when data type is Array
   *
   * @param length Maximum length of array
   * @return GeneratorBuilder
   */
  def arrayMaxLength(length: Int): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(ARRAY_MAXIMUM_LENGTH -> length.toString))

  /**
   * Numeric precision used for Decimal data type
   *
   * @param precision Decimal precision
   * @return GeneratorBuilder
   */
  def numericPrecision(precision: Int): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(NUMERIC_PRECISION -> precision.toString))

  /**
   * Numeric scale for Decimal data type
   *
   * @param scale Decimal scale
   * @return GeneratorBuilder
   */
  def numericScale(scale: Int): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(NUMERIC_SCALE -> scale.toString))

  /**
   * Rounding to decimal places for numeric data types
   *
   * @param round Number of decimal places to round to
   * @return GeneratorBuilder
   */
  def round(round: Int): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(ROUND -> round.toString))

  /**
   * Enable/disable including the value in the final output to the data source. Allows you to define intermediate values
   * that can be used to generate other fields
   *
   * @param omit Enable/disable the value being in output to data source
   * @return GeneratorBuilder
   */
  def omit(omit: Boolean): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(OMIT -> omit.toString))

  /**
   * Field is a primary key of the data source.
   *
   * @param isPrimaryKey Enable/disable field being a primary key
   * @return GeneratorBuilder
   */
  def primaryKey(isPrimaryKey: Boolean): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(IS_PRIMARY_KEY -> isPrimaryKey.toString))

  /**
   * If [[primaryKey]] is enabled, this defines the position of the primary key. Starts at 1.
   *
   * @param position Position of primary key
   * @return GeneratorBuilder
   */
  def primaryKeyPosition(position: Int): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(PRIMARY_KEY_POSITION -> position.toString))

  /**
   * If the data source supports clustering order (like Cassandra), this represents the order of the clustering key.
   * Starts at 1.
   *
   * @param position Position of clustering key
   * @return GeneratorBuilder
   */
  def clusteringPosition(position: Int): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(CLUSTERING_POSITION -> position.toString))

  /**
   * The standard deviation of the data if it follows a normal distribution.
   *
   * @param stddev Standard deviation
   * @return GeneratorBuilder
   */
  def standardDeviation(stddev: Double): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(STANDARD_DEVIATION -> stddev.toString))

  /**
   * The mean of the data if it follows a normal distribution.
   *
   * @param mean Mean
   * @return GeneratorBuilder
   */
  def mean(mean: Double): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(MEAN -> mean.toString))

  /**
   * The distribution of the data is exponential.
   *
   * @param rate Rate parameter to control skewness of distribution.
   * @return GeneratorBuilder
   */
  def exponentialDistribution(rate: Double): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(DISTRIBUTION -> DISTRIBUTION_EXPONENTIAL, DISTRIBUTION_RATE_PARAMETER -> rate.toString))

  /**
   * The distribution of the data is normal.
   *
   * @return GeneratorBuilder
   */
  def normalDistribution(): GeneratorBuilder =
    this.modify(_.options)(_ ++ Map(DISTRIBUTION -> DISTRIBUTION_NORMAL))
}

object HttpMethodEnum extends Enumeration {
  type HttpMethodEnum = Value
  val POST, GET, PUT, PATCH, DELETE, HEAD, OPTIONS, TRACE = Value
}

object HttpQueryParameterStyleEnum extends Enumeration {
  type HttpQueryParameterStyleEnum = Value
  val FORM, SPACE_DELIMITED, PIPE_DELIMITED, OTHER = Value
}
