package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.converter.Converters.toScalaMap
import io.github.datacatering.datacaterer.api.model.Constants._
import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api.model.{Count, DataType, Field, Generator, PerColumnCount, Schema, Step, StringType, Task, TaskSummary}

import scala.annotation.varargs

case class TaskSummaryBuilder(
                               taskSummary: TaskSummary = TaskSummary(DEFAULT_TASK_NAME, "myDefaultDataSource"),
                               task: Option[Task] = None
                             ) {
  def this() = this(TaskSummary(DEFAULT_TASK_NAME, DEFAULT_DATA_SOURCE_NAME), None)

  def name(name: String): TaskSummaryBuilder = {
    if (task.isEmpty) this.modify(_.taskSummary.name).setTo(name) else this
  }

  def task(taskBuilder: TaskBuilder): TaskSummaryBuilder = {
    this.modify(_.taskSummary.name).setTo(taskBuilder.task.name)
      .modify(_.task).setTo(Some(taskBuilder.task))
  }

  def task(task: Task): TaskSummaryBuilder = {
    this.modify(_.taskSummary.name).setTo(task.name)
      .modify(_.task).setTo(Some(task))
  }

  def dataSource(name: String): TaskSummaryBuilder =
    this.modify(_.taskSummary.dataSourceName).setTo(name)

  def enabled(enabled: Boolean): TaskSummaryBuilder =
    this.modify(_.taskSummary.enabled).setTo(enabled)

}

case class TasksBuilder(tasks: List[Task] = List(), dataSourceName: String = DEFAULT_DATA_SOURCE_NAME) {
  def this() = this(List(), DEFAULT_DATA_SOURCE_NAME)

  @varargs def addTasks(dataSourceName: String, taskBuilders: TaskBuilder*): TasksBuilder =
    this.modify(_.tasks)(_ ++ taskBuilders.map(_.task))
      .modify(_.dataSourceName).setTo(dataSourceName)

  @varargs def addTask(name: String, dataSourceName: String, stepBuilders: StepBuilder*): TasksBuilder =
    this.modify(_.tasks)(_ ++ List(TaskBuilder(Task(name, stepBuilders.map(_.step).toList)).task))
      .modify(_.dataSourceName).setTo(dataSourceName)

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

  def name(name: String): TaskBuilder = this.modify(_.task.name).setTo(name)

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
   * The columns within the generated data to use as partitions for a file data source.
   * Order of partition columns defined is used to define order of partitions.<br>
   * For example, {{{partitionBy("year", "account_id")}}}
   * will ensure that `year` is used as the top level partition
   * before `account_id`.
   *
   * @param partitionsBy Partition column names in order
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
   * If you also have defined a per column count, this value will not represent the full number of records generated.
   *
   * @param records Number of records to generate
   * @return StepBuilder
   * @see <a href=https://data.catering/setup/generator/count/>Count definition</a> for details
   */
  def count(records: Long): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().records(records).count)

  /**
   * Define a generator to be used for determining the number of records to generate.
   * If you also have defined a per column count, the value generated will be combined with the per column count to
   * determine the total number of records
   *
   * @param generator Generator builder for determining number of records to generate
   * @return StepBuilder
   * @see <a href=https://data.catering/setup/generator/count/>Count definition</a> for details
   */
  def count(generator: GeneratorBuilder): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().generator(generator).count)

  /**
   * Define the number of records to generate based off certain columns.<br>
   * For example, if you had a data set with columns account_id and amount, you can set that 10 records to be generated
   * per account_id via {{{.count(new PerColumnCountBuilder().total(10, "account_id")}}}.
   * The total number of records generated is also influenced by other count configurations.
   *
   * @param perColumnCountBuilder Per column count builder
   * @return StepBuilder
   * @see <a href=https://data.catering/setup/generator/count/>Count definition</a> for details
   */
  def count(perColumnCountBuilder: PerColumnCountBuilder): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().perColumn(perColumnCountBuilder).count)

  /**
   * Schema to use when generating data for data source.
   * The schema includes various metadata about each field to guide the data generator on what the data should look
   * like.
   *
   * @param schemaBuilder Schema builder
   * @return StepBuilder
   */
  def schema(schemaBuilder: SchemaBuilder): StepBuilder =
    this.modify(_.step.schema).setTo(schemaBuilder.schema)

  /**
   * Define fields of the schema of the data source to use when generating data.
   *
   * @param fields Fields of the schema
   * @return StepBuilder
   */
  @varargs def schema(fields: FieldBuilder*): StepBuilder =
    this.modify(_.step.schema).setTo(SchemaBuilder().addFields(fields: _*).schema)

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

case class CountBuilder(count: Count = Count()) {
  def this() = this(Count())

  def records(records: Long): CountBuilder =
    this.modify(_.count.records).setTo(Some(records))

  def generator(generator: GeneratorBuilder): CountBuilder =
    this.modify(_.count.generator).setTo(Some(generator.generator))
      .modify(_.count.records).setTo(None)

  def perColumn(perColumnCountBuilder: PerColumnCountBuilder): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColumnCountBuilder.perColumnCount))

  @varargs def recordsPerColumn(records: Long, cols: String*): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColCount.records(records, cols: _*).perColumnCount))

  @varargs def recordsPerColumnGenerator(generator: GeneratorBuilder, cols: String*): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColCount.generator(generator, cols: _*).perColumnCount))

  @varargs def recordsPerColumnGenerator(records: Long, generator: GeneratorBuilder, cols: String*): CountBuilder =
    this.modify(_.count.records).setTo(Some(records))
      .modify(_.count.perColumn).setTo(Some(perColCount.generator(generator, cols: _*).perColumnCount))

  private def perColCount: PerColumnCountBuilder = {
    count.perColumn match {
      case Some(value) => PerColumnCountBuilder(value)
      case None => PerColumnCountBuilder()
    }
  }
}

/**
 * Define number of records to generate based on certain column values. This is used in situations where
 * you want to generate multiple records for a given set of column values to closer represent the real production
 * data setting. For example, you may have a data set containing bank transactions where you want to generate
 * multiple transactions per account.
 */
case class PerColumnCountBuilder(perColumnCount: PerColumnCount = PerColumnCount()) {

  /**
   * Define the set of columns that should have multiple records generated for.
   *
   * @param cols Column names
   * @return PerColumnCountBuilder
   */
  @varargs def columns(cols: String*): PerColumnCountBuilder =
    this.modify(_.perColumnCount.columnNames).setTo(cols.toList)

  /**
   * Number of records to generate per set of column values defined
   *
   * @param records Number of records
   * @param cols    Column names
   * @return PerColumnCountBuilder
   */
  @varargs def records(records: Long, cols: String*): PerColumnCountBuilder =
    columns(cols: _*).modify(_.perColumnCount.count).setTo(Some(records))

  /**
   * Define a generator to determine the number of records to generate per set of column value defined
   *
   * @param generator Generator for number of records
   * @param cols      Column names
   * @return PerColumnCountBuilder
   */
  @varargs def generator(generator: GeneratorBuilder, cols: String*): PerColumnCountBuilder =
    columns(cols: _*).modify(_.perColumnCount.generator).setTo(Some(generator.generator))
}

case class SchemaBuilder(schema: Schema = Schema()) {
  def this() = this(Schema())

  def addField(name: String, `type`: DataType = StringType): SchemaBuilder =
    addFields(FieldBuilder().name(name).`type`(`type`))

  @varargs def addFields(fields: FieldBuilder*): SchemaBuilder =
    this.modify(_.schema.fields).setTo(schema.fields match {
      case Some(value) => Some(value ++ fields.map(_.field))
      case None => Some(fields.map(_.field).toList)
    })
}

case class FieldBuilder(field: Field = Field()) {
  def this() = this(Field())

  def name(name: String): FieldBuilder =
    this.modify(_.field.name).setTo(name)

  def `type`(`type`: DataType): FieldBuilder =
    this.modify(_.field.`type`).setTo(Some(`type`.toString))

  def schema(schema: SchemaBuilder): FieldBuilder =
    this.modify(_.field.schema).setTo(Some(schema.schema))

  def schema(schema: Schema): FieldBuilder =
    this.modify(_.field.schema).setTo(Some(schema))

  @varargs def schema(fields: FieldBuilder*): FieldBuilder =
    this.modify(_.field.schema).setTo(Some(getSchema.addFields(fields: _*).schema))

  def schema(metadataSourceBuilder: MetadataSourceBuilder): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.options(metadataSourceBuilder.metadataSource.allOptions).generator))

  def nullable(nullable: Boolean): FieldBuilder =
    this.modify(_.field.nullable).setTo(nullable)

  def generator(generator: GeneratorBuilder): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(generator.generator))

  def generator(generator: Generator): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(generator))

  def random: FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.random.generator))

  def sql(sql: String): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.sql(sql).generator))

  def regex(regex: String): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.regex(regex).generator))

  @varargs def oneOf(values: Any*): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.oneOf(values: _*).generator))
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

  def options(options: Map[String, Any]): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.options(options).generator))

  def option(option: (String, Any)): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.option(option).generator))

  def seed(seed: Long): FieldBuilder = this.modify(_.field.generator).setTo(Some(getGenBuilder.seed(seed).generator))

  def enableNull(enable: Boolean): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.enableNull(enable).generator))

  def nullProbability(probability: Double): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.nullProbability(probability).generator))

  def enableEdgeCases(enable: Boolean): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.enableEdgeCases(enable).generator))

  def edgeCaseProbability(probability: Double): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.edgeCaseProbability(probability).generator))

  def static(value: Any): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.static(value).generator))

  def staticValue(value: Any): FieldBuilder = static(value)

  def unique(isUnique: Boolean): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.unique(isUnique).generator))

  def arrayType(`type`: String): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.arrayType(`type`).generator))

  def expression(expr: String): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.expression(expr).generator))

  def avgLength(length: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.avgLength(length).generator))

  def min(min: Any): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.min(min).generator))

  def minLength(length: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.minLength(length).generator))

  def arrayMinLength(length: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.arrayMinLength(length).generator))

  def max(max: Any): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.max(max).generator))

  def maxLength(length: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.maxLength(length).generator))

  def arrayMaxLength(length: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.arrayMaxLength(length).generator))

  def numericPrecision(precision: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.numericPrecision(precision).generator))

  def numericScale(scale: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.numericScale(scale).generator))

  def omit(omit: Boolean): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.omit(omit).generator))

  def primaryKey(isPrimaryKey: Boolean): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.primaryKey(isPrimaryKey).generator))

  def primaryKeyPosition(position: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.primaryKeyPosition(position).generator))

  def clusteringPosition(position: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.clusteringPosition(position).generator))

  def standardDeviation(stddev: Double): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.standardDeviation(stddev).generator))

  def mean(mean: Double): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.mean(mean).generator))

  private def getGenBuilder: GeneratorBuilder = {
    field.generator match {
      case Some(gen) => GeneratorBuilder(gen)
      case None => GeneratorBuilder()
    }
  }

  private def getSchema: SchemaBuilder = {
    field.schema match {
      case Some(schema) => SchemaBuilder(schema)
      case None => SchemaBuilder()
    }
  }
}

/**
 * Data generator contains all the metadata, related to either a field or count generation, required to create new data.
 */
case class GeneratorBuilder(generator: Generator = Generator()) {
  def this() = this(Generator())

  /**
   * Create a random data generator. Depending on the data type, particular defaults are set for the metadata
   *
   * @return GeneratorBuilder GeneratorBuilder
   * @see <a href="https://data.catering/setup/generator/data-generator/">Data generator</a> default details here
   */
  def random: GeneratorBuilder =
    this.modify(_.generator.`type`).setTo(RANDOM_GENERATOR)

  /**
   * Create a SQL based generator. You can reference other columns and SQL functions to generate data. The output data
   * type from the SQL expression should also match the data type defined otherwise a runtime error will be thrown
   *
   * @param sql SQL expression
   * @return GeneratorBuilder
   */
  def sql(sql: String): GeneratorBuilder =
    this.modify(_.generator.`type`).setTo(SQL_GENERATOR)
      .modify(_.generator.options)(_ ++ Map(SQL_GENERATOR -> sql))

  /**
   * Create a generator based on a particular regex
   *
   * @param regex Regex data should adhere to
   * @return GeneratorBuilder
   */
  def regex(regex: String): GeneratorBuilder =
    this.modify(_.generator.`type`).setTo(REGEX_GENERATOR)
      .modify(_.generator.options)(_ ++ Map(REGEX_GENERATOR -> regex))

  /**
   * Create a generator that can only generate values from a set of values defined.
   *
   * @param values Set of valid values
   * @return GeneratorBuilder
   */
  @varargs def oneOf(values: Any*): GeneratorBuilder = this.modify(_.generator.`type`).setTo(ONE_OF_GENERATOR)
    .modify(_.generator.options)(_ ++ Map(ONE_OF_GENERATOR -> values))

  /**
   * Define metadata map for your generator. Add/overwrites existing metadata
   *
   * @param options Metadata map
   * @return GeneratorBuilder
   */
  def options(options: Map[String, Any]): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ options)

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
    this.modify(_.generator.options)(_ ++ Map(option))

  /**
   * Seed to use for random generator. If you want to generate a consistent set of values, use this method
   *
   * @param seed Random seed
   * @return GeneratorBuilder
   */
  def seed(seed: Long): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(RANDOM_SEED -> seed.toString))

  /**
   * Enable/disable null values to be generated for this field
   *
   * @param enable Enable/disable null values
   * @return GeneratorBuilder
   */
  def enableNull(enable: Boolean): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(ENABLED_NULL -> enable.toString))

  /**
   * If [[enableNull]] is enabled, the generator will generate null values with the probability defined.
   * Value needs to be between 0.0 and 1.0.
   *
   * @param probability Probability of null values generated
   * @return GeneratorBuilder
   */
  def nullProbability(probability: Double): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(PROBABILITY_OF_NULL -> probability.toString))

  /**
   * Enable/disable edge case values to be generated. The edge cases are based on the data type defined.
   *
   * @param enable Enable/disable edge case values
   * @return GeneratorBuilder
   * @see <a href="https://data.catering/setup/generator/data-generator/">Generator</a> details here
   */
  def enableEdgeCases(enable: Boolean): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(ENABLED_EDGE_CASE -> enable.toString))


  /**
   * If [[enableEdgeCases]] is enabled, the generator will generate edge case values with the probability
   * defined. Value needs to be between 0.0 and 1.0.
   *
   * @param probability Probability of edge case values generated
   * @return GeneratorBuilder
   */
  def edgeCaseProbability(probability: Double): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(PROBABILITY_OF_EDGE_CASE -> probability.toString))

  /**
   * Generator will always give back the static value, ignoring all other metadata defined
   *
   * @param value Always generate this value
   * @return GeneratorBuilder
   */
  def static(value: Any): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(STATIC -> value.toString))

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
    this.modify(_.generator.options)(_ ++ Map(IS_UNIQUE -> isUnique.toString))

  /**
   * If data type is array, define the inner data type of the array
   *
   * @param type Type of array
   * @return GeneratorBuilder
   */
  def arrayType(`type`: String): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(ARRAY_TYPE -> `type`))

  /**
   * Use a DataFaker expression to generate data. If you want to know what is possible to use as an expression, follow
   * the below link.
   *
   * @param expr DataFaker expression
   * @return GeneratorBuilder
   * @see <a href="https://data.catering/setup/generator/data-generator/#string">Expression</a> details
   */
  def expression(expr: String): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(EXPRESSION -> expr))

  /**
   * Average length of data generated. Length is specifically used for String data type and is ignored for other data types
   *
   * @param length Average length
   * @return GeneratorBuilder
   */
  def avgLength(length: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(AVERAGE_LENGTH -> length.toString))

  /**
   * Minimum value to be generated. This can be used for any data type except for Struct and Array.
   *
   * @param min Minimum value
   * @return GeneratorBuilder
   */
  def min(min: Any): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(MINIMUM -> min.toString))

  /**
   * Minimum length of data generated. Length is specifically used for String data type and is ignored for other data types
   *
   * @param length Minimum length
   * @return GeneratorBuilder
   */
  def minLength(length: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(MINIMUM_LENGTH -> length.toString))

  /**
   * Minimum length of array generated. Only used when data type is Array
   *
   * @param length Minimum length of array
   * @return GeneratorBuilder
   */
  def arrayMinLength(length: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(ARRAY_MINIMUM_LENGTH -> length.toString))

  /**
   * Maximum value to be generated. This can be used for any data type except for Struct and Array. Can be ignored in
   * scenario where database column is auto increment where values generated start from the max value.
   *
   * @param max Maximum value
   * @return GeneratorBuilder
   */
  def max(max: Any): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(MAXIMUM -> max.toString))

  /**
   * Maximum length of data generated. Length is specifically used for String data type and is ignored for other data types
   *
   * @param length Maximum length
   * @return GeneratorBuilder
   */
  def maxLength(length: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(MAXIMUM_LENGTH -> length.toString))

  /**
   * Maximum length of array generated. Only used when data type is Array
   *
   * @param length Maximum length of array
   * @return GeneratorBuilder
   */
  def arrayMaxLength(length: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(ARRAY_MAXIMUM_LENGTH -> length.toString))

  /**
   * Numeric precision used for Decimal data type
   *
   * @param precision Decimal precision
   * @return GeneratorBuilder
   */
  def numericPrecision(precision: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(NUMERIC_PRECISION -> precision.toString))

  /**
   * Numeric scale for Decimal data type
   *
   * @param scale Decimal scale
   * @return GeneratorBuilder
   */
  def numericScale(scale: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(NUMERIC_SCALE -> scale.toString))

  /**
   * Enable/disable including the value in the final output to the data source. Allows you to define intermediate values
   * that can be used to generate other columns
   *
   * @param omit Enable/disable the value being in output to data source
   * @return GeneratorBuilder
   */
  def omit(omit: Boolean): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(OMIT -> omit.toString))

  /**
   * Field is a primary key of the data source.
   *
   * @param isPrimaryKey Enable/disable field being a primary key
   * @return GeneratorBuilder
   */
  def primaryKey(isPrimaryKey: Boolean): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(IS_PRIMARY_KEY -> isPrimaryKey.toString))

  /**
   * If [[primaryKey]] is enabled, this defines the position of the primary key. Starts at 1.
   *
   * @param position Position of primary key
   * @return GeneratorBuilder
   */
  def primaryKeyPosition(position: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(PRIMARY_KEY_POSITION -> position.toString))

  /**
   * If the data source supports clustering order (like Cassandra), this represents the order of the clustering key.
   * Starts at 1.
   *
   * @param position Position of clustering key
   * @return GeneratorBuilder
   */
  def clusteringPosition(position: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(CLUSTERING_POSITION -> position.toString))

  /**
   * The standard deviation of the data if it follows a normal distribution.
   *
   * @param stddev Standard deviation
   * @return GeneratorBuilder
   */
  def standardDeviation(stddev: Double): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(STANDARD_DEVIATION -> stddev.toString))

  /**
   * The mean of the data if it follows a normal distribution.
   *
   * @param mean Mean
   * @return GeneratorBuilder
   */
  def mean(mean: Double): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(MEAN -> mean.toString))
}