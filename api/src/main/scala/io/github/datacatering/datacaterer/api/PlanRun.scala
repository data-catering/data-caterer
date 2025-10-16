package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.connection.{BigQueryBuilder, CassandraBuilder, ConnectionTaskBuilder, FileBuilder, HttpBuilder, KafkaBuilder, MySqlBuilder, PostgresBuilder, RabbitmqBuilder, SolaceBuilder}
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, ForeignKeyRelation, Plan, Task, ValidationConfiguration}

import scala.annotation.varargs


trait PlanRun {
  var _plan: Plan = Plan()
  var _tasks: List[Task] = List()
  var _configuration: DataCatererConfiguration = DataCatererConfiguration()
  var _validations: List[ValidationConfiguration] = List()
  var _connectionTaskBuilders: Seq[ConnectionTaskBuilder[_]] = Seq()

  /**
   * Create new plan builder
   *
   * @return PlanBuilder
   */
  def plan: PlanBuilder = PlanBuilder()

  /**
   * Create new task summary builder
   *
   * @return TaskSummaryBuilder
   */
  def taskSummary: TaskSummaryBuilder = TaskSummaryBuilder()

  /**
   * Create new tasks builder
   *
   * @return TasksBuilder
   */
  def tasks: TasksBuilder = TasksBuilder()

  /**
   * Create new task builder
   *
   * @return TaskBuilder
   */
  def task: TaskBuilder = TaskBuilder()

  /**
   * Create new step builder
   *
   * @return StepBuilder
   */
  def step: StepBuilder = StepBuilder()

  /**
   * Create new field builder
   * Can create field with regex, range, list, or value
   * For example, `field.name("id").regex("ID[0-9]{8}")`
   *
   * @return FieldBuilder
   */
  def field: FieldBuilder = FieldBuilder()

  /**
   * Create new generator builder
   * Used to alter way data is generated, either for fields or records
   *
   * @return GeneratorBuilder
   */
  def generator: GeneratorBuilder = GeneratorBuilder()

  /**
   * Create new count builder
   * Used to set the number of records to generate
   *
   * @return CountBuilder
   */
  def count: CountBuilder = CountBuilder()

  /**
   * Create new connection task builder
   *
   * @return ConnectionTaskBuilder
   */
  def configuration: DataCatererConfigurationBuilder = DataCatererConfigurationBuilder()

  /**
   * Create new wait condition builder
   * Used to set conditions for waiting before running a set of validations
   *
   * @return WaitConditionBuilder
   */
  def waitCondition: WaitConditionBuilder = WaitConditionBuilder()

  /**
   * Create new validation builder
   * Used to set conditions for validating data
   *
   * @return ValidationBuilder
   */
  def validation: ValidationBuilder = ValidationBuilder()

  /**
   * Create new pre-filter builder
   * Used to set conditions for filtering data before validation
   *
   * @return PreFilterBuilder
   */
  def preFilterBuilder(validationBuilder: ValidationBuilder): CombinationPreFilterBuilder = PreFilterBuilder().filter(validationBuilder)

  /**
   * Create new data source validation builder
   * Used to set conditions for validating a data source
   *
   * @return DataSourceValidationBuilder
   */
  def dataSourceValidation: DataSourceValidationBuilder = DataSourceValidationBuilder()

  /**
   * Create new validation configuration builder
   * Used to set configurations for running validations
   *
   * @return ValidationConfigurationBuilder
   */
  def validationConfig: ValidationConfigurationBuilder = ValidationConfigurationBuilder()

  /**
   * Create new foreign key relation
   * Used to set relationships between data sources
   *
   * @return ForeignKeyRelation
   */
  def foreignField(dataSource: String, step: String, field: String): ForeignKeyRelation =
    new ForeignKeyRelation(dataSource, step, field)

  /**
   * Create new foreign key relation
   * Used to set relationships between data sources
   *
   * @return ForeignKeyRelation
   */
  def foreignField(dataSource: String, step: String, fields: List[String]): ForeignKeyRelation =
    ForeignKeyRelation(dataSource, step, fields)

  /**
   * Create new foreign key relation
   * Used to set relationships between data sources
   *
   * @return ForeignKeyRelation
   */
  def foreignField(connectionTask: ConnectionTaskBuilder[_], field: String): ForeignKeyRelation =
    ForeignKeyRelation(
      connectionTask.connectionConfigWithTaskBuilder.dataSourceName,
      connectionTask.getStep.step.name,
      List(field)
    )

  /**
   * Create new foreign key relation
   * Used to set relationships between data sources
   *
   * @return ForeignKeyRelation
   */
  def foreignField(connectionTask: ConnectionTaskBuilder[_], fields: List[String]): ForeignKeyRelation =
    ForeignKeyRelation(
      connectionTask.connectionConfigWithTaskBuilder.dataSourceName,
      connectionTask.getStep.step.name,
      fields
    )

  /**
   * Create new foreign key relation
   * Used to set relationships between data sources
   *
   * @return ForeignKeyRelation
   */
  def foreignField(connectionTask: ConnectionTaskBuilder[_], step: String, fields: List[String]): ForeignKeyRelation =
    ForeignKeyRelation(connectionTask.connectionConfigWithTaskBuilder.dataSourceName, step, fields)

  /**
   * Create new metadata source builder
   * Used to set metadata for a data source to gather information about schema or validations
   *
   * @return
   */
  def metadataSource: MetadataSourceBuilder = MetadataSourceBuilder()

  /**
   * Create new YAML builder for referencing existing YAML plan and task files.
   * This allows loading existing YAML configurations and overriding specific parts using the programmatic API.
   *
   * @return YamlBuilder
   */
  def yaml: YamlBuilder = YamlBuilder()

  /**
   * Create new CSV generation step with configurations
   *
   * @param name    Data source name
   * @param path    File path to generated CSV
   * @param options Additional options for CSV generation
   * @return FileBuilder
   */
  def csv(name: String, path: String, options: Map[String, String] = Map()): FileBuilder =
    ConnectionConfigWithTaskBuilder().file(name, CSV, path, options)

  /**
   * Create new JSON generation step with configurations
   *
   * @param name    Data source name
   * @param path    File path to generated JSON
   * @param options Additional options for JSON generation
   * @return FileBuilder
   */
  def json(name: String, path: String, options: Map[String, String] = Map()): FileBuilder =
    ConnectionConfigWithTaskBuilder().file(name, JSON, path, options)

  /**
   * Create new ORC generation step with configurations
   *
   * @param name    Data source name
   * @param path    File path to generated ORC
   * @param options Additional options for ORC generation
   * @return FileBuilder
   */
  def orc(name: String, path: String, options: Map[String, String] = Map()): FileBuilder =
    ConnectionConfigWithTaskBuilder().file(name, ORC, path, options)

  /**
   * Create new PARQUET generation step with configurations
   *
   * @param name    Data source name
   * @param path    File path to generated PARQUET
   * @param options Additional options for PARQUET generation
   * @return FileBuilder
   */
  def parquet(name: String, path: String, options: Map[String, String] = Map()): FileBuilder =
    ConnectionConfigWithTaskBuilder().file(name, PARQUET, path, options)

  /**
   * Create new HUDI generation step with configurations
   *
   * @param name      Data source name
   * @param path      File path to generated HUDI
   * @param tableName Table name to be used for HUDI generation
   * @param options   Additional options for HUDI generation
   * @return FileBuilder
   */
  def hudi(name: String, path: String, tableName: String, options: Map[String, String] = Map()): FileBuilder =
    ConnectionConfigWithTaskBuilder().file(name, HUDI, path, options ++ Map(HUDI_TABLE_NAME -> tableName))

  /**
   * Create new DELTA generation step with configurations
   *
   * @param name    Data source name
   * @param path    File path to generated DELTA
   * @param options Additional options for DELTA generation
   * @return FileBuilder
   */
  def delta(name: String, path: String, options: Map[String, String] = Map()): FileBuilder =
    ConnectionConfigWithTaskBuilder().file(name, DELTA, path, options)

  /**
   * Create new ICEBERG generation step with configurations
   *
   * @param name        Data source name
   * @param tableName   Table name for generated ICEBERG
   * @param path        Warehouse path to generated ICEBERG
   * @param catalogType Type of catalog for generated ICEBERG
   * @param catalogUri  Uri of catalog for generated ICEBERG
   * @param options     Additional options for ICEBERG generation
   * @return FileBuilder
   */
  def iceberg(
               name: String,
               tableName: String,
               path: String = "",
               catalogType: String = DEFAULT_ICEBERG_CATALOG_TYPE,
               catalogUri: String = "",
               options: Map[String, String] = Map()
             ): FileBuilder = {
    ConnectionConfigWithTaskBuilder().file(name, ICEBERG, path, options ++ Map(
      TABLE -> tableName,
      SPARK_ICEBERG_CATALOG_WAREHOUSE -> path,
      SPARK_ICEBERG_CATALOG_TYPE -> catalogType,
      SPARK_ICEBERG_CATALOG_URI -> catalogUri,
    ))
  }

  /**
   * Create new ICEBERG generation step with only warehouse path and table name. Uses hadoop as the catalog type.
   *
   * @param name      Data source name
   * @param tableName Table name for generated ICEBERG
   * @param path      Warehouse path to generated ICEBERG
   * @return FileBuilder
   */
  def icebergJava(name: String, tableName: String, path: String): FileBuilder =
    iceberg(name, tableName, path)

  /**
   * Create new POSTGRES generation step with connection configuration
   *
   * @param name     Data source name
   * @param url      Postgres url in format: jdbc:postgresql://_host_:_port_/_database_
   * @param username Postgres username
   * @param password Postgres password
   * @param options  Additional driver options
   * @return PostgresBuilder
   */
  def postgres(
                name: String,
                url: String = DEFAULT_POSTGRES_URL,
                username: String = DEFAULT_POSTGRES_USERNAME,
                password: String = DEFAULT_POSTGRES_PASSWORD,
                options: Map[String, String] = Map()
              ): PostgresBuilder =
    ConnectionConfigWithTaskBuilder().postgres(name, url, username, password, options)

  /**
   * Create new POSTGRES generation step with only Postgres URL and default username and password of 'postgres'
   *
   * @param name Data source name
   * @param url  Postgres url in format: jdbc:postgresql://_host_:_port_/_database_
   * @return PostgresBuilder
   */
  def postgresJava(name: String, url: String): PostgresBuilder = postgres(name, url)

  /**
   * Create new POSTGRES generation step using the same connection configuration from another PostgresBuilder
   *
   * @param connectionTaskBuilder Postgres builder with connection configuration
   * @return PostgresBuilder
   */
  def postgres(connectionTaskBuilder: ConnectionTaskBuilder[PostgresBuilder]): PostgresBuilder =
    PostgresBuilder().fromBaseConfig(connectionTaskBuilder)

  /**
   * Create new MYSQL generation step with connection configuration
   *
   * @param name     Data source name
   * @param url      Mysql url in format: jdbc:mysql://_host_:_port_/_database_
   * @param username Mysql username
   * @param password Mysql password
   * @param options  Additional driver options
   * @return MySqlBuilder
   */
  def mysql(
             name: String,
             url: String = DEFAULT_MYSQL_URL,
             username: String = DEFAULT_MYSQL_USERNAME,
             password: String = DEFAULT_MYSQL_PASSWORD,
             options: Map[String, String] = Map()
           ): MySqlBuilder =
    ConnectionConfigWithTaskBuilder().mysql(name, url, username, password, options)


  /**
   * Create new MYSQL generation step with only Mysql URL and default username and password of 'root'
   *
   * @param name Data source name
   * @param url  Mysql url in format: jdbc:mysql://_host_:_port_/_dbname_
   * @return MySqlBuilder
   */
  def mysqlJava(name: String, url: String): MySqlBuilder = mysql(name, url)


  /**
   * Create new MYSQL generation step using the same connection configuration from another MySqlBuilder
   *
   * @param connectionTaskBuilder Mysql builder with connection configuration
   * @return MySqlBuilder
   */
  def mysql(connectionTaskBuilder: ConnectionTaskBuilder[MySqlBuilder]): MySqlBuilder =
    MySqlBuilder().fromBaseConfig(connectionTaskBuilder)


  /**
   * Create new CASSANDRA generation step with connection configuration
   *
   * @param name     Data source name
   * @param url      Cassandra url with format: _host_:_port_
   * @param username Cassandra username
   * @param password Cassandra password
   * @param options  Additional connection options
   * @return CassandraBuilder
   */
  def cassandra(
                 name: String,
                 url: String = DEFAULT_CASSANDRA_URL,
                 username: String = DEFAULT_CASSANDRA_USERNAME,
                 password: String = DEFAULT_CASSANDRA_PASSWORD,
                 options: Map[String, String] = Map()
               ): CassandraBuilder =
    ConnectionConfigWithTaskBuilder().cassandra(name, url, username, password, options)


  /**
   * Create new CASSANDRA generation step with only Cassandra URL and default username and password of 'cassandra'
   *
   * @param name Data source name
   * @param url  Cassandra url with format: _host_:_port_
   * @return CassandraBuilder
   */
  def cassandraJava(name: String, url: String): CassandraBuilder = cassandra(name, url)


  /**
   * Create new Cassandra generation step using the same connection configuration from another CassandraBuilder
   *
   * @param connectionTaskBuilder Cassandra builder with connection configuration
   * @return CassandraBuilder
   */
  def cassandra(connectionTaskBuilder: ConnectionTaskBuilder[CassandraBuilder]): CassandraBuilder =
    CassandraBuilder().fromBaseConfig(connectionTaskBuilder)


  /**
   * Create new BigQuery generation step with name
   *
   * @param name Data source name
   * @return BigQueryBuilder
   */
  def bigquery(name: String): BigQueryBuilder = ConnectionConfigWithTaskBuilder().bigquery(name)

  /**
   * Create new BigQuery generation step with name and temporaryGcsBucket
   *
   * @param name               Data source name
   * @param temporaryGcsBucket Temporary GCS bucket to store temporary data
   * @return BigQueryBuilder
   */
  def bigquery(name: String, temporaryGcsBucket: String): BigQueryBuilder =
    ConnectionConfigWithTaskBuilder().bigquery(name, "", temporaryGcsBucket)

  /**
   * Create new BigQuery generation step with name, temporaryGcsBucket and options
   *
   * @param name               Data source name
   * @param temporaryGcsBucket Temporary GCS bucket to store temporary data
   * @param options            Additional connection options
   * @return BigQueryBuilder
   */
  def bigquery(name: String, temporaryGcsBucket: String, options: Map[String, String]): BigQueryBuilder =
    ConnectionConfigWithTaskBuilder().bigquery(name, "", temporaryGcsBucket, options)

  /**
   * Create new BigQuery generation step with name, credentials file, temporaryGcsBucket and options
   *
   * @param name               Data source name
   * @param credentialsFile    Path to BigQuery credentials file
   * @param temporaryGcsBucket Temporary GCS bucket to store temporary data
   * @param options            Additional connection options
   * @return BigQueryBuilder
   */
  def bigquery(name: String, credentialsFile: String, temporaryGcsBucket: String, options: Map[String, String]): BigQueryBuilder =
    ConnectionConfigWithTaskBuilder().bigquery(name, credentialsFile, temporaryGcsBucket, options)


  /**
   * Create new BigQuery generation step using the same connection configuration from another BigQueryBuilder
   *
   * @param connectionTaskBuilder BigQuery builder with connection configuration
   * @return BigQueryBuilder
   */
  def bigquery(connectionTaskBuilder: ConnectionTaskBuilder[BigQueryBuilder]): BigQueryBuilder =
    BigQueryBuilder().fromBaseConfig(connectionTaskBuilder)


  /**
   * Create new RABBITMQ generation step with connection configuration
   *
   * @param name              Data source name
   * @param url               Solace url
   * @param username          Solace username
   * @param password          Solace password
   * @param virtualHost       Virtual host in rabbitmq to connect to
   * @param connectionFactory Connection factory
   * @param options           Additional connection options
   * @return SolaceBuilder
   */
  def rabbitmq(
                name: String,
                url: String = DEFAULT_RABBITMQ_URL,
                username: String = DEFAULT_RABBITMQ_USERNAME,
                password: String = DEFAULT_RABBITMQ_PASSWORD,
                virtualHost: String = DEFAULT_RABBITMQ_VIRTUAL_HOST,
                connectionFactory: String = DEFAULT_RABBITMQ_CONNECTION_FACTORY,
                options: Map[String, String] = Map()
              ): RabbitmqBuilder =
    ConnectionConfigWithTaskBuilder().rabbitmq(name, url, username, password, virtualHost, connectionFactory, options)

  /**
   * Create new RABBITMQ generation step with URL, username, password and virtual host. Default connection factory used
   *
   * @param name        Data source name
   * @param url         Rabbitmq url
   * @param username    Rabbitmq username
   * @param password    Rabbitmq password
   * @param virtualHost Virtual host in Rabbitmq
   * @return RabbitmqBuilder
   */
  def rabbitmqJava(name: String, url: String, username: String, password: String, virtualHost: String): RabbitmqBuilder =
    rabbitmq(name, url, username, password, virtualHost)

  /**
   * Create new RABBITMQ generation step with URL, username, password and virtual host. Default username, password,
   * virtual host, connection factory used
   *
   * @param name Data source name
   * @param url  Rabbitmq url
   * @return RabbitmqBuilder
   */
  def rabbitmqJava(name: String, url: String): RabbitmqBuilder = rabbitmq(name, url)


  /**
   * Create new SOLACE generation step with connection configuration
   *
   * @param name                  Data source name
   * @param url                   Solace url
   * @param username              Solace username
   * @param password              Solace password
   * @param vpnName               VPN name in Solace to connect to
   * @param connectionFactory     Connection factory
   * @param initialContextFactory Initial context factory
   * @param options               Additional connection options
   * @return SolaceBuilder
   */
  def solace(
              name: String,
              url: String = DEFAULT_SOLACE_URL,
              username: String = DEFAULT_SOLACE_USERNAME,
              password: String = DEFAULT_SOLACE_PASSWORD,
              vpnName: String = DEFAULT_SOLACE_VPN_NAME,
              connectionFactory: String = DEFAULT_SOLACE_CONNECTION_FACTORY,
              initialContextFactory: String = DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY,
              options: Map[String, String] = Map()
            ): SolaceBuilder =
    ConnectionConfigWithTaskBuilder().solace(name, url, username, password, vpnName, connectionFactory, initialContextFactory, options)


  /**
   * Create new SOLACE generation step with Solace URL, username, password and vpnName. Default connection factory and
   * initial context factory used
   *
   * @param name     Data source name
   * @param url      Solace url
   * @param username Solace username
   * @param password Solace password
   * @param vpnName  VPN name in Solace to connect to
   * @return SolaceBuilder
   */
  def solaceJava(name: String, url: String, username: String, password: String, vpnName: String): SolaceBuilder =
    solace(name, url, username, password, vpnName)


  /**
   * Create new SOLACE generation step with Solace URL. Other configurations are set to default values
   *
   * @param name Data source name
   * @param url  Solace url
   * @return SolaceBuilder
   */
  def solaceJava(name: String, url: String): SolaceBuilder = solace(name, url)


  /**
   * Create new Solace generation step using the same connection configuration from another SolaceBuilder
   *
   * @param connectionTaskBuilder Solace step with connection configuration
   * @return SolaceBuilder
   */
  def solace(connectionTaskBuilder: ConnectionTaskBuilder[SolaceBuilder]): SolaceBuilder =
    SolaceBuilder().fromBaseConfig(connectionTaskBuilder)

  /**
   * Create new KAFKA generation step with connection configuration
   *
   * @param name    Data source name
   * @param url     Kafka url
   * @param options Additional connection options
   * @return KafkaBuilder
   */
  def kafka(name: String, url: String = DEFAULT_KAFKA_URL, options: Map[String, String] = Map()): KafkaBuilder =
    ConnectionConfigWithTaskBuilder().kafka(name, url, options)


  /**
   * Create new KAFKA generation step with url
   *
   * @param name Data source name
   * @param url  Kafka url
   * @return KafkaBuilder
   */
  def kafkaJava(name: String, url: String): KafkaBuilder = kafka(name, url)

  /**
   * Create new Kafka generation step using the same connection configuration from another KafkaBuilder
   *
   * @param connectionTaskBuilder Kafka step with connection configuration
   * @return KafkaBuilder
   */
  def kafka(connectionTaskBuilder: ConnectionTaskBuilder[KafkaBuilder]): KafkaBuilder =
    KafkaBuilder().fromBaseConfig(connectionTaskBuilder)

  /**
   * Create new HTTP generation step using connection configuration
   *
   * @param name     Data source name
   * @param username HTTP username
   * @param password HTTP password
   * @param options  Additional connection options
   * @return HttpBuilder
   */
  def http(name: String, username: String = "", password: String = "", options: Map[String, String] = Map()): HttpBuilder =
    ConnectionConfigWithTaskBuilder().http(name, username, password, options)

  /**
   * Create new HTTP generation step without authentication
   *
   * @param name Data source name
   * @return HttpBuilder
   */
  def httpJava(name: String): HttpBuilder = http(name)

  /**
   * Create new HTTP generation step using the same connection configuration from another HttpBuilder
   *
   * @param connectionTaskBuilder Http step with connection configuration
   * @return HttpBuilder
   */
  def http(connectionTaskBuilder: ConnectionTaskBuilder[HttpBuilder]): HttpBuilder =
    HttpBuilder().fromBaseConfig(connectionTaskBuilder)


  /**
   * Execute with the following connections and tasks defined
   *
   * @param connectionTaskBuilder  First connection and task
   * @param connectionTaskBuilders Other connections and tasks
   */
  def execute(connectionTaskBuilder: ConnectionTaskBuilder[_], connectionTaskBuilders: ConnectionTaskBuilder[_]*): Unit = {
    execute(configuration, connectionTaskBuilder, connectionTaskBuilders: _*)
  }

  /**
   * Execute with non-default configurations for a set of tasks
   *
   * @param baseConfiguration      Runtime configurations
   * @param connectionTaskBuilder  First connection and task
   * @param connectionTaskBuilders Other connections and tasks
   */
  def execute(
               baseConfiguration: DataCatererConfigurationBuilder,
               connectionTaskBuilder: ConnectionTaskBuilder[_],
               connectionTaskBuilders: ConnectionTaskBuilder[_]*
             ): Unit = {
    execute(plan, baseConfiguration, List(), connectionTaskBuilder, connectionTaskBuilders: _*)
  }

  /**
   * Execute with non-default configurations with validations and tasks
   *
   * @param planBuilder            Plan to set high level task configurations
   * @param baseConfiguration      Runtime configurations
   * @param connectionTaskBuilder  First connection and task
   * @param connectionTaskBuilders Other connections and tasks
   */
  def execute(
               planBuilder: PlanBuilder,
               baseConfiguration: DataCatererConfigurationBuilder,
               connectionTaskBuilder: ConnectionTaskBuilder[_],
               connectionTaskBuilders: ConnectionTaskBuilder[_]*
             ): Unit = {
    execute(planBuilder, baseConfiguration, List(), connectionTaskBuilder, connectionTaskBuilders: _*)
  }

  /**
   * Execute with non-default configurations with validations and tasks. Validations have to be enabled before running
   * (see [[DataCatererConfigurationBuilder.enableValidation()]].
   *
   * @param planBuilder       Plan to set high level task configurations
   * @param baseConfiguration Runtime configurations
   * @param validations       Validations to run if enabled
   * @param connectionTask    First connection and task
   * @param connectionTasks   Other connections and tasks
   */
  @varargs def execute(
                        planBuilder: PlanBuilder,
                        baseConfiguration: DataCatererConfigurationBuilder,
                        validations: List[ValidationConfigurationBuilder],
                        connectionTask: ConnectionTaskBuilder[_],
                        connectionTasks: ConnectionTaskBuilder[_]*
                      ): Unit = {
    val allConnectionTasks = connectionTask +: connectionTasks
    //need to merge options of same data source name
    val connectionConfig = allConnectionTasks.groupBy(_.connectionConfigWithTaskBuilder.dataSourceName).map(x => {
      val options = x._2.map(_.connectionConfigWithTaskBuilder.options).reduce(_ ++ _)
      x._1 -> options
    })
    val withConnectionConfig = baseConfiguration.connectionConfig(connectionConfig)
    val allValidations = validations ++ getValidations(allConnectionTasks)
    val allTasks = allConnectionTasks.map(_.toTasksBuilder).filter(_.isDefined).map(_.get).toList

    _connectionTaskBuilders = allConnectionTasks
    execute(allTasks, planBuilder, withConnectionConfig, allValidations)
  }

  /**
   * Execute with set of tasks and default configurations
   *
   * @param tasks Tasks to generate data
   */
  def execute(tasks: TasksBuilder): Unit = execute(List(tasks))

  /**
   * Execute with plan and non-default configuration
   *
   * @param planBuilder   Plan to set high level task configurations
   * @param configuration Runtime configuration
   */
  def execute(planBuilder: PlanBuilder, configuration: DataCatererConfigurationBuilder): Unit = {
    execute(planBuilder.tasks, planBuilder, configuration)
  }

  /**
   * Execute with tasks, plan, runtime configurations and validations defined
   *
   * @param tasks         Set of generation tasks
   * @param plan          Plan to set high level task configurations
   * @param configuration Runtime configurations
   * @param validations   Validations on data sources
   */
  def execute(
               tasks: List[TasksBuilder] = List(),
               plan: PlanBuilder = PlanBuilder(),
               configuration: DataCatererConfigurationBuilder = DataCatererConfigurationBuilder(),
               validations: List[ValidationConfigurationBuilder] = List()
             ): Unit = {
    val taskToDataSource = tasks.flatMap(x => x.tasks.map(t => (t.name, x.dataSourceName, t)))
    val planWithTaskToDataSource = plan.taskSummaries(taskToDataSource.map(t => taskSummary.name(t._1).dataSource(t._2)): _*)

    _plan = planWithTaskToDataSource.plan
    _tasks = taskToDataSource.map(_._3)
    _configuration = configuration.build
    _validations = validations.map(_.validationConfiguration)
  }

  private def getValidations(allConnectionTasks: Seq[ConnectionTaskBuilder[_]]) = {
    val validationsByDataSource = allConnectionTasks.map(x => {
        val dataSource = x.connectionConfigWithTaskBuilder.dataSourceName
        val optValidation = x.step
          .flatMap(_.optValidation)
          .map(dsValid => {
            DataSourceValidationBuilder()
              .options(x.step.map(_.step.options).getOrElse(Map()) ++ x.connectionConfigWithTaskBuilder.options)
              .wait(dsValid.dataSourceValidation.waitCondition)
              .validations(dsValid.dataSourceValidation.validations: _*)
          })
        (dataSource, optValidation)
      })
      .filter(_._2.isDefined)
      .map(ds => (ds._1, validationConfig.addDataSourceValidation(ds._1, ds._2.get)))

    validationsByDataSource
      .groupBy(_._1)
      .map(x => {
        val dataSourceName = x._1
        val validationsToMerge = x._2.tail.flatMap(_._2.validationConfiguration.dataSources(dataSourceName))
        if (validationsToMerge.nonEmpty) {
          x._2.head._2.addDataSourceValidation(dataSourceName, validationsToMerge)
        } else {
          x._2.head._2
        }
      })
      .filter(vc => vc.validationConfiguration.dataSources.exists(_._2.nonEmpty))
  }
}

class BasePlanRun extends PlanRun
