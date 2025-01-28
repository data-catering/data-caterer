package io.github.datacatering.datacaterer.javaapi.api;


import io.github.datacatering.datacaterer.api.BasePlanRun;
import io.github.datacatering.datacaterer.api.CombinationPreFilterBuilder;
import io.github.datacatering.datacaterer.api.CountBuilder;
import io.github.datacatering.datacaterer.api.DataCatererConfigurationBuilder;
import io.github.datacatering.datacaterer.api.DataSourceValidationBuilder;
import io.github.datacatering.datacaterer.api.FieldBuilder;
import io.github.datacatering.datacaterer.api.FieldValidationBuilder;
import io.github.datacatering.datacaterer.api.GeneratorBuilder;
import io.github.datacatering.datacaterer.api.MetadataSourceBuilder;
import io.github.datacatering.datacaterer.api.PlanBuilder;
import io.github.datacatering.datacaterer.api.PreFilterBuilder;
import io.github.datacatering.datacaterer.api.StepBuilder;
import io.github.datacatering.datacaterer.api.TaskBuilder;
import io.github.datacatering.datacaterer.api.TaskSummaryBuilder;
import io.github.datacatering.datacaterer.api.TasksBuilder;
import io.github.datacatering.datacaterer.api.ValidationBuilder;
import io.github.datacatering.datacaterer.api.ValidationConfigurationBuilder;
import io.github.datacatering.datacaterer.api.WaitConditionBuilder;
import io.github.datacatering.datacaterer.api.connection.CassandraBuilder;
import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder;
import io.github.datacatering.datacaterer.api.connection.FileBuilder;
import io.github.datacatering.datacaterer.api.connection.HttpBuilder;
import io.github.datacatering.datacaterer.api.connection.KafkaBuilder;
import io.github.datacatering.datacaterer.api.connection.MySqlBuilder;
import io.github.datacatering.datacaterer.api.connection.PostgresBuilder;
import io.github.datacatering.datacaterer.api.connection.SolaceBuilder;
import io.github.datacatering.datacaterer.api.model.ForeignKeyRelation;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.github.datacatering.datacaterer.api.converter.Converters.toScalaList;
import static io.github.datacatering.datacaterer.api.converter.Converters.toScalaMap;

/*
 * An abstract class representing a plan run.
 */
public abstract class PlanRun {

    private io.github.datacatering.datacaterer.api.PlanRun basePlanRun = new BasePlanRun();

    /**
     * Gets the base PlanRun instance.
     *
     * @return The base PlanRun instance.
     */
    public io.github.datacatering.datacaterer.api.PlanRun getPlan() {
        return basePlanRun;
    }

    /**
     * Creates a PlanBuilder instance.
     *
     * @return A PlanBuilder instance.
     */
    public PlanBuilder plan() {
        return new PlanBuilder();
    }

    /**
     * Creates a TaskSummaryBuilder instance.
     *
     * @return A TaskSummaryBuilder instance.
     */
    public TaskSummaryBuilder taskSummary() {
        return new TaskSummaryBuilder();
    }

    /**
     * Creates a TasksBuilder instance.
     *
     * @return A TasksBuilder instance.
     */
    public TasksBuilder tasks() {
        return new TasksBuilder();
    }

    /**
     * Creates a TaskBuilder instance.
     *
     * @return A TaskBuilder instance.
     */
    public TaskBuilder task() {
        return new TaskBuilder();
    }

    /**
     * Creates a StepBuilder instance.
     *
     * @return A StepBuilder instance.
     */
    public StepBuilder step() {
        return new StepBuilder();
    }

    /**
     * Creates a FieldBuilder instance.
     *
     * @return A FieldBuilder instance.
     */
    public FieldBuilder field() {
        return new FieldBuilder();
    }

    /**
     * Creates a GeneratorBuilder instance.
     *
     * @return A GeneratorBuilder instance.
     */
    public GeneratorBuilder generator() {
        return new GeneratorBuilder();
    }

    /**
     * Creates a CountBuilder instance.
     *
     * @return A CountBuilder instance.
     */
    public CountBuilder count() {
        return new CountBuilder();
    }

    /**
     * Creates a DataCatererConfigurationBuilder instance.
     *
     * @return A DataCatererConfigurationBuilder instance.
     */
    public DataCatererConfigurationBuilder configuration() {
        return new DataCatererConfigurationBuilder();
    }

    /**
     * Creates a WaitConditionBuilder instance.
     *
     * @return A WaitConditionBuilder instance.
     */
    public WaitConditionBuilder waitCondition() {
        return new WaitConditionBuilder();
    }

    /**
     * Creates a ValidationBuilder instance.
     *
     * @return A ValidationBuilder instance.
     */
    public ValidationBuilder validation() {
        return new ValidationBuilder();
    }

    /**
     * Creates a CombinationPreFilterBuilder instance with the provided ValidationBuilder.
     *
     * @param validationBuilder The ValidationBuilder instance.
     * @return A CombinationPreFilterBuilder instance.
     */
    public CombinationPreFilterBuilder preFilterBuilder(ValidationBuilder validationBuilder) {
        return new PreFilterBuilder().filter(validationBuilder);
    }

    /**
     * Creates a FieldValidationBuilder instance for the specified field.
     *
     * @param field The name of the field.
     * @return A FieldValidationBuilder instance for the specified field.
     */
    public FieldValidationBuilder fieldPreFilter(String field) {
        return new ValidationBuilder().field(field);
    }

    /**
     * Creates a DataSourceValidationBuilder instance.
     *
     * @return A DataSourceValidationBuilder instance.
     */
    public DataSourceValidationBuilder dataSourceValidation() {
        return new DataSourceValidationBuilder();
    }

    /**
     * Creates a ValidationConfigurationBuilder instance.
     *
     * @return A ValidationConfigurationBuilder instance.
     */
    public ValidationConfigurationBuilder validationConfig() {
        return new ValidationConfigurationBuilder();
    }

    /**
     * Creates a MetadataSourceBuilder instance.
     *
     * @return A MetadataSourceBuilder instance.
     */
    public MetadataSourceBuilder metadataSource() {
        return new MetadataSourceBuilder();
    }

    /**
     * Creates a ForeignKeyRelation instance with the provided data source, step, and field.
     *
     * @param dataSource The name of the data source.
     * @param step       The step associated with the ForeignKeyRelation.
     * @param field      The field for the ForeignKeyRelation.
     * @return A ForeignKeyRelation instance.
     */
    public ForeignKeyRelation foreignField(String dataSource, String step, String field) {
        return new ForeignKeyRelation(dataSource, step, field);
    }

    /**
     * Creates a ForeignKeyRelation instance with the provided data source, step, and fields.
     *
     * @param dataSource The name of the data source.
     * @param step       The step associated with the ForeignKeyRelation.
     * @param fields     The list of fields for the ForeignKeyRelation.
     * @return A ForeignKeyRelation instance.
     */
    public ForeignKeyRelation foreignField(String dataSource, String step, List<String> fields) {
        return new ForeignKeyRelation(dataSource, step, toScalaList(fields));
    }

    /**
     * Creates a ForeignKeyRelation instance with the provided ConnectionTaskBuilder and field.
     * It assumes there is only one step inside the ConnectionTaskBuilder.
     *
     * @param connectionTaskBuilder The ConnectionTaskBuilder instance representing the task.
     * @param field                 The list of fields for the ForeignKeyRelation.
     * @return A ForeignKeyRelation instance.
     */
    public ForeignKeyRelation foreignField(ConnectionTaskBuilder<?> connectionTaskBuilder, String field) {
        return new ForeignKeyRelation(
                connectionTaskBuilder.connectionConfigWithTaskBuilder().dataSourceName(),
                connectionTaskBuilder.getStep().step().name(),
                toScalaList(List.of(field))
        );
    }

    /**
     * Creates a ForeignKeyRelation instance with the provided ConnectionTaskBuilder and fields.
     * It assumes there is only one step inside the ConnectionTaskBuilder.
     *
     * @param connectionTaskBuilder The ConnectionTaskBuilder instance representing the task.
     * @param fields                The list of fields for the ForeignKeyRelation.
     * @return A ForeignKeyRelation instance.
     */
    public ForeignKeyRelation foreignField(ConnectionTaskBuilder<?> connectionTaskBuilder, List<String> fields) {
        return new ForeignKeyRelation(
                connectionTaskBuilder.connectionConfigWithTaskBuilder().dataSourceName(),
                connectionTaskBuilder.getStep().step().name(),
                toScalaList(fields)
        );
    }

    /**
     * Creates a ForeignKeyRelation instance with the provided ConnectionTaskBuilder, step, and fields.
     *
     * @param connectionTaskBuilder The ConnectionTaskBuilder instance representing the task.
     * @param step                  The step associated with the ForeignKeyRelation.
     * @param fields                The list of fields for the ForeignKeyRelation.
     * @return A ForeignKeyRelation instance.
     */
    public ForeignKeyRelation foreignField(ConnectionTaskBuilder<?> connectionTaskBuilder, String step, List<String> fields) {
        return new ForeignKeyRelation(connectionTaskBuilder.connectionConfigWithTaskBuilder().dataSourceName(), step, toScalaList(fields));
    }

    /**
     * Creates a FileBuilder instance for a CSV file with the provided name, path, and options.
     *
     * @param name    The name of the FileBuilder instance.
     * @param path    The path to the CSV file.
     * @param options A map of options to be used for the CSV file.
     * @return A FileBuilder instance for the CSV file.
     */
    public FileBuilder csv(
            String name, String path, Map<String, String> options
    ) {
        return basePlanRun.csv(name, path, toScalaMap(options));
    }

    /**
     * Creates a FileBuilder instance for a CSV file with the provided name and path.
     *
     * @param name The name of the FileBuilder instance.
     * @param path The path to the CSV file.
     * @return A FileBuilder instance for the CSV file.
     */
    public FileBuilder csv(String name, String path) {
        return csv(name, path, Collections.emptyMap());
    }

    /**
     * Creates a FileBuilder instance for a JSON file with the provided name, path, and options.
     *
     * @param name    The name of the FileBuilder instance.
     * @param path    The path to the JSON file.
     * @param options A map of options to be used for the JSON file.
     * @return A FileBuilder instance for the JSON file.
     */
    public FileBuilder json(String name, String path, Map<String, String> options) {
        return basePlanRun.json(name, path, toScalaMap(options));
    }

    /**
     * Creates a FileBuilder instance for a JSON file with the provided name and path.
     *
     * @param name The name of the FileBuilder instance.
     * @param path The path to the JSON file.
     * @return A FileBuilder instance for the JSON file.
     */
    public FileBuilder json(String name, String path) {
        return json(name, path, Collections.emptyMap());
    }

    /**
     * Creates a FileBuilder instance for an ORC file with the provided name, path, and options.
     *
     * @param name    The name of the FileBuilder instance.
     * @param path    The path to the ORC file.
     * @param options A map of options to be used for the ORC file.
     * @return A FileBuilder instance for the ORC file.
     */
    public FileBuilder orc(String name, String path, Map<String, String> options) {
        return basePlanRun.orc(name, path, toScalaMap(options));
    }

    /**
     * Creates a FileBuilder instance for an ORC file with the provided name and path.
     *
     * @param name The name of the FileBuilder instance.
     * @param path The path to the ORC file.
     * @return A FileBuilder instance for the ORC file.
     */
    public FileBuilder orc(String name, String path) {
        return orc(name, path, Collections.emptyMap());
    }

    /**
     * Creates a FileBuilder instance for a Parquet file with the provided name, path, and options.
     *
     * @param name    The name of the FileBuilder instance.
     * @param path    The path to the Parquet file.
     * @param options A map of options to be used for the Parquet file.
     * @return A FileBuilder instance for the Parquet file.
     */
    public FileBuilder parquet(String name, String path, Map<String, String> options) {
        return basePlanRun.parquet(name, path, toScalaMap(options));
    }

    /**
     * Creates a FileBuilder instance for a Parquet file with the provided name and path.
     *
     * @param name The name of the FileBuilder instance.
     * @param path The path to the Parquet file.
     * @return A FileBuilder instance for the Parquet file.
     */
    public FileBuilder parquet(String name, String path) {
        return parquet(name, path, Collections.emptyMap());
    }

    /**
     * Creates a FileBuilder instance for an Iceberg table with the provided name, path, and table name.
     *
     * @param name      The name of the FileBuilder instance.
     * @param path      The path to the Iceberg table.
     * @param tableName The name of the Iceberg table.
     * @return A FileBuilder instance for the Iceberg table.
     */

    public FileBuilder iceberg(String name, String tableName, String path) {
        return basePlanRun.icebergJava(name, tableName, path);
    }

    /**
     * Creates a FileBuilder instance for an Iceberg table with the provided parameters.
     *
     * @param name        The name of the FileBuilder instance.
     * @param tableName   The name of the Iceberg table.
     * @param path        The path to the Iceberg table.
     * @param catalogType The type of the Iceberg catalog.
     * @param catalogUri  The URI of the Iceberg catalog.
     * @param options     A map of options to be used for the Iceberg table.
     * @return A FileBuilder instance for the Iceberg table.
     */
    public FileBuilder iceberg(
            String name,
            String tableName,
            String path,
            String catalogType,
            String catalogUri,
            Map<String, String> options
    ) {
        return basePlanRun.iceberg(name, tableName, path, catalogType, catalogUri, toScalaMap(options));
    }

    /**
     * Creates a FileBuilder instance for a Delta Lake file with the provided name, path, and options.
     *
     * @param name    The name of the FileBuilder instance.
     * @param path    The path to the Delta Lake file.
     * @param options A map of options to be used for the Delta Lake file.
     * @return A FileBuilder instance for the Delta Lake file.
     */
    public FileBuilder delta(String name, String path, Map<String, String> options) {
        return basePlanRun.delta(name, path, toScalaMap(options));
    }

    /**
     * Creates a FileBuilder instance for a Delta Lake file with the provided name and path.
     *
     * @param name The name of the FileBuilder instance.
     * @param path The path to the Delta Lake file.
     * @return A FileBuilder instance for the Delta Lake file.
     */
    public FileBuilder delta(String name, String path) {
        return delta(name, path, Collections.emptyMap());
    }

    /**
     * Creates a PostgresBuilder instance with the provided name, URL, username, password, and options.
     *
     * @param name     The name of the PostgresBuilder instance.
     * @param url      The URL for the PostgresBuilder.
     * @param username The username for the PostgresBuilder.
     * @param password The password for the PostgresBuilder.
     * @param options  A map of options to be used for the PostgresBuilder.
     * @return A PostgresBuilder instance.
     */
    public PostgresBuilder postgres(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return basePlanRun.postgres(name, url, username, password, toScalaMap(options));
    }

    /**
     * Creates a PostgresBuilder instance with the provided name and URL.
     *
     * @param name The name of the PostgresBuilder instance.
     * @param url  The URL for the PostgresBuilder.
     * @return A PostgresBuilder instance.
     */
    public PostgresBuilder postgres(String name, String url) {
        return basePlanRun.postgresJava(name, url);
    }

    /**
     * Creates a PostgresBuilder instance from the provided ConnectionTaskBuilder.
     *
     * @param connectionTaskBuilder The ConnectionTaskBuilder object representing the task to build a PostgresBuilder.
     * @return A PostgresBuilder instance.
     */
    public PostgresBuilder postgres(
            ConnectionTaskBuilder<PostgresBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.postgres(connectionTaskBuilder);
    }

    /**
     * Creates a MySqlBuilder instance with the provided name, URL, username, password, and options.
     *
     * @param name     The name of the MySqlBuilder instance.
     * @param url      The URL for the MySqlBuilder.
     * @param username The username for the MySqlBuilder.
     * @param password The password for the MySqlBuilder.
     * @param options  A map of options to be used for the MySqlBuilder.
     * @return A MySqlBuilder instance.
     */
    public MySqlBuilder mysql(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return basePlanRun.mysql(name, url, username, password, toScalaMap(options));
    }

    /**
     * Creates a MySqlBuilder instance with the provided name and URL.
     *
     * @param name The name of the MySqlBuilder instance.
     * @param url  The URL for the MySqlBuilder.
     * @return A MySqlBuilder instance.
     */
    public MySqlBuilder mysql(String name, String url) {
        return basePlanRun.mysqlJava(name, url);
    }

    /**
     * Creates a MySqlBuilder instance from the provided ConnectionTaskBuilder.
     *
     * @param connectionTaskBuilder The ConnectionTaskBuilder object representing the task to build a MySqlBuilder.
     * @return A MySqlBuilder instance.
     */
    public MySqlBuilder mysql(
            ConnectionTaskBuilder<MySqlBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.mysql(connectionTaskBuilder);
    }

    /**
     * Creates a CassandraBuilder instance with the provided name, URL, username, password, and options.
     *
     * @param name     The name of the CassandraBuilder instance.
     * @param url      The URL for the CassandraBuilder.
     * @param username The username for the CassandraBuilder.
     * @param password The password for the CassandraBuilder.
     * @param options  A map of options to be used for the CassandraBuilder.
     * @return A CassandraBuilder instance.
     */
    public CassandraBuilder cassandra(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return basePlanRun.cassandra(name, url, username, password, toScalaMap(options));
    }

    /**
     * Creates a CassandraBuilder instance with the provided name and URL.
     *
     * @param name The name of the CassandraBuilder instance.
     * @param url  The URL for the CassandraBuilder.
     * @return A CassandraBuilder instance.
     */
    public CassandraBuilder cassandra(String name, String url) {
        return basePlanRun.cassandraJava(name, url);
    }

    /**
     * Creates a CassandraBuilder instance from the provided ConnectionTaskBuilder.
     *
     * @param connectionTaskBuilder The ConnectionTaskBuilder object representing the task to build a CassandraBuilder.
     * @return A CassandraBuilder instance.
     */
    public CassandraBuilder cassandra(
            ConnectionTaskBuilder<CassandraBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.cassandra(connectionTaskBuilder);
    }

    /**
     * Creates a SolaceBuilder instance with the provided name, URL, username, password, VPN name, connection factory, initial context factory, and options.
     *
     * @param name                  The name of the SolaceBuilder instance.
     * @param url                   The URL for the SolaceBuilder.
     * @param username              The username for the SolaceBuilder.
     * @param password              The password for the SolaceBuilder.
     * @param vpnName               The VPN name for the SolaceBuilder.
     * @param connectionFactory     The connection factory for the SolaceBuilder.
     * @param initialContextFactory The initial context factory for the SolaceBuilder.
     * @param options               A map of options to be used for the SolaceBuilder.
     * @return A SolaceBuilder instance.
     */
    public SolaceBuilder solace(
            String name,
            String url,
            String username,
            String password,
            String vpnName,
            String connectionFactory,
            String initialContextFactory,
            Map<String, String> options
    ) {
        return basePlanRun.solace(name, url, username, password, vpnName, connectionFactory, initialContextFactory, toScalaMap(options));
    }

    /**
     * Creates a SolaceBuilder instance with the provided name, URL, username, password, and VPN name.
     *
     * @param name     The name of the SolaceBuilder instance.
     * @param url      The URL for the SolaceBuilder.
     * @param username The username for the SolaceBuilder.
     * @param password The password for the SolaceBuilder.
     * @param vpnName  The VPN name for the SolaceBuilder.
     * @return A SolaceBuilder instance.
     */
    public SolaceBuilder solace(
            String name,
            String url,
            String username,
            String password,
            String vpnName
    ) {
        return basePlanRun.solaceJava(name, url, username, password, vpnName);
    }

    /**
     * Creates a SolaceBuilder instance with the provided name and URL.
     *
     * @param name The name of the SolaceBuilder instance.
     * @param url  The URL for the SolaceBuilder.
     * @return A SolaceBuilder instance.
     */
    public SolaceBuilder solace(String name, String url) {
        return basePlanRun.solaceJava(name, url);
    }

    /**
     * Creates a SolaceBuilder instance from the provided ConnectionTaskBuilder.
     *
     * @param connectionTaskBuilder The ConnectionTaskBuilder object representing the task to build a SolaceBuilder.
     * @return A SolaceBuilder instance.
     */
    public SolaceBuilder solace(
            ConnectionTaskBuilder<SolaceBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.solace(connectionTaskBuilder);
    }

    /**
     * Creates a KafkaBuilder instance with the provided name, URL, and options.
     *
     * @param name    The name of the KafkaBuilder instance.
     * @param url     The URL for the KafkaBuilder.
     * @param options A map of options to be used for the KafkaBuilder.
     * @return A KafkaBuilder instance.
     */
    public KafkaBuilder kafka(String name, String url, Map<String, String> options) {
        return basePlanRun.kafka(name, url, toScalaMap(options));
    }

    /**
     * Creates a KafkaBuilder instance with the provided name and URL.
     *
     * @param name The name of the KafkaBuilder instance.
     * @param url  The URL for the KafkaBuilder.
     * @return A KafkaBuilder instance.
     */
    public KafkaBuilder kafka(String name, String url) {
        return basePlanRun.kafkaJava(name, url);
    }

    /**
     * Creates a KafkaBuilder instance from the provided ConnectionTaskBuilder.
     *
     * @param connectionTaskBuilder The ConnectionTaskBuilder object representing the task to build a KafkaBuilder.
     * @return A KafkaBuilder instance.
     */
    public KafkaBuilder kafka(
            ConnectionTaskBuilder<KafkaBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.kafka(connectionTaskBuilder);
    }

    /**
     * Creates an HttpBuilder instance with the provided name, username, password, and options.
     *
     * @param name     The name of the HttpBuilder instance.
     * @param username The username for the HttpBuilder.
     * @param password The password for the HttpBuilder.
     * @param options  A map of options to be used for the HttpBuilder.
     * @return An HttpBuilder instance.
     */
    public HttpBuilder http(String name, String username, String password, Map<String, String> options) {
        return basePlanRun.http(name, username, password, toScalaMap(options));
    }

    /**
     * Creates an HttpBuilder instance with the provided name and options.
     *
     * @param name    The name of the HttpBuilder instance.
     * @param options A map of options to be used for the HttpBuilder.
     * @return An HttpBuilder instance.
     */
    public HttpBuilder http(String name, Map<String, String> options) {
        return basePlanRun.http(name, "", "", toScalaMap(options));
    }

    /**
     * Creates an HttpBuilder instance with the provided name.
     *
     * @param name The name of the HttpBuilder instance.
     * @return An HttpBuilder instance.
     */
    public HttpBuilder http(String name) {
        return basePlanRun.httpJava(name);
    }

    /**
     * Creates an HttpBuilder instance from the provided ConnectionTaskBuilder.
     *
     * @param connectionTaskBuilder The ConnectionTaskBuilder object representing the task to build an HttpBuilder.
     * @return An HttpBuilder instance.
     */
    public HttpBuilder http(
            ConnectionTaskBuilder<HttpBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.http(connectionTaskBuilder);
    }

    /**
     * Executes the data catering plan with the provided tasks.
     *
     * @param connectionTaskBuilder  The ConnectionTaskBuilder object representing the task to be executed.
     * @param connectionTaskBuilders A variable-length argument list of ConnectionTaskBuilder objects representing additional tasks to be executed.
     */
    public void execute(
            ConnectionTaskBuilder<?> connectionTaskBuilder,
            ConnectionTaskBuilder<?>... connectionTaskBuilders
    ) {
        execute(plan(), configuration(), Collections.emptyList(), connectionTaskBuilder, connectionTaskBuilders);
    }


    /**
     * Executes the data catering plan with the provided configurations.
     *
     * @param configurationBuilder   The DataCatererConfigurationBuilder object representing the configuration for the plan execution.
     * @param connectionTaskBuilder  The ConnectionTaskBuilder object representing the task to be executed.
     * @param connectionTaskBuilders A variable-length argument list of ConnectionTaskBuilder objects representing additional tasks to be executed.
     */
    public void execute(
            DataCatererConfigurationBuilder configurationBuilder,
            ConnectionTaskBuilder<?> connectionTaskBuilder,
            ConnectionTaskBuilder<?>... connectionTaskBuilders
    ) {
        execute(plan(), configurationBuilder, Collections.emptyList(), connectionTaskBuilder, connectionTaskBuilders);
    }

    /**
     * Executes the data catering plan with the provided configurations.
     *
     * @param planBuilder            The PlanBuilder object representing the plan to be executed.
     * @param configurationBuilder   The DataCatererConfigurationBuilder object representing the configuration for the plan execution.
     * @param connectionTaskBuilder  The ConnectionTaskBuilder object representing the task to be executed.
     * @param connectionTaskBuilders A variable-length argument list of ConnectionTaskBuilder objects representing additional tasks to be executed.
     */
    public void execute(
            PlanBuilder planBuilder,
            DataCatererConfigurationBuilder configurationBuilder,
            ConnectionTaskBuilder<?> connectionTaskBuilder,
            ConnectionTaskBuilder<?>... connectionTaskBuilders
    ) {
        execute(planBuilder, configurationBuilder, Collections.emptyList(), connectionTaskBuilder, connectionTaskBuilders);
    }

    /**
     * Executes the data catering plan with the provided configurations.
     *
     * @param planBuilder            The PlanBuilder object representing the plan to be executed.
     * @param configurationBuilder   The DataCatererConfigurationBuilder object representing the configuration for the plan execution.
     * @param validations            A list of ValidationConfigurationBuilder objects representing the validations to be performed during the plan execution.
     * @param connectionTaskBuilder  The ConnectionTaskBuilder object representing the task to be executed.
     * @param connectionTaskBuilders A variable-length argument list of ConnectionTaskBuilder objects representing additional tasks to be executed.
     */
    public void execute(
            PlanBuilder planBuilder,
            DataCatererConfigurationBuilder configurationBuilder,
            List<ValidationConfigurationBuilder> validations,
            ConnectionTaskBuilder<?> connectionTaskBuilder,
            ConnectionTaskBuilder<?>... connectionTaskBuilders
    ) {
        var planWithConfig = getPlan();
        planWithConfig.execute(
                planBuilder,
                configurationBuilder,
                toScalaList(validations),
                connectionTaskBuilder,
                connectionTaskBuilders
        );
        this.basePlanRun = planWithConfig;
    }

    /**
     * Executes the data catering plan with the provided configurations.
     *
     * @param tasks A list of TasksBuilder objects representing the tasks to be executed.
     */
    public void execute(TasksBuilder tasks) {
        execute(List.of(tasks), plan(), configuration(), Collections.emptyList());
    }

    /**
     * Executes the data catering plan with the provided configurations.
     *
     * @param configurationBuilder The DataCatererConfigurationBuilder object representing the configuration for the plan execution.
     */
    public void execute(DataCatererConfigurationBuilder configurationBuilder) {
        execute(Collections.emptyList(), plan(), configurationBuilder, Collections.emptyList());
    }

    /**
     * Executes the data catering plan with the provided configurations.
     *
     * @param plan          The PlanBuilder object representing the plan to be executed.
     * @param configuration The DataCatererConfigurationBuilder object representing the configuration for the plan execution.
     */
    public void execute(PlanBuilder plan, DataCatererConfigurationBuilder configuration) {
        execute(Collections.emptyList(), plan, configuration, Collections.emptyList());
    }

    /**
     * Executes the data catering plan with the provided configurations.
     *
     * @param tasks         A list of TasksBuilder objects representing the tasks to be executed.
     * @param plan          The PlanBuilder object representing the plan to be executed.
     * @param configuration The DataCatererConfigurationBuilder object representing the configuration for the plan execution.
     * @param validations   A list of ValidationConfigurationBuilder objects representing the validations to be performed during the plan execution.
     */
    public void execute(
            List<TasksBuilder> tasks,
            PlanBuilder plan,
            DataCatererConfigurationBuilder configuration,
            List<ValidationConfigurationBuilder> validations
    ) {
        var planWithConfig = getPlan();
        planWithConfig.execute(
                toScalaList(tasks),
                plan,
                configuration,
                toScalaList(validations)
        );
        this.basePlanRun = planWithConfig;
    }

}
