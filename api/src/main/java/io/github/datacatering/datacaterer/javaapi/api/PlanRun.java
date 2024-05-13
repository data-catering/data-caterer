package io.github.datacatering.datacaterer.javaapi.api;


import io.github.datacatering.datacaterer.api.BasePlanRun;
import io.github.datacatering.datacaterer.api.ColumnValidationBuilder;
import io.github.datacatering.datacaterer.api.CombinationPreFilterBuilder;
import io.github.datacatering.datacaterer.api.CountBuilder;
import io.github.datacatering.datacaterer.api.DataCatererConfigurationBuilder;
import io.github.datacatering.datacaterer.api.DataSourceValidationBuilder;
import io.github.datacatering.datacaterer.api.FieldBuilder;
import io.github.datacatering.datacaterer.api.GeneratorBuilder;
import io.github.datacatering.datacaterer.api.MetadataSourceBuilder;
import io.github.datacatering.datacaterer.api.PlanBuilder;
import io.github.datacatering.datacaterer.api.PreFilterBuilder;
import io.github.datacatering.datacaterer.api.SchemaBuilder;
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

public abstract class PlanRun {

    private io.github.datacatering.datacaterer.api.PlanRun basePlanRun = new BasePlanRun();

    public io.github.datacatering.datacaterer.api.PlanRun getPlan() {
        return basePlanRun;
    }

    public PlanBuilder plan() {
        return new PlanBuilder();
    }

    public TaskSummaryBuilder taskSummary() {
        return new TaskSummaryBuilder();
    }

    public TasksBuilder tasks() {
        return new TasksBuilder();
    }

    public TaskBuilder task() {
        return new TaskBuilder();
    }

    public StepBuilder step() {
        return new StepBuilder();
    }

    public SchemaBuilder schema() {
        return new SchemaBuilder();
    }

    public FieldBuilder field() {
        return new FieldBuilder();
    }

    public GeneratorBuilder generator() {
        return new GeneratorBuilder();
    }

    public CountBuilder count() {
        return new CountBuilder();
    }

    public DataCatererConfigurationBuilder configuration() {
        return new DataCatererConfigurationBuilder();
    }

    public WaitConditionBuilder waitCondition() {
        return new WaitConditionBuilder();
    }

    public ValidationBuilder validation() {
        return new ValidationBuilder();
    }

    public CombinationPreFilterBuilder preFilterBuilder(ValidationBuilder validationBuilder) {
        return new PreFilterBuilder().filter(validationBuilder);
    }

    public ColumnValidationBuilder columnPreFilter(String column) {
        return new ValidationBuilder().col(column);
    }

    public DataSourceValidationBuilder dataSourceValidation() {
        return new DataSourceValidationBuilder();
    }

    public ValidationConfigurationBuilder validationConfig() {
        return new ValidationConfigurationBuilder();
    }

    public MetadataSourceBuilder metadataSource() {
        return new MetadataSourceBuilder();
    }

    public ForeignKeyRelation foreignField(String dataSource, String step, String column) {
        return new ForeignKeyRelation(dataSource, step, column);
    }

    public ForeignKeyRelation foreignField(String dataSource, String step, List<String> columns) {
        return new ForeignKeyRelation(dataSource, step, toScalaList(columns));
    }

    public ForeignKeyRelation foreignField(ConnectionTaskBuilder<?> connectionTaskBuilder, String step, List<String> columns) {
        return new ForeignKeyRelation(connectionTaskBuilder.connectionConfigWithTaskBuilder().dataSourceName(), step, toScalaList(columns));
    }

    public FileBuilder csv(
            String name, String path, Map<String, String> options
    ) {
        return basePlanRun.csv(name, path, toScalaMap(options));
    }

    public FileBuilder csv(String name, String path) {
        return csv(name, path, Collections.emptyMap());
    }


    public FileBuilder json(String name, String path, Map<String, String> options) {
        return basePlanRun.json(name, path, toScalaMap(options));
    }

    public FileBuilder json(String name, String path) {
        return json(name, path, Collections.emptyMap());
    }


    public FileBuilder orc(String name, String path, Map<String, String> options) {
        return basePlanRun.orc(name, path, toScalaMap(options));
    }

    public FileBuilder orc(String name, String path) {
        return orc(name, path, Collections.emptyMap());
    }

    public FileBuilder parquet(String name, String path, Map<String, String> options) {
        return basePlanRun.parquet(name, path, toScalaMap(options));
    }

    public FileBuilder parquet(String name, String path) {
        return parquet(name, path, Collections.emptyMap());
    }

    public PostgresBuilder postgres(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return basePlanRun.postgres(name, url, username, password, toScalaMap(options));
    }

    public PostgresBuilder postgres(String name, String url) {
        return basePlanRun.postgresJava(name, url);
    }

    public PostgresBuilder postgres(
            ConnectionTaskBuilder<PostgresBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.postgres(connectionTaskBuilder);
    }

    public MySqlBuilder mysql(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return basePlanRun.mysql(name, url, username, password, toScalaMap(options));
    }

    public MySqlBuilder mysql(String name, String url) {
        return basePlanRun.mysqlJava(name, url);
    }

    public MySqlBuilder mysql(
            ConnectionTaskBuilder<MySqlBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.mysql(connectionTaskBuilder);
    }

    public CassandraBuilder cassandra(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return basePlanRun.cassandra(name, url, username, password, toScalaMap(options));
    }

    public CassandraBuilder cassandra(String name, String url) {
        return basePlanRun.cassandraJava(name, url);
    }

    public CassandraBuilder cassandra(
            ConnectionTaskBuilder<CassandraBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.cassandra(connectionTaskBuilder);
    }

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

    public SolaceBuilder solace(
            String name,
            String url,
            String username,
            String password,
            String vpnName
    ) {
        return basePlanRun.solaceJava(name, url, username, password, vpnName);
    }

    public SolaceBuilder solace(String name, String url) {
        return basePlanRun.solaceJava(name, url);
    }

    public SolaceBuilder solace(
            ConnectionTaskBuilder<SolaceBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.solace(connectionTaskBuilder);
    }

    public KafkaBuilder kafka(String name, String url, Map<String, String> options) {
        return basePlanRun.kafka(name, url, toScalaMap(options));
    }

    public KafkaBuilder kafka(String name, String url) {
        return basePlanRun.kafkaJava(name, url);
    }

    public KafkaBuilder kafka(
            ConnectionTaskBuilder<KafkaBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.kafka(connectionTaskBuilder);
    }

    public HttpBuilder http(String name, String username, String password, Map<String, String> options) {
        return basePlanRun.http(name, username, password, toScalaMap(options));
    }

    public HttpBuilder http(String name, Map<String, String> options) {
        return basePlanRun.http(name, "", "", toScalaMap(options));
    }

    public HttpBuilder http(String name) {
        return basePlanRun.httpJava(name);
    }

    public HttpBuilder http(
            ConnectionTaskBuilder<HttpBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.http(connectionTaskBuilder);
    }


    public void execute(
            ConnectionTaskBuilder<?> connectionTaskBuilder,
            ConnectionTaskBuilder<?>... connectionTaskBuilders
    ) {
        execute(plan(), configuration(), Collections.emptyList(), connectionTaskBuilder, connectionTaskBuilders);
    }

    public void execute(
            DataCatererConfigurationBuilder configurationBuilder,
            ConnectionTaskBuilder<?> connectionTaskBuilder,
            ConnectionTaskBuilder<?>... connectionTaskBuilders
    ) {
        execute(plan(), configurationBuilder, Collections.emptyList(), connectionTaskBuilder, connectionTaskBuilders);
    }

    public void execute(
            PlanBuilder planBuilder,
            DataCatererConfigurationBuilder configurationBuilder,
            ConnectionTaskBuilder<?> connectionTaskBuilder,
            ConnectionTaskBuilder<?>... connectionTaskBuilders
    ) {
        execute(planBuilder, configurationBuilder, Collections.emptyList(), connectionTaskBuilder, connectionTaskBuilders);
    }

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

    public void execute(TasksBuilder tasks) {
        execute(List.of(tasks), plan(), configuration(), Collections.emptyList());
    }

    public void execute(DataCatererConfigurationBuilder configurationBuilder) {
        execute(Collections.emptyList(), plan(), configurationBuilder, Collections.emptyList());
    }

    public void execute(PlanBuilder plan, DataCatererConfigurationBuilder configuration) {
        execute(Collections.emptyList(), plan, configuration, Collections.emptyList());
    }

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
