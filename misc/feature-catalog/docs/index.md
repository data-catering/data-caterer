# Data Caterer Feature Catalog

Complete reference for all features in Data Caterer.

- **Version**: 0.19.0
- **Total Features**: 169
- **Last Updated**: 2026-02-11
- **Repository**: https://github.com/data-catering/data-caterer

## Categories

### [Data Source Connectors](categories/connectors.md)
16 features | Data Caterer supports connecting to databases, file systems, messaging systems, and HTTP APIs for reading and writing test data.

### [Data Generation](categories/generation.md)
55 features | Comprehensive data generation capabilities including regex patterns, faker expressions, SQL computations, and field-level configuration options.

### [Data Validation](categories/validation.md)
42 features | Over 30 validation types for verifying generated data quality, schema compliance, statistical properties, and cross-source consistency.

### [Configuration](categories/configuration.md)
29 features | Runtime configuration for controlling generation behavior, validation, performance tuning, alerts, and output paths.

### [Advanced Features](categories/advanced.md)
11 features | Foreign key relationships, streaming load patterns, custom transformations, metadata-driven generation, and more.

### [Metadata Integration](categories/metadata.md)
10 features | Import schemas and metadata from external catalogs, registries, and standards to drive automatic data generation.

### [UI and API](categories/ui-api.md)
6 features | Web-based user interface for visual plan creation, execution management, and result viewing.

## Feature Summary

| Category | Features | Description |
|----------|----------|-------------|
| [Data Source Connectors](categories/connectors.md) | 16 | Data Caterer supports connecting to databases, file systems, messaging systems, ... |
| [Data Generation](categories/generation.md) | 55 | Comprehensive data generation capabilities including regex patterns, faker expre... |
| [Data Validation](categories/validation.md) | 42 | Over 30 validation types for verifying generated data quality, schema compliance... |
| [Configuration](categories/configuration.md) | 29 | Runtime configuration for controlling generation behavior, validation, performan... |
| [Advanced Features](categories/advanced.md) | 11 | Foreign key relationships, streaming load patterns, custom transformations, meta... |
| [Metadata Integration](categories/metadata.md) | 10 | Import schemas and metadata from external catalogs, registries, and standards to... |
| [UI and API](categories/ui-api.md) | 6 | Web-based user interface for visual plan creation, execution management, and res... |

**Total: 169 features**

## All Features (Alphabetical)

- [Address Label](categories/generation.md#address-label) - `generation.label.address`
- [Alert Configuration](categories/configuration.md#alert-configuration) - `configuration.alerts`
- [Alerts](categories/configuration.md#alerts) - `configuration.flags.enablealerts`
- [Amundsen Integration](categories/metadata.md#amundsen-integration) - `metadata.source.amundsen`
- [Apache Hudi Connector](categories/connectors.md#apache-hudi-connector) - `connector.files.hudi`
- [Apache Iceberg Connector](categories/connectors.md#apache-iceberg-connector) - `connector.files.iceberg`
- [Apache Kafka Connector](categories/connectors.md#apache-kafka-connector) - `connector.messaging.kafka`
- [Apache Spark Configuration](categories/configuration.md#apache-spark-configuration) - `configuration.runtime.spark`
- [Application Label](categories/generation.md#application-label) - `generation.label.app`
- [Array Configuration](categories/generation.md#array-configuration) - `generation.option.array_config`
- [Array Type](categories/generation.md#array-type) - `generation.type.array`
- [Auto-Generate Plan and Tasks](categories/configuration.md#auto-generate-plan-and-tasks) - `configuration.flags.enablegenerateplanandtasks`
- [Batch Size](categories/configuration.md#batch-size) - `configuration.generation.batch_size`
- [Between Range](categories/validation.md#between-range) - `validation.field.between`
- [BigQuery Connector](categories/connectors.md#bigquery-connector) - `connector.databases.bigquery`
- [Binary Type](categories/generation.md#binary-type) - `generation.type.binary`
- [Bloom Filter Configuration](categories/configuration.md#bloom-filter-configuration) - `configuration.generation.bloom_filter`
- [Boolean Type](categories/generation.md#boolean-type) - `generation.type.boolean`
- [Cassandra Connector](categories/connectors.md#cassandra-connector) - `connector.databases.cassandra`
- [Cassandra Key Configuration](categories/generation.md#cassandra-key-configuration) - `generation.option.cassandra_keys`
- [Conditional Value Generation](categories/generation.md#conditional-value-generation) - `generation.field.conditional_value`
- [Configuration Interfaces](categories/advanced.md#configuration-interfaces) - `advanced.interfaces`
- [Confluent Schema Registry Integration](categories/metadata.md#confluent-schema-registry-integration) - `metadata.source.confluent_schema_registry`
- [Connection Management](categories/ui-api.md#connection-management) - `ui.connection_management`
- [Contains Check](categories/validation.md#contains-check) - `validation.field.contains`
- [Correlated Field Generation](categories/generation.md#correlated-field-generation) - `generation.field.correlated`
- [Count Records](categories/configuration.md#count-records) - `configuration.flags.enablecount`
- [CSV File Connector](categories/connectors.md#csv-file-connector) - `connector.files.csv`
- [Daily Batch Sequence](categories/generation.md#daily-batch-sequence) - `generation.field.daily_batch_sequence`
- [Data Contract CLI Integration](categories/metadata.md#data-contract-cli-integration) - `metadata.source.data_contract_cli`
- [Data Validation](categories/configuration.md#data-validation) - `configuration.flags.enablevalidation`
- [DataFaker Expression](categories/generation.md#datafaker-expression) - `generation.field.expression`
- [DataHub Integration](categories/metadata.md#datahub-integration) - `metadata.source.datahub`
- [Date Type](categories/generation.md#date-type) - `generation.type.date`
- [Date/Time Range](categories/generation.md#date-time-range) - `generation.option.date_range`
- [DateTime Format Match](categories/validation.md#datetime-format-match) - `validation.field.match_date_time_format`
- [Decimal Type](categories/generation.md#decimal-type) - `generation.type.decimal`
- [Delete Generated Records](categories/configuration.md#delete-generated-records) - `configuration.flags.enabledeletegeneratedrecords`
- [Delta Lake Connector](categories/connectors.md#delta-lake-connector) - `connector.files.delta`
- [Distinct Contains Set](categories/validation.md#distinct-contains-set) - `validation.field.distinct_contains_set`
- [Distinct Equal](categories/validation.md#distinct-equal) - `validation.field.distinct_equal`
- [Distinct In Set](categories/validation.md#distinct-in-set) - `validation.field.distinct_in_set`
- [Distinct Value Count](categories/generation.md#distinct-value-count) - `generation.option.distinct_count`
- [Double Type](categories/generation.md#double-type) - `generation.type.double`
- [Edge Case Generation](categories/generation.md#edge-case-generation) - `generation.option.edge_cases`
- [Ends With](categories/validation.md#ends-with) - `validation.field.ends_with`
- [Environment Variable Substitution](categories/advanced.md#environment-variable-substitution) - `advanced.env_substitution`
- [Equality Check](categories/validation.md#equality-check) - `validation.field.equal`
- [Execution History](categories/ui-api.md#execution-history) - `ui.execution_history`
- [Fail on Error](categories/configuration.md#fail-on-error) - `configuration.flags.enablefailonerror`
- [Fast Generation](categories/configuration.md#fast-generation) - `configuration.flags.enablefastgeneration`
- [Field Omission](categories/generation.md#field-omission) - `generation.option.omit`
- [Float Type](categories/generation.md#float-type) - `generation.type.float`
- [Food Label](categories/generation.md#food-label) - `generation.label.food`
- [Foreign Key Cardinality Control](categories/advanced.md#foreign-key-cardinality-control) - `advanced.foreign_key_cardinality`
- [Foreign Key Generation Modes](categories/advanced.md#foreign-key-generation-modes) - `advanced.foreign_key_generation_modes`
- [Foreign Key Nullability](categories/advanced.md#foreign-key-nullability) - `advanced.foreign_key_nullability`
- [Foreign Key Relationships](categories/advanced.md#foreign-key-relationships) - `advanced.foreign_keys`
- [Generate Data](categories/configuration.md#generate-data) - `configuration.flags.enablegeneratedata`
- [generatedPlanAndTask Folder](categories/configuration.md#generatedplanandtask-folder) - `configuration.folders.generatedplanandtaskfolderpath`
- [generatedReports Folder](categories/configuration.md#generatedreports-folder) - `configuration.folders.generatedreportsfolderpath`
- [Geo Label](categories/generation.md#geo-label) - `generation.label.geo`
- [Global Sink Options](categories/configuration.md#global-sink-options) - `configuration.sink_options`
- [Great Expectations Integration](categories/metadata.md#great-expectations-integration) - `metadata.source.great_expectations`
- [Greater Than](categories/validation.md#greater-than) - `validation.field.greater_than`
- [Greater Than Size](categories/validation.md#greater-than-size) - `validation.field.greater_than_size`
- [Group By Aggregation Validation](categories/validation.md#group-by-aggregation-validation) - `validation.group_by`
- [HTTP Parameter Type](categories/generation.md#http-parameter-type) - `generation.option.http_param_type`
- [HTTP/REST API Connector](categories/connectors.md#http-rest-api-connector) - `connector.http.http`
- [In Set](categories/validation.md#in-set) - `validation.field.in`
- [Incremental Generation](categories/generation.md#incremental-generation) - `generation.option.incremental`
- [Integer Type](categories/generation.md#integer-type) - `generation.type.integer`
- [Interactive Plan Creation](categories/ui-api.md#interactive-plan-creation) - `ui.plan_creation`
- [Internet Label](categories/generation.md#internet-label) - `generation.label.internet`
- [Job Label](categories/generation.md#job-label) - `generation.label.job`
- [JSON File Connector](categories/connectors.md#json-file-connector) - `connector.files.json`
- [JSON Parsable](categories/validation.md#json-parsable) - `validation.field.is_json_parsable`
- [JSON Schema Integration](categories/metadata.md#json-schema-integration) - `metadata.source.json_schema`
- [JSON Schema Match](categories/validation.md#json-schema-match) - `validation.field.match_json_schema`
- [Length Between](categories/validation.md#length-between) - `validation.field.length_between`
- [Length Equal](categories/validation.md#length-equal) - `validation.field.length_equal`
- [Less Than](categories/validation.md#less-than) - `validation.field.less_than`
- [Less Than Size](categories/validation.md#less-than-size) - `validation.field.less_than_size`
- [Long Type](categories/generation.md#long-type) - `generation.type.long`
- [Luhn Check](categories/validation.md#luhn-check) - `validation.field.luhn_check`
- [Map Configuration](categories/generation.md#map-configuration) - `generation.option.map_config`
- [Map Type](categories/generation.md#map-type) - `generation.type.map`
- [Marquez Integration](categories/metadata.md#marquez-integration) - `metadata.source.marquez`
- [Max Between](categories/validation.md#max-between) - `validation.statistical.max_between`
- [Mean Between](categories/validation.md#mean-between) - `validation.statistical.mean_between`
- [Median Between](categories/validation.md#median-between) - `validation.statistical.median_between`
- [Metadata Analysis Configuration](categories/configuration.md#metadata-analysis-configuration) - `configuration.metadata`
- [Min Between](categories/validation.md#min-between) - `validation.statistical.min_between`
- [Money Label](categories/generation.md#money-label) - `generation.label.money`
- [Monotonically Decreasing](categories/validation.md#monotonically-decreasing) - `validation.field.is_decreasing`
- [Monotonically Increasing](categories/validation.md#monotonically-increasing) - `validation.field.is_increasing`
- [Most Common Value](categories/validation.md#most-common-value) - `validation.field.most_common_value_in_set`
- [Multi-Type Check](categories/validation.md#multi-type-check) - `validation.field.has_types`
- [MySQL Connector](categories/connectors.md#mysql-connector) - `connector.databases.mysql`
- [Name Label](categories/generation.md#name-label) - `generation.label.name`
- [Nation Label](categories/generation.md#nation-label) - `generation.label.nation`
- [Null Check](categories/validation.md#null-check) - `validation.field.null`
- [Null Value Control](categories/generation.md#null-value-control) - `generation.option.null_handling`
- [Numeric Precision and Scale](categories/generation.md#numeric-precision-and-scale) - `generation.option.numeric_precision`
- [Numeric Range](categories/generation.md#numeric-range) - `generation.option.numeric_range`
- [One-Of Selection](categories/generation.md#one-of-selection) - `generation.field.one_of`
- [Open Data Contract Standard Integration](categories/metadata.md#open-data-contract-standard-integration) - `metadata.source.open_data_contract_standard`
- [OpenAPI/Swagger Integration](categories/metadata.md#openapi-swagger-integration) - `metadata.source.open_api`
- [OpenMetadata Integration](categories/metadata.md#openmetadata-integration) - `metadata.source.open_metadata`
- [ORC File Connector](categories/connectors.md#orc-file-connector) - `connector.files.orc`
- [Parquet File Connector](categories/connectors.md#parquet-file-connector) - `connector.files.parquet`
- [Phone Label](categories/generation.md#phone-label) - `generation.label.phone`
- [plan File](categories/configuration.md#plan-file) - `configuration.folders.planfilepath`
- [Post-Generation Transformation](categories/advanced.md#post-generation-transformation) - `advanced.transformation`
- [Post-SQL Expression](categories/generation.md#post-sql-expression) - `generation.option.post_sql_expression`
- [PostgreSQL Connector](categories/connectors.md#postgresql-connector) - `connector.databases.postgres`
- [Quantile Values Between](categories/validation.md#quantile-values-between) - `validation.statistical.quantile_values_between`
- [RabbitMQ Connector](categories/connectors.md#rabbitmq-connector) - `connector.messaging.rabbitmq`
- [Random Seed](categories/generation.md#random-seed) - `generation.option.seed`
- [Real-time Results](categories/ui-api.md#real-time-results) - `ui.results_viewing`
- [Record Count Configuration](categories/advanced.md#record-count-configuration) - `advanced.count`
- [Record Tracking](categories/configuration.md#record-tracking) - `configuration.flags.enablerecordtracking`
- [recordTracking Folder](categories/configuration.md#recordtracking-folder) - `configuration.folders.recordtrackingfolderpath`
- [recordTrackingForValidation Folder](categories/configuration.md#recordtrackingforvalidation-folder) - `configuration.folders.recordtrackingforvalidationfolderpath`
- [Reference Mode](categories/advanced.md#reference-mode) - `advanced.reference_mode`
- [Regex Match](categories/validation.md#regex-match) - `validation.field.matches`
- [Regex Match List](categories/validation.md#regex-match-list) - `validation.field.matches_list`
- [Regex Pattern Generation](categories/generation.md#regex-pattern-generation) - `generation.field.regex`
- [Relationship Label](categories/generation.md#relationship-label) - `generation.label.relationship`
- [Report Generation](categories/ui-api.md#report-generation) - `ui.report_generation`
- [Sample Data Generation](categories/ui-api.md#sample-data-generation) - `ui.sample_data`
- [Save Reports](categories/configuration.md#save-reports) - `configuration.flags.enablesavereports`
- [Schema Field Names Validation](categories/validation.md#schema-field-names-validation) - `validation.field_names`
- [Semantic Version Generation](categories/generation.md#semantic-version-generation) - `generation.field.semantic_version`
- [Sequential Value Generation](categories/generation.md#sequential-value-generation) - `generation.field.sequence`
- [Sink Metadata](categories/configuration.md#sink-metadata) - `configuration.flags.enablesinkmetadata`
- [Size Check](categories/validation.md#size-check) - `validation.field.size`
- [Solace JMS Connector](categories/connectors.md#solace-jms-connector) - `connector.messaging.solace`
- [SQL Expression](categories/generation.md#sql-expression) - `generation.field.sql`
- [SQL Expression Validation](categories/validation.md#sql-expression-validation) - `validation.expression`
- [Starts With](categories/validation.md#starts-with) - `validation.field.starts_with`
- [Static Value](categories/generation.md#static-value) - `generation.field.static`
- [Std Dev Between](categories/validation.md#std-dev-between) - `validation.statistical.std_dev_between`
- [Step Field Filtering](categories/advanced.md#step-field-filtering) - `advanced.step_options`
- [Streaming Configuration](categories/configuration.md#streaming-configuration) - `configuration.streaming`
- [Streaming Load Patterns](categories/advanced.md#streaming-load-patterns) - `advanced.streaming_load_patterns`
- [String Length Control](categories/generation.md#string-length-control) - `generation.option.string_length`
- [String Type](categories/generation.md#string-type) - `generation.type.string`
- [Struct Type](categories/generation.md#struct-type) - `generation.type.struct`
- [Suggest Validations](categories/configuration.md#suggest-validations) - `configuration.flags.enablegeneratevalidations`
- [Sum Between](categories/validation.md#sum-between) - `validation.statistical.sum_between`
- [task Folder](categories/configuration.md#task-folder) - `configuration.folders.taskfolderpath`
- [Timestamp Type](categories/generation.md#timestamp-type) - `generation.type.timestamp`
- [Type Check](categories/validation.md#type-check) - `validation.field.has_type`
- [Unique Check](categories/configuration.md#unique-check) - `configuration.flags.enableuniquecheck`
- [Unique Check Only In Batch](categories/configuration.md#unique-check-only-in-batch) - `configuration.flags.enableuniquecheckonlyinbatch`
- [Unique Values](categories/validation.md#unique-values) - `validation.field.unique`
- [Unique Values Proportion](categories/validation.md#unique-values-proportion) - `validation.statistical.unique_values_proportion_between`
- [Uniqueness Constraint](categories/generation.md#uniqueness-constraint) - `generation.option.uniqueness`
- [Upstream Cross-Source Validation](categories/validation.md#upstream-cross-source-validation) - `validation.upstream`
- [Username Label](categories/generation.md#username-label) - `generation.label.username`
- [UUID Generation](categories/generation.md#uuid-generation) - `generation.field.uuid`
- [validation Folder](categories/configuration.md#validation-folder) - `configuration.folders.validationfolderpath`
- [Validation Runtime Configuration](categories/configuration.md#validation-runtime-configuration) - `configuration.validation`
- [Value Distribution](categories/generation.md#value-distribution) - `generation.option.distribution`
- [Value Mapping](categories/generation.md#value-mapping) - `generation.field.mapping`
- [Wait Conditions](categories/validation.md#wait-conditions) - `validation.wait_condition`
- [Weather Label](categories/generation.md#weather-label) - `generation.label.weather`
- [XML File Connector](categories/connectors.md#xml-file-connector) - `connector.files.xml`
