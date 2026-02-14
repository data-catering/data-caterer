# Data Caterer vs Data Caterer Lite — Feature Comparison

**Data Caterer** (Scala/Spark) vs **Data Caterer Lite** (Go/DuckDB)

- Full version: 169 features
- Lite version: ~95 features implemented (56% coverage)

Legend: Y = Implemented | N = Not implemented | P = Partial

---

## Data Source Connectors (16 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | PostgreSQL Connector | `connector.databases.postgres` | Y | Y | |
| 2 | MySQL Connector | `connector.databases.mysql` | Y | Y | |
| 3 | Cassandra Connector | `connector.databases.cassandra` | Y | N | NoSQL; no Go driver integrated |
| 4 | BigQuery Connector | `connector.databases.bigquery` | Y | N | Cloud; no Go SDK integrated |
| 5 | CSV File Connector | `connector.files.csv` | Y | Y | |
| 6 | JSON File Connector | `connector.files.json` | Y | Y | Includes JSONL support |
| 7 | Parquet File Connector | `connector.files.parquet` | Y | Y | |
| 8 | ORC File Connector | `connector.files.orc` | Y | N | |
| 9 | Delta Lake Connector | `connector.files.delta` | Y | Y | With transaction log |
| 10 | Apache Iceberg Connector | `connector.files.iceberg` | Y | Y | Read support |
| 11 | Apache Hudi Connector | `connector.files.hudi` | Y | N | |
| 12 | XML File Connector | `connector.files.xml` | Y | N | |
| 13 | Apache Kafka Connector | `connector.messaging.kafka` | Y | N | |
| 14 | Solace JMS Connector | `connector.messaging.solace` | Y | N | |
| 15 | RabbitMQ Connector | `connector.messaging.rabbitmq` | Y | N | |
| 16 | HTTP/REST API Connector | `connector.http.http` | Y | Y | |

**Lite total: 8/16 (50%)**

---

## Data Generation — Generator Types (12 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | Regex Pattern Generation | `generation.field.regex` | Y | Y | Parsed to DuckDB SQL |
| 2 | DataFaker Expression | `generation.field.expression` | Y | Y | ~100 faker expressions mapped to DuckDB SQL |
| 3 | One-Of Selection | `generation.field.one_of` | Y | Y | oneOf and oneOfWeighted |
| 4 | SQL Expression | `generation.field.sql` | Y | Y | DuckDB SQL instead of Spark SQL |
| 5 | Static Value | `generation.field.static` | Y | Y | |
| 6 | UUID Generation | `generation.field.uuid` | Y | Y | |
| 7 | Sequential Value Generation | `generation.field.sequence` | Y | Y | With prefix and padding |
| 8 | Conditional Value Generation | `generation.field.conditional_value` | Y | N | CASE WHEN logic |
| 9 | Correlated Field Generation | `generation.field.correlated` | Y | N | Statistical correlation |
| 10 | Value Mapping | `generation.field.mapping` | Y | N | Source field to output mapping |
| 11 | Semantic Version Generation | `generation.field.semantic_version` | Y | N | |
| 12 | Daily Batch Sequence | `generation.field.daily_batch_sequence` | Y | N | |

**Lite total: 7/12 (58%)**

---

## Data Generation — Data Types (13 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | String Type | `generation.type.string` | Y | Y | |
| 2 | Integer Type | `generation.type.integer` | Y | Y | |
| 3 | Long Type | `generation.type.long` | Y | Y | |
| 4 | Double Type | `generation.type.double` | Y | Y | |
| 5 | Float Type | `generation.type.float` | Y | Y | |
| 6 | Decimal Type | `generation.type.decimal` | Y | Y | |
| 7 | Boolean Type | `generation.type.boolean` | Y | Y | |
| 8 | Date Type | `generation.type.date` | Y | Y | |
| 9 | Timestamp Type | `generation.type.timestamp` | Y | Y | |
| 10 | Binary Type | `generation.type.binary` | Y | Y | |
| 11 | Array Type | `generation.type.array` | Y | Y | |
| 12 | Struct Type | `generation.type.struct` | Y | Y | |
| 13 | Map Type | `generation.type.map` | Y | Y | |

**Lite total: 13/13 (100%)**

---

## Data Generation — Field Options (17 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | Numeric Range | `generation.option.numeric_range` | Y | Y | min/max |
| 2 | Date/Time Range | `generation.option.date_range` | Y | Y | Includes withinDays, futureDays, excludeWeekends |
| 3 | Null Value Control | `generation.option.null_handling` | Y | Y | enableNull, nullProb |
| 4 | Edge Case Generation | `generation.option.edge_cases` | Y | N | |
| 5 | String Length Control | `generation.option.string_length` | Y | P | Via regex or SQL; no explicit minLen/maxLen/avgLen options |
| 6 | Array Configuration | `generation.option.array_config` | Y | P | Basic array support; no arrayWeightedOneOf, arrayUniqueFrom |
| 7 | Map Configuration | `generation.option.map_config` | Y | P | Basic map support; no explicit mapMinSize/mapMaxSize |
| 8 | Value Distribution | `generation.option.distribution` | Y | N | uniform/normal/exponential distributions |
| 9 | Uniqueness Constraint | `generation.option.uniqueness` | Y | Y | isUnique, isPrimaryKey |
| 10 | Numeric Precision and Scale | `generation.option.numeric_precision` | Y | P | Supported via DuckDB types; no explicit precision/scale/round options |
| 11 | Field Omission | `generation.option.omit` | Y | N | |
| 12 | Random Seed | `generation.option.seed` | Y | N | Per-field seed |
| 13 | Distinct Value Count | `generation.option.distinct_count` | Y | N | Metadata-driven |
| 14 | Cassandra Key Configuration | `generation.option.cassandra_keys` | Y | N | Cassandra-specific |
| 15 | Incremental Generation | `generation.option.incremental` | Y | N | |
| 16 | HTTP Parameter Type | `generation.option.http_param_type` | Y | Y | path/query/header |
| 17 | Post-SQL Expression | `generation.option.post_sql_expression` | Y | N | |

**Lite total: 6/17 (35%), plus 4 partial**

---

## Data Generation — Field Labels (13 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | Name Label | `generation.label.name` | Y | N | Auto-detection labels not implemented |
| 2 | Username Label | `generation.label.username` | Y | N | |
| 3 | Address Label | `generation.label.address` | Y | N | |
| 4 | Application Label | `generation.label.app` | Y | N | |
| 5 | Nation Label | `generation.label.nation` | Y | N | |
| 6 | Money Label | `generation.label.money` | Y | N | |
| 7 | Internet Label | `generation.label.internet` | Y | N | |
| 8 | Food Label | `generation.label.food` | Y | N | |
| 9 | Job Label | `generation.label.job` | Y | N | |
| 10 | Relationship Label | `generation.label.relationship` | Y | N | |
| 11 | Weather Label | `generation.label.weather` | Y | N | |
| 12 | Phone Label | `generation.label.phone` | Y | N | |
| 13 | Geo Label | `generation.label.geo` | Y | N | |

**Lite total: 0/13 (0%)** — Labels are a metadata-driven auto-detection feature not in Lite

---

## Data Validation (42 features)

### Field-Level Validations (29 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | Null Check | `validation.field.null` | Y | Y | notNull |
| 2 | Unique Values | `validation.field.unique` | Y | Y | |
| 3 | Equality Check | `validation.field.equal` | Y | N | |
| 4 | Contains Check | `validation.field.contains` | Y | N | |
| 5 | Starts With | `validation.field.starts_with` | Y | N | |
| 6 | Ends With | `validation.field.ends_with` | Y | N | |
| 7 | Less Than | `validation.field.less_than` | Y | N | |
| 8 | Greater Than | `validation.field.greater_than` | Y | N | |
| 9 | Between Range | `validation.field.between` | Y | Y | |
| 10 | In Set | `validation.field.in` | Y | Y | Also notIn |
| 11 | Regex Match | `validation.field.matches` | Y | Y | matches |
| 12 | Regex Match List | `validation.field.matches_list` | Y | N | |
| 13 | Size Check | `validation.field.size` | Y | N | |
| 14 | Less Than Size | `validation.field.less_than_size` | Y | N | |
| 15 | Greater Than Size | `validation.field.greater_than_size` | Y | N | |
| 16 | Length Between | `validation.field.length_between` | Y | P | Via minLength/maxLength options |
| 17 | Length Equal | `validation.field.length_equal` | Y | N | |
| 18 | Luhn Check | `validation.field.luhn_check` | Y | N | |
| 19 | Type Check | `validation.field.has_type` | Y | N | |
| 20 | Multi-Type Check | `validation.field.has_types` | Y | N | |
| 21 | Monotonically Decreasing | `validation.field.is_decreasing` | Y | N | |
| 22 | Monotonically Increasing | `validation.field.is_increasing` | Y | N | |
| 23 | JSON Parsable | `validation.field.is_json_parsable` | Y | N | |
| 24 | JSON Schema Match | `validation.field.match_json_schema` | Y | N | |
| 25 | DateTime Format Match | `validation.field.match_date_time_format` | Y | N | |
| 26 | Distinct In Set | `validation.field.distinct_in_set` | Y | N | |
| 27 | Distinct Contains Set | `validation.field.distinct_contains_set` | Y | N | |
| 28 | Distinct Equal | `validation.field.distinct_equal` | Y | N | |
| 29 | Most Common Value | `validation.field.most_common_value_in_set` | Y | N | |

### Statistical Validations (8 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | Max Between | `validation.statistical.max_between` | Y | N | |
| 2 | Mean Between | `validation.statistical.mean_between` | Y | N | |
| 3 | Median Between | `validation.statistical.median_between` | Y | N | |
| 4 | Min Between | `validation.statistical.min_between` | Y | N | |
| 5 | Std Dev Between | `validation.statistical.std_dev_between` | Y | N | |
| 6 | Sum Between | `validation.statistical.sum_between` | Y | N | |
| 7 | Unique Values Proportion | `validation.statistical.unique_values_proportion_between` | Y | N | |
| 8 | Quantile Values Between | `validation.statistical.quantile_values_between` | Y | N | |

### Other Validations (5 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | SQL Expression Validation | `validation.expression` | Y | Y | expr-based |
| 2 | Group By Aggregation Validation | `validation.group_by` | Y | Y | sum, avg, min, max, count, stddev |
| 3 | Upstream Cross-Source Validation | `validation.upstream` | Y | N | Cross-system joins |
| 4 | Schema Field Names Validation | `validation.field_names` | Y | N | |
| 5 | Wait Conditions | `validation.wait_condition` | Y | N | pause/fileExists/dataExists/webhook |

**Lite total: 8/42 (19%), plus 1 partial**

---

## Configuration (29 features)

### Feature Flags (14 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | Count Records | `configuration.flags.enablecount` | Y | Y | |
| 2 | Generate Data | `configuration.flags.enablegeneratedata` | Y | Y | |
| 3 | Record Tracking | `configuration.flags.enablerecordtracking` | Y | Y | |
| 4 | Delete Generated Records | `configuration.flags.enabledeletegeneratedrecords` | Y | Y | |
| 5 | Auto-Generate Plan and Tasks | `configuration.flags.enablegenerateplanandtasks` | Y | Y | |
| 6 | Fail on Error | `configuration.flags.enablefailonerror` | Y | Y | |
| 7 | Unique Check | `configuration.flags.enableuniquecheck` | Y | Y | |
| 8 | Sink Metadata | `configuration.flags.enablesinkmetadata` | Y | Y | |
| 9 | Save Reports | `configuration.flags.enablesavereports` | Y | Y | |
| 10 | Data Validation | `configuration.flags.enablevalidation` | Y | Y | |
| 11 | Suggest Validations | `configuration.flags.enablegeneratevalidations` | Y | Y | |
| 12 | Alerts | `configuration.flags.enablealerts` | Y | Y | |
| 13 | Unique Check Only In Batch | `configuration.flags.enableuniquecheckonlyinbatch` | Y | N | |
| 14 | Fast Generation | `configuration.flags.enablefastgeneration` | Y | N | Always fast in Lite (DuckDB SQL-native) |

### Folder Paths (7 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | plan File | `configuration.folders.planfilepath` | Y | Y | Via YAML_FILE env var or CLI arg |
| 2 | task Folder | `configuration.folders.taskfolderpath` | Y | N | Legacy format |
| 3 | generatedPlanAndTask Folder | `configuration.folders.generatedplanandtaskfolderpath` | Y | N | |
| 4 | generatedReports Folder | `configuration.folders.generatedreportsfolderpath` | Y | Y | |
| 5 | recordTracking Folder | `configuration.folders.recordtrackingfolderpath` | Y | N | |
| 6 | validation Folder | `configuration.folders.validationfolderpath` | Y | N | |
| 7 | recordTrackingForValidation Folder | `configuration.folders.recordtrackingforvalidationfolderpath` | Y | N | |

### Other Configuration (8 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | Batch Size | `configuration.generation.batch_size` | Y | P | Has maxRecordsPerChunk instead |
| 2 | Bloom Filter Configuration | `configuration.generation.bloom_filter` | Y | N | |
| 3 | Metadata Analysis Configuration | `configuration.metadata` | Y | N | |
| 4 | Streaming Configuration | `configuration.streaming` | Y | P | Different streaming model |
| 5 | Alert Configuration | `configuration.alerts` | Y | N | Flag exists but Slack integration not implemented |
| 6 | Validation Runtime Configuration | `configuration.validation` | Y | N | |
| 7 | Apache Spark Configuration | `configuration.runtime.spark` | Y | N | Not applicable (DuckDB engine) |
| 8 | Global Sink Options | `configuration.sink_options` | Y | Y | seed, locale |

**Lite total: 16/29 (55%), plus 2 partial**

---

## Advanced Features (11 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | Foreign Key Relationships | `advanced.foreign_keys` | Y | Y | Topological sort, referential integrity |
| 2 | Foreign Key Cardinality Control | `advanced.foreign_key_cardinality` | Y | Y | min/max per parent |
| 3 | Foreign Key Nullability | `advanced.foreign_key_nullability` | Y | Y | nullPercentage |
| 4 | Foreign Key Generation Modes | `advanced.foreign_key_generation_modes` | Y | N | all-exist/all-combinations/partial |
| 5 | Record Count Configuration | `advanced.count` | Y | Y | Fixed count, per-field |
| 6 | Streaming Load Patterns | `advanced.streaming_load_patterns` | Y | Y | ramp, spike, sine, steps |
| 7 | Post-Generation Transformation | `advanced.transformation` | Y | N | Custom Java/Scala classes |
| 8 | Step Field Filtering | `advanced.step_options` | Y | N | include/excludeFields |
| 9 | Reference Mode | `advanced.reference_mode` | Y | N | |
| 10 | Configuration Interfaces | `advanced.interfaces` | Y | P | CLI + YAML only (no Java/Scala API, no Web UI) |
| 11 | Environment Variable Substitution | `advanced.env_substitution` | Y | Y | ${VAR} and ${VAR:-default} |

**Lite total: 7/11 (64%), plus 1 partial**

---

## Metadata Integration (10 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | Marquez Integration | `metadata.source.marquez` | Y | N | |
| 2 | OpenMetadata Integration | `metadata.source.open_metadata` | Y | N | |
| 3 | OpenAPI/Swagger Integration | `metadata.source.open_api` | Y | Y | Generates HTTP steps |
| 4 | Great Expectations Integration | `metadata.source.great_expectations` | Y | N | |
| 5 | Open Data Contract Standard Integration | `metadata.source.open_data_contract_standard` | Y | Y | ODCS |
| 6 | Data Contract CLI Integration | `metadata.source.data_contract_cli` | Y | N | |
| 7 | Amundsen Integration | `metadata.source.amundsen` | Y | N | |
| 8 | DataHub Integration | `metadata.source.datahub` | Y | N | |
| 9 | Confluent Schema Registry Integration | `metadata.source.confluent_schema_registry` | Y | N | |
| 10 | JSON Schema Integration | `metadata.source.json_schema` | Y | Y | With allOf/anyOf/oneOf composition |

**Lite total: 3/10 (30%)**

Note: Lite also supports **dbt manifest.json** and **CSV/JSON/Parquet profiling** as metadata extraction sources, which are not in the Full version's feature catalog.

---

## UI and API (6 features)

| # | Feature | ID | Full | Lite | Notes |
|---|---------|-----|------|------|-------|
| 1 | Connection Management | `ui.connection_management` | Y | N | Web UI only |
| 2 | Interactive Plan Creation | `ui.plan_creation` | Y | N | Web UI only |
| 3 | Execution History | `ui.execution_history` | Y | N | Web UI only |
| 4 | Real-time Results | `ui.results_viewing` | Y | N | Web UI only |
| 5 | Sample Data Generation | `ui.sample_data` | Y | Y | Via `sample` CLI command |
| 6 | Report Generation | `ui.report_generation` | Y | Y | HTML reports |

**Lite total: 2/6 (33%)**

---

## Summary by Category

| Category | Full | Lite | Lite % |
|----------|------|------|--------|
| Connectors | 16 | 8 | 50% |
| Generation — Generator Types | 12 | 7 | 58% |
| Generation — Data Types | 13 | 13 | 100% |
| Generation — Field Options | 17 | 6 (+4P) | 35% |
| Generation — Field Labels | 13 | 0 | 0% |
| Validation | 42 | 8 (+1P) | 19% |
| Configuration | 29 | 16 (+2P) | 55% |
| Advanced Features | 11 | 7 (+1P) | 64% |
| Metadata Integration | 10 | 3 | 30% |
| UI and API | 6 | 2 | 33% |
| **Total** | **169** | **70 (+8P)** | **41% (46% incl. partial)** |

---

## Lite-Only Features (Not in Full)

These capabilities exist in Data Caterer Lite but are NOT listed in the Full version's feature catalog:

| Feature | Description |
|---------|-------------|
| MCP Server | Model Context Protocol server for LLM integration (6+ tools) |
| CLI Commands | `generate`, `validate`, `extract`, `init`, `list`, `sample`, `mcp` |
| dbt Integration | Extract metadata from dbt manifest.json |
| File Profiling | Auto-detect schemas from CSV/JSON/Parquet files |
| Resource Limits | maxRecordsPerStep, maxTotalRecords, maxFileSizeMB, maxMemoryMB |
| Single Binary | ~64MB cross-platform native binary (no JVM required) |
| DuckDB Engine | All generation done via native SQL (always "fast mode") |

---

## Biggest Gaps in Lite

These are the highest-impact missing features, ordered by likely user demand:

1. **Messaging connectors** — Kafka, RabbitMQ, Solace (0/3)
2. **Field labels / auto-detection** — All 13 labels missing
3. **Validation coverage** — Only 8/42 validation types (missing statistical, schema, cross-source)
4. **Database connectors** — Cassandra, BigQuery (0/2)
5. **File format connectors** — ORC, Hudi, XML (0/3)
6. **Advanced generation** — Conditional values, correlated fields, value mapping, edge cases
7. **Web UI** — No visual plan creation or execution management (0/4 UI features)
8. **Metadata integrations** — 7/10 sources missing (Marquez, OpenMetadata, Great Expectations, etc.)
9. **Post-generation transformation** — Custom class-based transforms
10. **Foreign key generation modes** — all-combinations and partial modes
