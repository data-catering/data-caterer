# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Caterer is a test data management tool built with Scala and Apache Spark that provides automated data generation, validation, and cleanup capabilities. It supports multiple data sources including databases, files, messaging systems, and HTTP APIs.

## Build System & Common Commands

The project uses Gradle with Kotlin DSL and follows a multi-module structure:
- **Root module**: Configuration and project orchestration
- **api**: Builder patterns and models for programmatic usage  
- **app**: Core execution engine, Spark integration, and UI server
- **example**: Sample implementations and Docker configurations

### Essential Commands

```bash
# Build the entire project
./gradlew build

# Build project without fat JAR tasks
./gradlew clean :app:build -x :app:shadowJar -x :app:distTar -x :app:distZip

# Build individual modules
./gradlew :app:build
./gradlew :api:build

# Run tests (use exact class names, NOT wildcards)
./gradlew :app:test --tests "io.github.datacatering.datacaterer.core.ui.plan.PlanRepositoryTest" --info
./gradlew :api:test

# Run integration tests (slower, more comprehensive)
./gradlew :app:integrationTest --tests "io.github.datacatering.datacaterer.core.ui.plan.YamlPlanIntegrationTest" --info

# Run performance tests (for benchmarking)
./gradlew :app:performanceTest --tests "io.github.datacatering.datacaterer.core.util.ForeignKeyUtilPerformanceTest" --info

# Generate test coverage with Scoverage
./gradlew reportScoverage

# Create fat/shadow JAR for distribution
./gradlew :app:shadowJar

# Run UI server (standalone mode)
./gradlew :app:runUI

# Run Spark job mode
./gradlew :app:runSpark

# Run specific configurations from IDE
./gradlew :app:run --args="DataCatererUI"
```

### Important Test Running Notes

ScalaTest with JUnit Platform has limitations with Gradle's `--tests` filtering:
- ✅ Use exact class names: `--tests "io.github.datacatering.datacaterer.core.ui.plan.PlanRepositoryTest"`
- ❌ Do NOT use wildcards: `--tests "*PlanRunTest*"` (runs ALL tests instead of filtering)

**Test Types**:
- **Unit tests** (`app/src/test`): Fast, isolated tests for individual components
- **Integration tests** (`app/src/integrationTest`): Slower tests that verify end-to-end workflows (e.g., YAML plan processing)
- **Performance tests** (`app/src/performanceTest`): Benchmarking tests for data generation and foreign key performance

## Architecture Overview

### Core Domain Concepts

- **Plans**: High-level configuration defining data operations to perform
- **Tasks**: Individual data sources (databases, files, messaging systems, HTTP)
- **Steps**: Sub-operations within tasks (tables, topics, file paths)
- **Fields**: Individual data field configurations with generation rules
- **Validations**: Data quality checks and assertions

### Module Structure

```
api/                          # Builder API and models
├── model/                    # Core data models and types
├── connection/              # Data source connection builders
└── validation/              # Validation builders

app/                          # Core application
├── core/
│   ├── generator/           # Data generation engine
│   ├── validator/           # Data validation engine
│   ├── sink/               # Data output processors
│   ├── plan/               # Plan and task processing
│   ├── ui/                 # Web UI server components
│   │   ├── cache/          # Caching layer for UI
│   │   ├── http/           # HTTP endpoints and routing
│   │   ├── plan/           # Plan repository and management
│   │   ├── resource/       # Resource management
│   │   ├── sample/         # Sample data generation
│   │   └── service/        # Business logic services
│   ├── util/               # Utilities and helpers
│   ├── alert/              # Alert/notification system
│   ├── config/             # Configuration management
│   ├── listener/           # Event listeners
│   ├── model/              # Core data models
│   └── parser/             # Parser utilities
└── main/resources/          # Configuration files and UI assets
```

### Key Architectural Patterns

**Builder Pattern**: All configuration uses immutable builders with method chaining
```scala
postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .table("accounts")
  .fields(
    field.name("account_id").regex("ACC[0-9]{8}").unique(true),
    field.name("status").regex("(ACTIVE|INACTIVE|PENDING)")
  )
```

**Case Class Data Models**: Immutable data structures with Jackson JSON serialization
```scala
@JsonIgnoreProperties(ignoreUnknown = true)
case class DataSource(
  name: String,
  `type`: String,
  options: Map[String, String] = Map(),
  enabled: Boolean = true
)
```

**Spark Integration**: Uses Apache Spark for distributed data processing and Spark SQL for data operations

## Development Patterns

### Code Style Requirements

- Use `com.softwaremill.quicklens.ModifyPimp` for immutable updates in builders
- Always provide parameterless constructors: `def this() = this(DefaultValue())`
- Use `@JsonIgnoreProperties(ignoreUnknown = true)` for JSON serialization compatibility
- Use `Option[T]` instead of `null` for optional values
- Follow package structure under `io.github.datacatering.datacaterer`

### Builder Implementation Pattern

```scala
case class TaskBuilder(task: Task = Task()) {
  def this() = this(Task())
  
  def name(name: String): TaskBuilder = 
    this.modify(_.task.name).setTo(name)
    
  def option(option: (String, String)): TaskBuilder =
    this.modify(_.task.options)(_ ++ Map(option))
}
```

### Environment Configuration

Runtime behavior is controlled via environment variables:
- `ENABLE_GENERATE_DATA`: Enable/disable data generation
- `ENABLE_DELETE_GENERATED_RECORDS`: Enable cleanup mode
- `ENABLE_GENERATE_PLAN_AND_TASKS`: Enable metadata-driven plan/task generation
- `ENABLE_RECORD_TRACKING`: Enable tracking of generated records for cleanup
- `PLAN_FILE_PATH`: Path to YAML plan configuration
- `TASK_FOLDER_PATH`: Directory containing task definitions
- `APPLICATION_CONFIG_PATH`: Custom application configuration
- `GENERATED_REPORTS_FOLDER_PATH`: Output directory for reports (default: `/tmp/data-caterer/report`)
- `LOG_LEVEL`: Logging level (`debug`, `info`, `warn`, `error`)

Configuration flags control performance optimizations:
- `enableFastGeneration`: Enable fast mode (pure SQL generation without UDFs) - default: `false`

### Data Source Support

The system supports:
- **Databases**: Postgres, MySQL, Cassandra, BigQuery
- **Files**: CSV, JSON, Parquet, Delta Lake, Iceberg, ORC
- **Messaging**: Kafka, RabbitMQ, Solace
- **HTTP**: REST APIs with OpenAPI/Swagger integration
- **Metadata Sources**: Great Expectations, JSON Schema, Data Contract CLI, OpenMetadata, Marquez

## Data Generation

### Regex Patterns

Fields can use regex patterns for data generation. The system has two generation modes:

**Standard Mode** (default):
- Uses DataFaker's `regexify()` for accurate regex generation
- Slower due to UDF calls in distributed execution

**Fast Mode** (`enableFastGeneration: true`):
- Parses regex patterns into SQL expressions (no UDFs)
- Supports common business patterns: `\d`, `[A-Z]`, `[0-9]`, quantifiers `{n}`, `{m,n}`, alternations `(A|B|C)`
- Falls back to UDF for unsupported patterns (backreferences, lookaheads, etc.)

```scala
// Supported patterns in fast mode
field.name("account_id").regex("ACC[0-9]{8}")              // → CONCAT('ACC', LPAD(...))
field.name("product_code").regex("[A-Z]{3}-[0-9]{2}")     // → CONCAT(letters, '-', digits)
field.name("status").regex("(ACTIVE|INACTIVE|PENDING)")   // → ELEMENT_AT(ARRAY(...), RAND())
field.name("serial").regex("[A-Z0-9]{16}")                // → Alphanumeric generation

// Unsupported patterns fall back to UDF
field.name("complex").regex("(?=lookahead)pattern")       // → Uses GENERATE_REGEX UDF
```

**Implementation**: Regex patterns are parsed using `RegexPatternParser` (in `core.generator.provider.regex` package) which converts supported patterns to an AST and generates pure SQL. Parsing happens once during generator initialization with success/failure logged at DEBUG/WARN levels.

## UI and API Integration

The application includes a web UI server that provides:
- Connection management and testing
- Interactive plan creation
- Execution history tracking
- Real-time results viewing
- Sample data generation

The UI is implemented within the app module at `app/src/main/scala/io/github/datacatering/datacaterer/core/ui/` with:
- **Frontend**: Static UI assets in `app/src/main/resources/ui/`
- **Backend**: Scala-based HTTP server using Apache Pekko (HTTP4S-like framework)
- **API Endpoints**: RESTful endpoints for connections, plans, tasks, and execution management
- **Caching**: In-memory caching layer for improved performance

To run the UI server:
```bash
./gradlew :app:runUI
# or
DEPLOY_MODE=standalone ./gradlew :app:run --args="DataCatererUI"
```

## Testing Strategy

- **Unit tests** (`app/src/test`): Fast, isolated tests using ScalaTest with Mockito for mocking
- **Integration tests** (`app/src/integrationTest`): End-to-end tests for YAML processing, plan execution, and API workflows
- **Performance tests** (`app/src/performanceTest`): Benchmarking for data generation performance and optimization validation
- Test both API builders and core application logic
- Mock external dependencies (databases, file systems) in unit tests
- Use exact class names for test filtering, NOT wildcards
- Leverage the example module for real-world integration scenarios

**Running Tests**:
```bash
# Unit tests only
./gradlew :app:test

# Integration tests
./gradlew :app:integrationTest

# Performance tests
./gradlew :app:performanceTest

# Specific test class
./gradlew :app:test --tests "io.github.datacatering.datacaterer.core.generator.DataGeneratorFactoryTest"
```

## Key Dependencies

- **Scala**: 2.12.x
- **Apache Spark**: 3.5.x (core data processing engine)
- **Jackson**: JSON/YAML serialization (2.15.3)
- **Quicklens**: Immutable data updates in builders
- **ScalaTest**: Testing framework with JUnit Platform runner
- **Apache Pekko**: Web server framework (HTTP/Actor system)
- **DataFaker**: Data generation library for realistic fake data
- **PureConfig**: Type-safe configuration loading
- **Logback**: Logging framework
- **Various connectors**: Postgres, MySQL, Cassandra, Kafka, BigQuery, Delta Lake, Iceberg, etc.

## Performance Optimization

Data Caterer includes several performance optimizations:

**Fast Generation Mode** (`enableFastGeneration: true`):
- Converts regex patterns to pure SQL expressions (avoiding UDF overhead)
- Dramatically improves generation speed for large datasets
- Automatically falls back to UDF for unsupported patterns
- Recommended for production workloads with regex-based field generation

**Foreign Key Optimization**:
- Efficient foreign key relationship handling for referential integrity
- Optimized sampling and distribution strategies
- Performance testing infrastructure in `app/src/performanceTest`

**Configuration**:
```scala
// In Scala API
config
  .generatedReportsFolderPath("/tmp/reports")
  .enableFastGeneration(true)  // Enable SQL-based regex generation
```

```yaml
# In YAML configuration
flags:
  enableFastGeneration: true
```