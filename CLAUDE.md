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

# Generate test coverage with Scoverage
./gradlew reportScoverage

# Create fat/shadow JAR for distribution
./gradlew :app:shadowJar

# Run specific configurations from IDE
./gradlew :app:run --args="DataCatererUI"
```

### Important Test Running Notes

ScalaTest with JUnit Platform has limitations with Gradle's `--tests` filtering:
- ✅ Use exact class names: `--tests "io.github.datacatering.datacaterer.core.ui.plan.PlanRepositoryTest"`
- ❌ Do NOT use wildcards: `--tests "*PlanRunTest*"` (runs ALL tests instead of filtering)

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
│   ├── metadata/           # Metadata discovery and integration
│   ├── ui/                 # Web UI server components
│   └── util/               # Utilities and helpers
└── main/resources/          # Configuration files and UI assets
```

### Key Architectural Patterns

**Builder Pattern**: All configuration uses immutable builders with method chaining
```scala
postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
  .table("accounts")  
  .fields(field.name("account_id").regex("ACC[0-9]{10}").unique(true))
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
- `PLAN_FILE_PATH`: Path to YAML plan configuration
- `TASK_FOLDER_PATH`: Directory containing task definitions
- `APPLICATION_CONFIG_PATH`: Custom application configuration

### Data Source Support

The system supports:
- **Databases**: Postgres, MySQL, Cassandra, BigQuery
- **Files**: CSV, JSON, Parquet, Delta Lake, Iceberg, ORC
- **Messaging**: Kafka, RabbitMQ, Solace
- **HTTP**: REST APIs with OpenAPI/Swagger integration
- **Metadata Sources**: Great Expectations, JSON Schema, Data Contract CLI, OpenMetadata, Marquez

## UI and API Integration

The application includes a web UI server that provides:
- Connection management and testing
- Interactive plan creation
- Execution history tracking
- Real-time results viewing

The UI is implemented as a separate module with React frontend and Scala backend using HTTP4S.

## Testing Strategy

- Use ScalaTest for unit testing
- Test both API builders and core application logic
- Mock external dependencies (databases, file systems)
- Use exact class names for test filtering, not wildcards
- Leverage the example module for integration testing

## Key Dependencies

- **Scala**: 2.12.x
- **Apache Spark**: 3.5.x
- **Jackson**: JSON serialization
- **Quicklens**: Immutable data updates
- **ScalaTest**: Testing framework
- **HTTP4S**: Web server framework
- **Logback/Log4j**: Logging