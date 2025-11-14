# Unified YAML Examples

This directory contains examples of the **unified YAML format** for Data Caterer. The unified format allows you to define connections, tasks, validations, and configuration all in a single file, following the same natural flow as the Scala/Java API.

## Why Unified YAML?

The traditional approach required managing multiple files:
- `application.conf` for connections
- `plan/*.yaml` for plan definitions
- `task/*.yaml` for task details
- `validation/*.yaml` for validation rules

The unified format combines everything into one file, making it:
- ✅ **Easier to understand** - See the complete picture in one place
- ✅ **More portable** - Copy one file instead of coordinating multiple files
- ✅ **Simpler to maintain** - Fewer files to track and update
- ✅ **Environment-aware** - Use `${VAR:-default}` for different environments
- ✅ **Familiar** - Mirrors the Scala/Java API structure

## Examples

### 1. [`simple-postgres-example.yaml`](simple-postgres-example.yaml)
**Best for**: Getting started, understanding the basics

Shows:
- Connection definition with environment variables
- Single PostgreSQL table generation
- Inline field definitions
- Inline validations (field, expression, metric)
- Basic configuration

```yaml
connections:
  - name: customer_db
    type: postgres
    url: "jdbc:postgresql://${DB_HOST:-localhost}:5432/customer"

tasks:
  - name: generate_customers
    dataSourceName: customer_db

---
name: generate_customers
steps:
  - name: customers
    fields: [...]
    validations: [...]  # ← Inline with task!
```

### 2. [`multi-source-example.yaml`](multi-source-example.yaml)
**Best for**: Complex scenarios with multiple data sources

Shows:
- Multiple connection types (Postgres, Kafka, CSV)
- Connection reuse across tasks
- Foreign key relationships
- Different count strategies (records, duration/rate)
- GroupBy validations
- Performance metrics validation

### 3. [`inline-connections-example.yaml`](inline-connections-example.yaml)
**Best for**: Quick tests, simple scenarios

Shows:
- Tasks with inline connection definitions
- No separate `connections` section needed
- Perfect for throwaway tests or simple file generation

```yaml
tasks:
  - name: json_data
    connection:
      type: json
      options:
        path: "/tmp/output/users"
```

### 4. [`environment-vars-example.yaml`](environment-vars-example.yaml)
**Best for**: Production deployments, multi-environment setups

Shows:
- Environment variable usage: `${VAR_NAME:-default}`
- Different configs for dev/staging/prod
- Secure credential management
- Environment-specific record counts and settings

```bash
# Development
./gradlew :app:run

# Production
export DB_HOST=prod-db.example.com
export DB_PASSWORD=$PROD_SECRET
export NUM_USERS=1000000
./gradlew :app:run
```

### 5. [`performance-test-example.yaml`](performance-test-example.yaml)
**Best for**: Load testing HTTP APIs

Shows:
- Performance testing configuration
- Load patterns (ramp, spike, wave)
- Weighted tasks for realistic traffic
- Performance metric validations (throughput, latency, error rate)
- Warmup and cooldown periods

### 6. [`complete-example.yaml`](complete-example.yaml)
**Best for**: Reference, learning advanced features

Shows ALL features:
- Multiple connection types and reuse
- Complex field generators (regex, faker, SQL, oneOf)
- Nested fields and arrays
- Transformations
- Comprehensive validations (field, expression, groupBy, metric)
- Foreign keys
- Load patterns
- Full configuration options

## Key Concepts

### Connection Definition

**Option 1: Reusable connections** (in plan section)
```yaml
connections:
  - name: my_db
    type: postgres
    url: "jdbc:postgresql://localhost:5432/db"
    options:
      user: "postgres"
      password: "postgres"

tasks:
  - name: task1
    dataSourceName: my_db  # Reference by name
```

**Option 2: Inline connections** (in task)
```yaml
tasks:
  - name: task1
    connection:
      type: csv
      options:
        path: "/tmp/data"
```

### Environment Variables

Use Docker Compose / Bash syntax:
```yaml
url: "jdbc:postgresql://${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME:-mydb}"
```

- `${VAR}` - Required variable (error if not set)
- `${VAR:-default}` - Variable with default value
- `$${VAR}` - Escaped literal (becomes `${VAR}`)

### Inline Validations

Validations are part of the task definition, **not a separate file**:

```yaml
steps:
  - name: users
    fields: [...]
    count: {records: 1000}
    validations:
      # Field validations
      - field: email
        validation:
          - type: unique
          - type: matches
            regex: "^[A-Za-z0-9+_.-]+@(.+)$"

      # Expression validations
      - expr: "age >= 18"

      # Metric validations
      - metric: "count"
        validation:
          - type: equal
            value: 1000
```

This mirrors the Scala API:
```scala
.fields(...)
.count(...)
.validations(...)  // ← Same level!
```

### Multi-Document YAML

Use `---` to separate plan and task definitions in one file:

```yaml
name: "my_plan"
connections: [...]
tasks: [...]

---
name: "task1"
steps: [...]
validations: [...]

---
name: "task2"
steps: [...]
```

## Migration from Legacy Format

If you have existing multi-file setups:

**Before** (3-4 files):
```
application.conf      ← Connections
plan/my-plan.yaml     ← Plan
task/my-task.yaml     ← Task
validation/my-val.yaml ← Validations
```

**After** (1 file):
```yaml
# unified.yaml
connections: [...]  # From application.conf
tasks: [...]         # From plan

---
name: "my-task"
steps: [...]        # From task file
validations: [...]  # From validation file - inline!
```

## Running Examples

```bash
# Set environment variables (optional)
export DB_HOST=localhost
export DB_PASSWORD=postgres

# Run an example
export PLAN_FILE_PATH=example/unified/simple-postgres-example.yaml
./gradlew :app:run
```

## Backward Compatibility

The unified format is **fully backward compatible**. Existing multi-file setups continue working:
- Plans can still reference external task files
- Connections in `application.conf` are still supported (with a warning)
- Validation files still work

The unified format is optional but recommended for new projects.

## JSON Schema

A JSON Schema for validation is available (coming soon) at:
```yaml
# yaml-language-server: $schema=https://data.catering/schema/plan-schema.json
```

Configure your IDE:
- **VSCode**: Install "YAML" extension by Red Hat
- **IntelliJ**: Built-in YAML support with schema validation

## Learn More

- [Unified YAML Configuration Guide](../../docs/guides/unified-yaml-configuration.md) (coming soon)
- [Scala API Documentation](../../docs/get-started/quick-start.md)
- [Data Caterer Website](https://data.catering)
