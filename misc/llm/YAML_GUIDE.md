# Data Caterer YAML Configuration Guide for Code Assistants

This guide helps code assistants understand how to use Data Caterer's YAML configuration interface to help users generate test data, validate data quality, and manage test data lifecycle without writing code.

## Quick Start Pattern

Data Caterer YAML configuration consists of three main file types:

1. **Plan Files** - Define overall execution plan and data source connections
2. **Task Files** - Define individual data generation/validation tasks
3. **Validation Files** - Define standalone validation rules

### Basic Directory Structure
```
my-data-caterer-config/
├── plan/
│   ├── my-plan.yaml              # Main execution plan
│   └── connection-config.yaml    # Data source connections
├── task/
│   ├── postgres/
│   │   └── customer-task.yaml    # Database tasks
│   ├── file/
│   │   ├── csv/
│   │   │   └── account-task.yaml # CSV file tasks
│   │   └── json/
│   │       └── transaction-task.yaml
│   └── kafka/
│       └── event-task.yaml       # Kafka tasks
└── validation/
    └── data-quality-checks.yaml  # Validation rules
```

## 1. Plan Configuration

Plans orchestrate the overall execution and define foreign key relationships.

### Basic Plan Structure
```yaml
name: "my_data_plan"
description: "Generate test data for customer database"
tasks:
  - name: "customer_task"
    dataSourceName: "postgres_customers"
    enabled: true
  - name: "order_task"
    dataSourceName: "postgres_orders"
    enabled: true

sinkOptions:
  foreignKeys:
    - source:
        dataSource: "postgres_customers"
        step: "customers"
        fields: ["customer_id"]
      generate:
        - dataSource: "postgres_orders"
          step: "orders"
          fields: ["customer_id"]
```

### Plan with Multiple Foreign Keys
```yaml
name: "multi_table_plan"
description: "Generate related data across multiple tables"
tasks:
  - name: "customer_task"
    dataSourceName: "my_postgres"
  - name: "account_task"
    dataSourceName: "my_postgres"
  - name: "transaction_task"
    dataSourceName: "my_postgres"

sinkOptions:
  foreignKeys:
    # Customer -> Account relationship
    - source:
        dataSource: "my_postgres"
        step: "customers"
        fields: ["customer_id"]
      generate:
        - dataSource: "my_postgres"
          step: "accounts"
          fields: ["customer_id"]

    # Account -> Transaction relationship
    - source:
        dataSource: "my_postgres"
        step: "accounts"
        fields: ["account_id"]
      generate:
        - dataSource: "my_postgres"
          step: "transactions"
          fields: ["account_id"]
```

### Multi-Column Foreign Keys
```yaml
sinkOptions:
  foreignKeys:
    - source:
        dataSource: "my_postgres"
        step: "customers"
        fields: ["customer_id", "region"]
      generate:
        - dataSource: "my_postgres"
          step: "orders"
          fields: ["customer_id", "customer_region"]
```

### Cross-Source Foreign Keys
```yaml
sinkOptions:
  foreignKeys:
    - source:
        dataSource: "postgres_db"
        step: "accounts"
        fields: ["account_id"]
      generate:
        - dataSource: "kafka_events"
          step: "account_events"
          fields: ["account_id"]
        - dataSource: "json_files"
          step: "account_summary"
          fields: ["account_id"]
```

## 2. Task Configuration

Tasks define individual data sources and their field generation rules.

### PostgreSQL Task
```yaml
name: "postgres_customer_task"
steps:
  - name: "customers"
    type: "postgres"
    options:
      dbtable: "public.customers"
    count:
      records: 1000
    fields:
      - name: "customer_id"
        type: "string"
        options:
          regex: "CUST[0-9]{8}"
          isPrimaryKey: true

      - name: "first_name"
        type: "string"
        options:
          expression: "#{Name.firstName}"

      - name: "last_name"
        type: "string"
        options:
          expression: "#{Name.lastName}"

      - name: "email"
        type: "string"
        options:
          expression: "#{Internet.emailAddress}"
          isUnique: true

      - name: "created_at"
        type: "timestamp"

      - name: "status"
        type: "string"
        options:
          oneOf:
            - "active"
            - "inactive"
            - "pending"
```

### CSV Task
```yaml
name: "csv_account_task"
steps:
  - name: "accounts"
    type: "csv"
    options:
      path: "/tmp/data/accounts.csv"
      header: "true"
      delimiter: ","
    count:
      records: 10000
    fields:
      - name: "account_id"
        type: "string"
        options:
          regex: "ACC[0-9]{10}"
          isUnique: true

      - name: "account_type"
        type: "string"
        options:
          oneOf: ["checking", "savings", "credit", "loan"]

      - name: "balance"
        type: "decimal(10,2)"
        options:
          min: 0.01
          max: 100000.00

      - name: "open_date"
        type: "date"
        options:
          min: "2020-01-01"
          max: "2024-12-31"

      - name: "status"
        type: "string"
        options:
          sql: "CASE WHEN balance > 1000 THEN 'active' ELSE 'pending' END"
```

### JSON Task with Nested Structure
```yaml
name: "json_customer_task"
steps:
  - name: "customers"
    type: "json"
    options:
      path: "/tmp/data/customers.json"
    count:
      records: 500
    fields:
      - name: "customer_id"
        type: "string"
        options:
          regex: "CUST[0-9]{8}"

      - name: "personal_details"
        type: "struct"
        fields:
          - name: "full_name"
            type: "string"
            options:
              expression: "#{Name.name}"

          - name: "first_name"
            type: "string"
            options:
              sql: "SPLIT(personal_details.full_name, ' ')[0]"

          - name: "last_name"
            type: "string"
            options:
              sql: "SPLIT(personal_details.full_name, ' ')[1]"

          - name: "age"
            type: "int"
            options:
              min: 18
              max: 90

          - name: "contact"
            type: "struct"
            fields:
              - name: "email"
                type: "string"
                options:
                  expression: "#{Internet.emailAddress}"

              - name: "phone"
                type: "string"
                options:
                  expression: "#{PhoneNumber.phoneNumber}"

      - name: "account_ids"
        type: "array"
        fields:
          - name: "account_id"
            type: "string"
            options:
              regex: "ACC[0-9]{8}"
```

### Kafka Task
```yaml
name: "kafka_event_task"
steps:
  - name: "user_events"
    type: "json"
    options:
      topic: "user-events"
    count:
      records: 1000
    fields:
      # Kafka key
      - name: "key"
        type: "string"
        options:
          sql: "content.user_id"

      # Kafka value (JSON payload)
      - name: "value"
        type: "string"
        options:
          sql: "to_json(content)"

      # Kafka headers
      - name: "headers"
        type: "array<struct<key: string, value: binary>>"
        options:
          sql: |
            array(
              named_struct('key', 'event-type', 'value', to_binary(content.event_type, 'utf-8')),
              named_struct('key', 'timestamp', 'value', to_binary(content.timestamp, 'utf-8'))
            )

      # Message content
      - name: "content"
        type: "struct"
        fields:
          - name: "user_id"
            type: "string"
            options:
              regex: "USER[0-9]{8}"

          - name: "event_type"
            type: "string"
            options:
              oneOf: ["login", "logout", "purchase", "view"]

          - name: "timestamp"
            type: "timestamp"

          - name: "metadata"
            type: "struct"
            fields:
              - name: "ip_address"
                type: "string"
                options:
                  expression: "#{Internet.ipV4Address}"

              - name: "user_agent"
                type: "string"
                options:
                  expression: "#{Internet.userAgentAny}"
```

### HTTP Task
```yaml
name: "http_api_task"
steps:
  - name: "create_user"
    type: "http"
    options:
      url: "http://api.example.com/users"
      method: "POST"
    count:
      records: 100
    fields:
      # HTTP headers
      - name: "headers"
        type: "map<string,string>"
        options:
          staticValue:
            Content-Type: "application/json"
            Authorization: "Bearer token123"

      # Path parameters
      - name: "pathParams"
        type: "struct"
        fields:
          - name: "userId"
            type: "string"
            options:
              regex: "USER[0-9]{5}"

      # Query parameters
      - name: "queryParams"
        type: "struct"
        fields:
          - name: "includeDetails"
            type: "boolean"
            options:
              staticValue: true

      # Request body
      - name: "body"
        type: "struct"
        fields:
          - name: "username"
            type: "string"
            options:
              expression: "#{Internet.username}"

          - name: "email"
            type: "string"
            options:
              expression: "#{Internet.emailAddress}"

          - name: "age"
            type: "int"
            options:
              min: 18
              max: 80
```

### Record Count Strategies

**Fixed Record Count**
```yaml
count:
  records: 1000
```

**Records Per Field Value**
```yaml
count:
  records: 5
  perField:
    fieldNames: ["customer_id"]
```

**Records Per Field with Distribution**
```yaml
count:
  records: 10
  perField:
    fieldNames: ["customer_id", "product_type"]
    options:
      oneOf: ["1->0.5", "2->0.3", "3->0.2"]  # Distribution: 50%, 30%, 20%
```

**Records Per Field with Range**
```yaml
count:
  perField:
    fieldNames: ["account_id"]
    options:
      min: 1
      max: 10
```

## 3. Field Generation Options

### Regex Patterns
```yaml
- name: "account_id"
  type: "string"
  options:
    regex: "ACC[0-9]{8}"

- name: "product_code"
  type: "string"
  options:
    regex: "[A-Z]{3}-[0-9]{4}"

- name: "status"
  type: "string"
  options:
    regex: "(ACTIVE|INACTIVE|PENDING)"
```

### Faker Expressions
```yaml
# Names
- name: "first_name"
  options:
    expression: "#{Name.firstName}"

- name: "full_name"
  options:
    expression: "#{Name.name}"

# Contact
- name: "email"
  options:
    expression: "#{Internet.emailAddress}"

- name: "phone"
  options:
    expression: "#{PhoneNumber.phoneNumber}"

# Address
- name: "city"
  options:
    expression: "#{Address.city}"

- name: "country"
  options:
    expression: "#{Address.country}"

# Business
- name: "company"
  options:
    expression: "#{Company.name}"

- name: "job_title"
  options:
    expression: "#{Job.title}"
```

### SQL Expressions
```yaml
# Reference other fields
- name: "full_name"
  options:
    sql: "CONCAT(first_name, ' ', last_name)"

# Conditional logic
- name: "status_code"
  options:
    sql: "CASE WHEN status = 'active' THEN 1 ELSE 0 END"

# Date operations
- name: "expiry_date"
  options:
    sql: "DATE_ADD(created_date, 365)"

# Array operations
- name: "total_transactions"
  options:
    sql: "SIZE(transaction_list)"

# Filter operations
- name: "active_count"
  options:
    sql: "SIZE(FILTER(items, x -> x.status = 'active'))"
```

### Discrete Values
```yaml
# Equal probability for each value
- name: "status"
  options:
    oneOf: ["active", "inactive", "pending", "suspended"]

- name: "priority"
  options:
    oneOf: ["low", "medium", "high", "critical"]

# Weighted probability for each value
- name: "priority"
  options:
    oneOfWeighted: ["low->0.1", "medium->0.7", "high->0.2", "critical->0.0"]

# Weighted probability in perField count
count:
  records: 100
  perField:
    fieldNames: ["account_id"]
    options:
      oneOf: ["1->0.6", "2->0.25", "3->0.1", "4->0.05"] # 60% have 1 record per account_id, 25% have 2 records, 10% have 3 records, 5% have 4 records
```

### Numeric Ranges
```yaml
- name: "age"
  type: "int"
  options:
    min: 18
    max: 65

- name: "price"
  type: "double"
  options:
    min: 0.01
    max: 999.99

- name: "balance"
  type: "decimal(10,2)"
  options:
    min: 0
    max: 100000
```

### Date Ranges
```yaml
- name: "created_date"
  type: "date"
  options:
    min: "2020-01-01"
    max: "2024-12-31"

- name: "updated_at"
  type: "timestamp"
  options:
    min: "2024-01-01T00:00:00Z"
```

### Unique and Primary Key Constraints
```yaml
- name: "customer_id"
  options:
    regex: "CUST[0-9]{8}"
    isPrimaryKey: true

- name: "email"
  options:
    expression: "#{Internet.emailAddress}"
    isUnique: true
```

### Hidden Fields (Computed but Not Output)
```yaml
- name: "_temp_name"
  options:
    expression: "#{Name.name}"
    omit: true

- name: "display_name"
  options:
    sql: "_temp_name"
```

### Incremental/Sequential Fields
```yaml
- name: "sequence_id"
  type: "int"
  options:
    incremental: 1  # Start from 1

- name: "customer_number"
  type: "int"
  options:
    incremental: 1000000  # Start from 1000000
```

## 4. Validation Configuration

Validations can be defined inline with tasks or in separate validation files.

### Inline Validations in Task
```yaml
name: "customer_task"
steps:
  - name: "customers"
    type: "postgres"
    options:
      dbtable: "public.customers"
    fields:
      - name: "customer_id"
        options:
          regex: "CUST[0-9]{8}"
      - name: "email"
        options:
          expression: "#{Internet.emailAddress}"
      - name: "age"
        type: "int"
        options:
          min: 18
          max: 90
    validations:
      # Field validations
      - field: "customer_id"
        validation:
          - type: "matches"
            pattern: "CUST[0-9]{8}"

      - field: "email"
        validation:
          - type: "matches"
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
          - type: "null"
            negate: true  # Must not be null

      - field: "age"
        validation:
          - type: "between"
            min: 18
            max: 90

      # Expression validations
      - expr: "age >= 18"
      - expr: "LENGTH(email) > 5"

      # Uniqueness
      - unique: ["customer_id"]
      - unique: ["email"]
```

### Standalone Validation File
```yaml
name: "data_quality_checks"
description: "Validate customer data quality"
dataSources:
  postgres_customers:
    - options:
        dbtable: "public.customers"
      validations:
        # Field-level validations
        - field: "email"
          validation:
            - type: "matches"
              pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
              errorThreshold: 0.01  # Allow 1% errors

        - field: "age"
          validation:
            - type: "greaterThan"
              value: 0
            - type: "lessThan"
              value: 150

        - field: "balance"
          validation:
            - type: "between"
              min: 0
              max: 1000000
              errorThreshold: 5  # Allow 5 errors

        # Expression validations
        - expr: "LENGTH(first_name) > 0"
          errorThreshold: 0

        - expr: "email LIKE '%@%'"

        # Aggregation validations
        - groupBy:
            fields: []
            aggregation: "count"
          validation:
            - type: "equal"
              value: 1000

        - groupBy:
            fields: ["status"]
            aggregation: "count"
          validation:
            - type: "greaterThan"
              value: 0

        - groupBy:
            fields: ["country"]
            aggregation: "sum"
            aggregationField: "balance"
          validation:
            - type: "lessThan"
              value: 10000000
```

### Advanced Validations

**Null Checks**
```yaml
- field: "required_field"
  validation:
    - type: "null"
      negate: true  # Must not be null

- field: "optional_field"
  validation:
    - type: "null"
      # Can be null
```

**Value Comparisons**
```yaml
- field: "amount"
  validation:
    - type: "greaterThan"
      value: 0
    - type: "lessThan"
      value: 10000

- field: "status_code"
  validation:
    - type: "equal"
      value: 200
```

**Set Membership**
```yaml
- field: "status"
  validation:
    - type: "in"
      values: ["active", "inactive", "pending"]
```

**Uniqueness**
```yaml
- unique: ["customer_id"]
- unique: ["email", "phone"]  # Composite uniqueness
```

**Array Size**
```yaml
- field: "transaction_list"
  validation:
    - type: "greaterThanSize"
      value: 0
    - type: "lessThanSize"
      value: 100
```

**Aggregations**
```yaml
# Total count
- groupBy:
    fields: []
    aggregation: "count"
  validation:
    - type: "equal"
      value: 1000

# Group by field
- groupBy:
    fields: ["customer_id"]
    aggregation: "count"
  validation:
    - type: "greaterThan"
      value: 0

# Sum aggregation
- groupBy:
    fields: ["region"]
    aggregation: "sum"
    aggregationField: "revenue"
  validation:
    - type: "lessThan"
      value: 1000000

# Min/Max aggregations
- groupBy:
    fields: ["category"]
    aggregation: "max"
    aggregationField: "price"
  validation:
    - type: "lessThan"
      value: 10000
```

**Cross-Dataset Validations**
```yaml
- upstreamData:
    dataSource: "source_db"
    step: "accounts"
  joinFields: ["account_id"]
  joinType: "inner"
  validations:
    - field: "source_db.balance"
      validation:
        - type: "equalField"
          compareField: "target_db.balance"

# Anti-join validation (records that don't match)
- upstreamData:
    dataSource: "source_db"
    step: "deleted_accounts"
  joinFields: ["account_id"]
  joinType: "anti"
  validations:
    - groupBy:
        fields: []
        aggregation: "count"
      validation:
        - type: "equal"
          value: 0  # No matches expected
```

## 5. Connection Configuration

Define data source connections and configuration in `application.conf` file (HOCON format).

### Application Configuration (application.conf)

**File location**: `app/src/main/resources/application.conf` or specify via `APPLICATION_CONFIG_PATH` environment variable.

```hocon
# Feature flags
flags {
    enableCount = true
    enableCount = ${?ENABLE_COUNT}
    enableGenerateData = true
    enableGenerateData = ${?ENABLE_GENERATE_DATA}
    enableGeneratePlanAndTasks = false
    enableGeneratePlanAndTasks = ${?ENABLE_GENERATE_PLAN_AND_TASKS}
    enableRecordTracking = false
    enableRecordTracking = ${?ENABLE_RECORD_TRACKING}
    enableDeleteGeneratedRecords = false
    enableDeleteGeneratedRecords = ${?ENABLE_DELETE_GENERATED_RECORDS}
    enableFailOnError = true
    enableFailOnError = ${?ENABLE_FAIL_ON_ERROR}
    enableUniqueCheck = true
    enableUniqueCheck = ${?ENABLE_UNIQUE_CHECK}
    enableSinkMetadata = false
    enableSinkMetadata = ${?ENABLE_SINK_METADATA}
    enableSaveReports = true
    enableSaveReports = ${?ENABLE_SAVE_REPORTS}
    enableValidation = false
    enableValidation = ${?ENABLE_VALIDATION}
    enableGenerateValidations = false
    enableGenerateValidations = ${?ENABLE_GENERATE_VALIDATIONS}
    enableAlerts = false
    enableAlerts = ${?ENABLE_ALERTS}
    enableUniqueCheckOnlyInBatch = false
    enableUniqueCheckOnlyInBatch = ${?ENABLE_UNIQUE_CHECK_ONLY_IN_BATCH}
    enableFastGeneration = false
    enableFastGeneration = ${?ENABLE_FAST_GENERATION}
}

# Folder paths
folders {
    generatedPlanAndTaskFolderPath = "/tmp"
    generatedPlanAndTaskFolderPath = ${?GENERATED_PLAN_AND_TASK_FOLDER_PATH}
    planFilePath = "app/src/test/resources/sample/plan/customer-create-plan.yaml"
    planFilePath = ${?PLAN_FILE_PATH}
    taskFolderPath = "app/src/test/resources/sample/task"
    taskFolderPath = ${?TASK_FOLDER_PATH}
    validationFolderPath = "app/src/test/resources/sample/validation"
    validationFolderPath = ${?VALIDATION_FOLDER_PATH}
    recordTrackingFolderPath = "/tmp/data/generated/recordTracking"
    recordTrackingFolderPath = ${?RECORD_TRACKING_FOLDER_PATH}
    recordTrackingForValidationFolderPath = "/tmp/data/validation/recordTracking"
    recordTrackingForValidationFolderPath = ${?RECORD_TRACKING_VALIDATION_FOLDER_PATH}
    generatedReportsFolderPath = "app/src/test/resources/sample/html"
    generatedReportsFolderPath = ${?GENERATED_REPORTS_FOLDER_PATH}
}

# Metadata configuration
metadata {
    numRecordsFromDataSource = 10000
    numRecordsFromDataSource = ${?NUM_RECORDS_FROM_DATA_SOURCE}
    numRecordsForAnalysis = 10000
    numRecordsForAnalysis = ${?NUM_RECORDS_FOR_ANALYSIS}
    oneOfDistinctCountVsCountThreshold = 0.1
    oneOfDistinctCountVsCountThreshold = ${?ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD}
    oneOfMinCount = 1000
    oneOfMinCount = ${?ONE_OF_MIN_COUNT}
    numGeneratedSamples = 10
    numGeneratedSamples = ${?NUM_GENERATED_SAMPLES}
}

# Generation configuration
generation {
    numRecordsPerBatch = 1000000
    numRecordsPerBatch = ${?NUM_RECORDS_PER_BATCH}
    uniqueBloomFilterNumItems = 1000000
    uniqueBloomFilterNumItems = ${?UNIQUE_BLOOM_FILTER_NUM_ITEMS}
    uniqueBloomFilterFalsePositiveProbability = 0.01
    uniqueBloomFilterFalsePositiveProbability = ${?UNIQUE_BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY}
}

# Validation configuration
validation {
    numSampleErrorRecords = 5
    numSampleErrorRecords = ${?NUM_SAMPLE_ERROR_RECORDS}
    enableDeleteRecordTrackingFiles = true
    enableDeleteRecordTrackingFiles = ${?ENABLE_DELETE_RECORD_TRACKING_FILES}
}

# Alert configuration
alert {
    triggerOn = "all"
    triggerOn = ${?ALERT_TRIGGER_ON}
    slackAlertConfig {
        token = ""
        token = ${?ALERT_SLACK_TOKEN}
        channels = []
        channels = ${?ALERT_SLACK_CHANNELS}
    }
}

# Runtime configuration (Spark settings)
runtime {
    master = "local[*]"
    master = ${?DATA_CATERER_MASTER}
    config {
        "spark.driver.memory" = "6g",
        "spark.executor.memory" = "6g",
        "spark.sql.shuffle.partitions" = "10"
    }
}

# Database connections
jdbc {
    postgres {
        url = "jdbc:postgresql://localhost:5432/customer"
        url = ${?POSTGRES_URL}
        user = "postgres"
        user = ${?POSTGRES_USER}
        password = "postgres"
        password = ${?POSTGRES_PASSWORD}
        driver = "org.postgresql.Driver"
    }
    mysql {
        url = "jdbc:mysql://localhost:3306/customer"
        url = ${?MYSQL_URL}
        user = "root"
        user = ${?MYSQL_USERNAME}
        password = "root"
        password = ${?MYSQL_PASSWORD}
        driver = "com.mysql.cj.jdbc.Driver"
    }
}

# Cassandra connection
org.apache.spark.sql.cassandra {
    cassandra {
        spark.cassandra.connection.host = "localhost"
        spark.cassandra.connection.host = ${?CASSANDRA_HOST}
        spark.cassandra.connection.port = "9042"
        spark.cassandra.connection.port = ${?CASSANDRA_PORT}
        spark.cassandra.auth.username = "cassandra"
        spark.cassandra.auth.username = ${?CASSANDRA_USERNAME}
        spark.cassandra.auth.password = "cassandra"
        spark.cassandra.auth.password = ${?CASSANDRA_PASSWORD}
    }
}

# Kafka connection
kafka {
    kafka {
        kafka.bootstrap.servers = "localhost:9092"
        kafka.bootstrap.servers = ${?KAFKA_BOOTSTRAP_SERVERS}
    }
}

# JMS connections (Solace, RabbitMQ)
jms {
    solace {
        initialContextFactory = "com.solacesystems.jndi.SolJNDIInitialContextFactory"
        initialContextFactory = ${?SOLACE_INITIAL_CONTEXT_FACTORY}
        connectionFactory = "/jms/cf/default"
        connectionFactory = ${?SOLACE_CONNECTION_FACTORY}
        url = "smf://localhost:55554"
        url = ${?SOLACE_URL}
        user = "admin"
        user = ${?SOLACE_USER}
        password = "admin"
        password = ${?SOLACE_PASSWORD}
        vpnName = "default"
        vpnName = ${?SOLACE_VPN}
    }
    rabbitmq {
        connectionFactory = "com.rabbitmq.jms.admin.RMQConnectionFactory"
        connectionFactory = ${?RABBITMQ_CONNECTION_FACTORY}
        url = "amqp://localhost:5672"
        url = ${?RABBITMQ_URL}
        user = "guest"
        user = ${?RABBITMQ_USER}
        password = "guest"
        password = ${?RABBITMQ_PASSWORD}
        virtualHost = "/"
        virtualHost = ${?RABBITMQ_VIRTUAL_HOST}
    }
}

# HTTP connection
http {
    http {
        # Add custom options here
    }
}
```

**Key Configuration Sections:**

1. **flags** - Feature toggles for data generation, validation, tracking, etc.
2. **folders** - File paths for plans, tasks, validations, and reports
3. **metadata** - Metadata analysis settings (sampling, thresholds)
4. **generation** - Batch sizes and unique value tracking
5. **validation** - Validation-specific settings
6. **alert** - Alerting configuration (Slack, etc.)
7. **runtime** - Spark configuration
8. **jdbc** - Database connections (Postgres, MySQL)
9. **org.apache.spark.sql.cassandra** - Cassandra connection
10. **kafka** - Kafka broker configuration
11. **jms** - JMS connections (Solace, RabbitMQ)
12. **http** - HTTP-specific options

**Environment Variable Substitution:**
- Use `${?VAR_NAME}` syntax to allow environment variables to override config values
- Example: `enableValidation = ${?ENABLE_VALIDATION}` allows `ENABLE_VALIDATION=true` to override the default
```

## Common Use Cases

### Use Case 1: Generate Database Tables with Relationships

**Plan: customer-order-plan.yaml**
```yaml
name: "customer_order_plan"
description: "Generate customers and their orders"
tasks:
  - name: "customer_task"
    dataSourceName: "my_postgres"
  - name: "order_task"
    dataSourceName: "my_postgres"

sinkOptions:
  foreignKeys:
    - source:
        dataSource: "my_postgres"
        step: "customers"
        fields: ["customer_id"]
      generate:
        - dataSource: "my_postgres"
          step: "orders"
          fields: ["customer_id"]
```

**Task: customer-task.yaml**
```yaml
name: "customer_task"
steps:
  - name: "customers"
    type: "postgres"
    options:
      dbtable: "public.customers"
    count:
      records: 100
    fields:
      - name: "customer_id"
        options:
          regex: "CUST[0-9]{8}"
          isPrimaryKey: true
      - name: "first_name"
        options:
          expression: "#{Name.firstName}"
      - name: "last_name"
        options:
          expression: "#{Name.lastName}"
      - name: "email"
        options:
          expression: "#{Internet.emailAddress}"
          isUnique: true
```

**Task: order-task.yaml**
```yaml
name: "order_task"
steps:
  - name: "orders"
    type: "postgres"
    options:
      dbtable: "public.orders"
    count:
      perField:
        fieldNames: ["customer_id"]
        options:
          min: 1
          max: 10
    fields:
      - name: "order_id"
        options:
          regex: "ORD[0-9]{10}"
          isPrimaryKey: true
      - name: "customer_id"
        options:
          regex: "CUST[0-9]{8}"
      - name: "order_date"
        type: "date"
        options:
          min: "2024-01-01"
      - name: "total_amount"
        type: "decimal(10,2)"
        options:
          min: 10
          max: 5000
```

### Use Case 2: Generate CSV Files with Validation

**Task: transaction-csv-task.yaml**
```yaml
name: "transaction_csv_task"
steps:
  - name: "transactions"
    type: "csv"
    options:
      path: "/tmp/transactions.csv"
      header: "true"
    count:
      records: 10000
    fields:
      - name: "transaction_id"
        options:
          regex: "TXN[0-9]{12}"
          isUnique: true
      - name: "account_id"
        options:
          regex: "ACC[0-9]{8}"
      - name: "transaction_date"
        type: "date"
        options:
          min: "2024-01-01"
          max: "2024-12-31"
      - name: "amount"
        type: "decimal(10,2)"
        options:
          min: 0.01
          max: 10000
      - name: "transaction_type"
        options:
          oneOf: ["debit", "credit"]
      - name: "status"
        options:
          oneOf: ["pending", "completed", "failed"]
    validations:
      - field: "amount"
        validation:
          - type: "greaterThan"
            value: 0
      - expr: "YEAR(transaction_date) = 2024"
      - groupBy:
          fields: ["account_id"]
          aggregation: "count"
        validation:
          - type: "greaterThan"
            value: 0
```

### Use Case 3: Generate Kafka Events

**Task: kafka-events-task.yaml**
```yaml
name: "kafka_events_task"
steps:
  - name: "user_events"
    type: "json"
    options:
      topic: "user-events"
    count:
      records: 1000
    fields:
      - name: "key"
        options:
          sql: "content.user_id"

      - name: "value"
        options:
          sql: "to_json(content)"

      - name: "headers"
        type: "array<struct<key: string, value: binary>>"
        options:
          sql: |
            array(
              named_struct('key', 'event-type', 'value', to_binary(content.event_type, 'utf-8'))
            )

      - name: "content"
        type: "struct"
        fields:
          - name: "user_id"
            options:
              regex: "USER[0-9]{8}"
          - name: "event_type"
            options:
              oneOf: ["login", "logout", "purchase"]
          - name: "timestamp"
            type: "timestamp"
          - name: "data"
            type: "struct"
            fields:
              - name: "product_id"
                options:
                  regex: "PROD[0-9]{6}"
              - name: "amount"
                type: "double"
                options:
                  min: 1
                  max: 1000
```

### Use Case 4: Multi-Source Data Pipeline

**Plan: multi-source-plan.yaml**
```yaml
name: "multi_source_pipeline"
description: "Generate data across Postgres, JSON, and Kafka"
tasks:
  - name: "postgres_account_task"
    dataSourceName: "my_postgres"
  - name: "json_account_task"
    dataSourceName: "my_json"
  - name: "kafka_event_task"
    dataSourceName: "my_kafka"

sinkOptions:
  foreignKeys:
    - source:
        dataSource: "my_postgres"
        step: "accounts"
        fields: ["account_id", "name"]
      generate:
        - dataSource: "my_json"
          step: "account_info"
          fields: ["account_id", "account_name"]
        - dataSource: "my_kafka"
          step: "account_events"
          fields: ["account_id", "name"]
```

## Key Patterns to Remember

1. **Use three-file structure**: Plans, Tasks, and Validations for better organization
2. **Define connections centrally** in application.yaml for reuse
3. **Use foreign keys in plans** to maintain referential integrity
4. **Leverage SQL expressions** for complex field generation
5. **Add validations early** to catch data quality issues
6. **Use error thresholds** for flexible validation rules
7. **Enable fast generation** for better performance with regex patterns
8. **Organize tasks by data source type** in subdirectories
9. **Use descriptive names** for tasks and steps
10. **Document complex SQL** with YAML multiline strings

## Running YAML Configurations

### Using Environment Variables
```bash
# Set plan and task locations
export PLAN_FILE_PATH="/path/to/plan/my-plan.yaml"
export TASK_FOLDER_PATH="/path/to/tasks"
export VALIDATION_FOLDER_PATH="/path/to/validations"
export APPLICATION_CONFIG_PATH="/path/to/application.yaml"
export GENERATED_REPORTS_FOLDER_PATH="/tmp/reports"

# Enable features
export ENABLE_GENERATE_DATA=true
export ENABLE_VALIDATION=true
export ENABLE_FAST_GENERATION=true

# Run Data Caterer
./gradlew :app:run
```

### Using Docker
```bash
docker run \
  -v /path/to/config:/opt/app/config \
  -v /path/to/data:/opt/app/data \
  -e PLAN_FILE_PATH=/opt/app/config/plan/my-plan.yaml \
  -e TASK_FOLDER_PATH=/opt/app/config/task \
  -e ENABLE_FAST_GENERATION=true \
  datacatering/data-caterer:latest
```

### Using Gradle
```bash
# Run with specific plan
./gradlew :app:run -Dplan.file=/path/to/plan.yaml

# Run with all configurations
./gradlew :app:run \
  -Dplan.file=/path/to/plan.yaml \
  -Dtask.folder=/path/to/tasks \
  -Dvalidation.folder=/path/to/validations
```

## Troubleshooting

**Issue: Foreign keys not working**
```yaml
# Make sure field names match exactly between source and target
# Check data source and step names are correct
sinkOptions:
  foreignKeys:
    - source:
        dataSource: "my_postgres"  # Must match connection name
        step: "customers"          # Must match step name in task
        fields: ["customer_id"]    # Must match field name
```

**Issue: Validation failures**
```yaml
# Add error thresholds for flexibility
- field: "amount"
  validation:
    - type: "greaterThan"
      value: 0
      errorThreshold: 0.05  # Allow 5% errors
```

**Issue: Slow generation**
```yaml
# Enable fast generation in application.yaml
flags:
  enableFastGeneration: true

# Use simple regex patterns
- name: "code"
  options:
    regex: "[A-Z]{3}[0-9]{5}"  # Fast
```

**Issue: File paths not working**
```yaml
# Use absolute paths or ensure relative paths are from execution directory
options:
  path: "/tmp/data/file.csv"  # Absolute path (recommended)
  # OR
  path: "data/file.csv"       # Relative to working directory
```

## See Also

- Main project documentation: `/docs`
- Example YAML configurations: `/app/src/test/resources/sample`
- Java API guide: `/misc/llm/JAVA_GUIDE.md`
- Scala API guide: `/misc/llm/SCALA_GUIDE.md`
