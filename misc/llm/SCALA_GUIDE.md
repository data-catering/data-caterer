# Data Caterer Scala API Guide for Code Assistants

This guide helps code assistants understand how to use Data Caterer's Scala API to help users generate test data, validate data quality, and manage test data lifecycle.

## Quick Start Pattern

Every Data Caterer Scala implementation follows this structure:

```scala
import io.github.datacatering.datacaterer.api.PlanRun

class MyDataPlan extends PlanRun {

  val task1 = <dataSource>("name", "connection-details")
    .fields(...)
    .count(...)

  val config = configuration
    .generatedReportsFolderPath("/path/to/reports")

  val myPlan = plan.addForeignKeyRelationship(...)

  execute(myPlan, config, task1, task2, ...)
}
```

## Core Building Blocks

### 1. Data Sources

**CSV Files**
```scala
val csvTask = csv("my_csv", "/path/to/csv", Map("header" -> "true"))
  .fields(...)
  .count(count.records(100))
```

**JSON Files**
```scala
val jsonTask = json("my_json", "/path/to/json")
  .fields(...)
  .count(count.records(100))
```

**Parquet Files**
```scala
val parquetTask = parquet("my_parquet", "/path/to/parquet")
  .fields(...)
  .count(count.records(100))
```

**PostgreSQL**
```scala
val postgresTask = postgres("my_postgres", "jdbc:postgresql://localhost:5432/mydb", "user", "password")
  .table("schema_name", "table_name")
  .fields(...)
  .count(count.records(100))
```

**MySQL**
```scala
val mysqlTask = mysql("my_mysql", "jdbc:mysql://localhost:3306/mydb", "user", "password")
  .table("schema_name", "table_name")
  .fields(...)
  .count(count.records(100))
```

**Cassandra**
```scala
val cassandraTask = cassandra("my_cassandra", "localhost:9042", "user", "password")
  .table("keyspace", "table_name")
  .fields(
    field.name("id").regex("ID[0-9]{8}").primaryKey(true),
    field.name("created_at").`type`(TimestampType).clusteringPosition(1)
  )
  .count(count.records(100))
```

**Kafka**
```scala
val kafkaTask = kafka("my_kafka", "localhost:9092")
  .topic("topic_name")
  .fields(
    field.name("key").sql("body.id"),
    field.messageHeaders(
      field.messageHeader("correlation-id", "body.id")
    ),
    field.messageBody(
      field.name("id").regex("ID[0-9]{8}"),
      field.name("data").fields(...)
    )
  )
  .count(count.records(100))
```

**HTTP/REST APIs**
```scala
val httpTask = http("my_http", Map(ROWS_PER_SECOND -> "1"))
  .fields(
    field.httpHeader("Content-Type", "application/json"),
    field.httpUrl(
      "http://api.example.com/resource/{id}",
      HttpMethodEnum.POST,
      List(field.name("id").regex("ID[0-9]{5}")),  // Path parameters
      List(field.name("limit").`type`(IntegerType).min(1).max(100))  // Query parameters
    ),
    field.httpBody(
      field.name("account_id").regex("ACC[0-9]{8}"),
      field.name("details").fields(
        field.name("name").expression("#{Name.name}"),
        field.name("amount").`type`(DoubleType).min(1.0).max(1000.0)
      )
    )
  )
  .validations(
    validation.field("response.statusCode").isEqual(200),
    validation.field("response.timeTaken").lessThan(100)
  )
```

**Solace (JMS)**
```scala
val solaceTask = solace("my_solace", "smf://localhost:55554")
  .queue("my-queue")
  .fields(
    field.name("account_id").regex("ACC[0-9]{8}"),
    field.name("amount").`type`(DoubleType).min(1).max(1000)
  )
  .count(count.records(100))
```

### 2. Field Definitions

**Basic Field with Regex Pattern**
```scala
field.name("account_id").regex("ACC[0-9]{8}")
```

**Field with Faker Expression**
```scala
field.name("customer_name").expression("#{Name.name}")
field.name("email").expression("#{Internet.emailAddress}")
field.name("city").expression("#{Address.city}")
field.name("phone").expression("#{PhoneNumber.phoneNumber}")
```

**Field with SQL Expression**
```scala
// Reference other fields
field.name("full_name").sql("CONCAT(first_name, ' ', last_name)")

// Conditional logic
field.name("status_code").sql("CASE WHEN status = 'active' THEN 1 ELSE 0 END")

// Date operations
field.name("expiry_date").sql("DATE_ADD(created_date, 365)")

// Array operations
field.name("total_transactions").sql("SIZE(transaction_list)")
field.name("active_transactions").sql("SIZE(FILTER(transaction_list, x -> x.status = 'active'))")
```

**Field with Constraints**
```scala
field.name("age")
  .`type`(IntegerType)
  .min(18)
  .max(65)
```

**Field with Discrete Values**
```scala
// Equal probability for each value
field.name("status").oneOf("active", "inactive", "pending", "suspended")

// Weighted distribution (returns list of fields including hidden weight field)
val statusFields = field.name("status").oneOfWeighted(
  ("active", 0.6),      // 60% active
  ("inactive", 0.25),   // 25% inactive
  ("pending", 0.1),     // 10% pending
  ("suspended", 0.05)   // 5% suspended
)
// Returns list of fields including hidden weight field
```

**Unique Fields**
```scala
field.name("email").expression("#{Internet.emailAddress}").unique(true)
```

**Primary Key Fields**
```scala
field.name("id").regex("ID[0-9]{8}").primaryKey(true).primaryKeyPosition(1)
```

**Hidden/Computed Fields**
```scala
// Generate field but don't include in output
field.name("_temp_value").expression("#{Name.name}").omit(true)
field.name("display_name").sql("_temp_value")
```

**Incremental/Sequential Fields**
```scala
field.name("id").`type`(IntegerType).sql("ROW_NUMBER() OVER (ORDER BY account_number)")
```

### 3. Data Types

```scala
// Numeric types
field.name("quantity").`type`(IntegerType).min(1).max(1000)
field.name("price").`type`(DoubleType).min(0.01).max(999.99)
field.name("total").`type`(DecimalType(10, 2)).min(0).max(10000)
field.name("amount").`type`(LongType)

// String type (default)
field.name("description").`type`(StringType)

// Date and time
field.name("created_date").`type`(DateType).min(Date.valueOf("2020-01-01"))
field.name("updated_at").`type`(TimestampType)

// Boolean
field.name("is_active").`type`(BooleanType)

// Complex types
field.name("metadata").`type`(StructType)
field.name("tags").`type`(ArrayType)
field.name("attributes").`type`(MapType)

// Binary
field.name("file_content").`type`(BinaryType)
```

### 4. Nested Structures

**Nested Objects (Structs)**
```scala
field.name("customer_details")
  .`type`(StructType)
  .fields(
    field.name("first_name").expression("#{Name.firstName}"),
    field.name("last_name").expression("#{Name.lastName}"),
    field.name("contact").fields(
      field.name("email").expression("#{Internet.emailAddress}"),
      field.name("phone").expression("#{PhoneNumber.phoneNumber}")
    )
  )
```

**Arrays**
```scala
field.name("transaction_list")
  .`type`(ArrayType)
  .fields(
    field.name("txn_id").regex("TXN[0-9]{10}"),
    field.name("amount").`type`(DoubleType).min(1).max(1000),
    field.name("date").`type`(DateType)
  )
```

**Complex Nested Example**
```scala
field.name("account")
  .fields(
    field.name("id").regex("ACC[0-9]{8}"),
    field.name("holder").fields(
      field.name("name").expression("#{Name.name}"),
      field.name("contacts").`type`(ArrayType).fields(
        field.name("type").oneOf("email", "phone", "address"),
        field.name("value"),
        field.name("is_primary").`type`(BooleanType)
      )
    ),
    field.name("transactions").`type`(ArrayType).fields(
      field.name("id").regex("TXN[0-9]{10}"),
      field.name("date").`type`(DateType),
      field.name("details").fields(
        field.name("amount").`type`(DoubleType),
        field.name("currency").oneOf("USD", "EUR", "GBP")
      )
    )
  )
```

**Unwrap Top-Level Array**
```scala
// For JSON files that are top-level arrays
val jsonTask = json("my_json", "/path/to/json")
  .fields(
    field.name("items").`type`(ArrayType).unwrapTopLevelArray(true)
      .fields(
        field.name("id"),
        field.name("value")
      )
  )
```

### 5. Record Count Strategies

**Fixed Number of Records**
```scala
.count(count.records(1000))
```

**Records Per Field Value**
```scala
// Generate 1-5 records for each distinct customer_id
.count(count.recordsPerFieldGenerator(generator.min(1).max(5), "customer_id"))

// Generate exactly 3 records per customer_id
.count(count.recordsPerField(3, "customer_id"))
```

**Records Per Multiple Fields**
```scala
// Generate records per combination of customer_id and product_id
.count(count.recordsPerField(10, "customer_id", "product_id"))
```

**Records with Distribution**
```scala
// Generate with specific distribution
.count(
  count.recordsPerField(5, "customer_id")
    .generator(generator.min(1).max(10))
)
```

### 6. Foreign Key Relationships

**Single Column Foreign Key**
```scala
val myPlan = plan.addForeignKeyRelationship(
  accountTask, "account_id",
  List(transactionTask -> "account_id", balanceTask -> "account_id")
)
```

**Multi-Column Foreign Key**
```scala
val myPlan = plan.addForeignKeyRelationship(
  customerTask, List("customer_id", "region"),
  List(orderTask -> List("customer_id", "customer_region"))
)
```

**Multiple Foreign Key Relationships**
```scala
val myPlan = plan
  .addForeignKeyRelationship(
    customerTask, "customer_id",
    List(accountTask -> "customer_id")
  )
  .addForeignKeyRelationship(
    accountTask, "account_id",
    List(transactionTask -> "account_id", balanceTask -> "account_id")
  )
```

**Foreign Keys Across Data Sources**
```scala
val myPlan = plan.addForeignKeyRelationship(
  foreignField("postgres_conn", "accounts", "account_id"),
  foreignField("postgres_conn", "transactions", "account_id"),
  foreignField("json_conn", "account_info", "account_id"),
  foreignField("kafka_conn", "events", "account_id")
)
```

**Multi-Column Foreign Keys Across Sources**
```scala
val accountPlan = plan
  .addForeignKeyRelationship(
    foreignField("my_postgres", "account", "account_id"),
    foreignField("my_postgres", "transaction", "account_id"),
    foreignField("my_json", "account_info", "account_id")
  )
  .addForeignKeyRelationship(
    foreignField("my_postgres", "account", "name"),
    foreignField("my_postgres", "transaction", "name"),
    foreignField("my_json", "account_info", "name")
  )
```

### 7. Data Validations

**Field-Level Validations**
```scala
.validations(
  // Value checks
  validation.field("amount").greaterThan(0),
  validation.field("amount").lessThan(10000),
  validation.field("amount").between(1, 9999),

  // Pattern matching
  validation.field("email").matches("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"),

  // Null checks
  validation.field("required_field").isNull(false),

  // Set membership
  validation.field("status").in("active", "inactive", "pending"),

  // Uniqueness
  validation.unique("account_id"),
  validation.unique("customer_id", "email")
)
```

**Expression-Based Validations**
```scala
.validations(
  // SQL expressions
  validation.expr("amount > 0 AND amount < balance"),
  validation.expr("YEAR(created_date) >= 2020"),
  validation.expr("LENGTH(description) > 10")
)
```

**Aggregation Validations**
```scala
.validations(
  // Total count check
  validation.groupBy().count().isEqual(1000),

  // Group by single field
  validation.groupBy("customer_id").count().greaterThan(0),
  validation.groupBy("region").sum("amount").lessThan(1000000),

  // Group by multiple fields
  validation.groupBy("customer_id", "product_type")
    .avg("price").between(10.0, 100.0),

  // Min/max checks
  validation.groupBy("category").min("price").greaterThan(0),
  validation.groupBy("category").max("price").lessThan(10000)
)
```

**Cross-Dataset Validations**
```scala
.validations(
  // Join validation - check matching records
  validation.upstreamData(sourceTask)
    .joinFields("account_id")
    .joinType("inner")
    .validations(
      validation.field("source_task.balance")
        .isEqualField("current_task.balance")
    ),

  // Anti-join validation - check non-matching records
  validation.upstreamData(sourceTask)
    .joinFields("account_id")
    .joinType("anti")
    .validations(validation.count().isEqual(0))
)
```

**Validation with Error Thresholds**
```scala
.validations(
  // Allow up to 10% errors
  validation.field("amount").greaterThan(0).errorThreshold(0.1),

  // Allow up to 5 errors
  validation.field("status").in("active", "inactive").errorThreshold(5)
)
```

**Array Size Validations**
```scala
.validations(
  validation.field("transaction_list").greaterThanSize(0),
  validation.field("tags").lessThanSize(10)
)
```

**Pre-Filter Validations (Subset Validation)**
```scala
// Run validations only on a subset of data that matches pre-filter conditions
.validations(
  // Check that all closed accounts have zero balance
  validation.preFilter(validation.field("status").isEqual("closed"))
    .field("balance").isEqual(0),

  // Multiple pre-filter conditions with AND
  validation.preFilter(
    PreFilterBuilder()
      .filter(validation.field("status").isEqual("active"))
      .and(validation.field("country").isEqual("US"))
  ).field("balance").greaterThan(0),

  // Multiple pre-filter conditions with OR
  validation.preFilter(
    PreFilterBuilder()
      .filter(validation.field("type").isEqual("savings"))
      .or(validation.field("type").isEqual("checking"))
  ).field("interest_rate").greaterThan(0),

  // Pre-filter with expression
  validation.preFilter(validation.expr("STARTSWITH(account_id, 'ACC')"))
    .field("amount").greaterThan(100)
)
```

### 8. Custom Transformations

Apply custom logic to generated files after they are written ("last mile" transformation). Useful for converting to custom formats, restructuring data, or applying business-specific transformations.

**Per-Record Transformation** - Transform each line/record individually:
```scala
val task = csv("my_csv", "/path/to/csv")
  .fields(
    field.name("account_id"),
    field.name("name")
  )
  .count(count.records(100))
  .transformationPerRecord(
    "com.example.MyPerRecordTransformer",
    "transformRecord"  // Method name (default: "transformRecord")
  )
  .transformationOptions(Map(
    "prefix" -> "BANK_",
    "suffix" -> "_VERIFIED"
  ))
```

**Whole-File Transformation** - Transform entire file as a unit:
```scala
val task = json("my_json", "/path/to/json")
  .fields(
    field.name("id"),
    field.name("data")
  )
  .count(count.records(200))
  .transformationWholeFile(
    "com.example.JsonArrayWrapperTransformer",
    "transformFile"  // Method name (default: "transformFile")
  )
  .transformationOptions(Map("minify" -> "true"))
```

**Task-Level Transformation** - Apply to all steps in a task:
```scala
val myTask = task("my_task", "csv")
  .transformation("com.example.MyTransformer")  // Defaults to whole-file mode
  .transformationOptions(Map("format" -> "custom"))
```

**Custom Output Path** - Save transformed file to different location:
```scala
val task = csv("my_csv", "/path/to/original")
  .fields(...)
  .transformationPerRecord("com.example.Transformer")
  .transformationOutput(
    "/path/to/transformed/output.csv",
    true  // Delete original file after transformation
  )
```

**Enable/Disable Transformation**:
```scala
val task = json("my_json", "/path")
  .fields(...)
  .transformationWholeFile("com.example.Transformer")
  .enableTransformation(sys.env.getOrElse("ENABLE_TRANSFORM", "false").toBoolean)
```

**Implementing Transformers**:
```scala
// Per-Record Transformer
class MyPerRecordTransformer {
  def transformRecord(record: String, options: Map[String, String]): String = {
    val prefix = options.getOrElse("prefix", "")
    val suffix = options.getOrElse("suffix", "")
    s"$prefix$record$suffix"
  }
}

// Whole-File Transformer
class MyWholeFileTransformer {
  def transformFile(inputPath: String, outputPath: String, options: Map[String, String]): Unit = {
    val content = scala.io.Source.fromFile(inputPath).mkString
    val transformed = // ... apply transformation ...
    java.nio.file.Files.writeString(java.nio.file.Paths.get(outputPath), transformed)
  }
}
```

### 9. Metadata Source Integration

Metadata sources allow you to automatically extract schemas and validations from external systems. Different metadata sources serve different purposes:

**Data Generation (Schema/Field Definitions):**
- **Marquez** - OpenLineage-based metadata catalog
- **OpenMetadata** - Enterprise metadata management
- **OpenAPI/Swagger** - REST API schema definitions
- **JSON Schema** - JSON schema files
- **ODCS** - Open Data Contract Standard (v2/v3)
- **Data Contract CLI** - Data contract files
- **Confluent Schema Registry** - Kafka schema management
- **YAML Plan/Task** - Reuse existing Data Caterer configurations

**Data Validation (Quality Rules):**
- **Great Expectations** - Extract validation rules from expectation suites
- **OpenMetadata** - Extract test cases as validations
- **ODCS v3** - Extract quality checks from data contracts
- **JSON Schema** - Derive validations from schema constraints

**Pull Schema for Data Generation**
```scala
// From Marquez (OpenLineage metadata catalog)
val task = csv("my_csv", "/path/to/csv")
  .fields(metadataSource.marquez(
    "http://marquez-host:5001",
    "namespace",
    "dataset_name"
  ))
  .count(count.records(100))

// From OpenMetadata (with authentication)
val task = json("my_json", "/path")
  .fields(metadataSource.openMetadata(
    "http://openmetadata-host:8585/api",
    OPEN_METADATA_AUTH_TYPE_OPEN_METADATA,
    Map(
      OPEN_METADATA_JWT_TOKEN -> "your_token",
      OPEN_METADATA_TABLE_FQN -> "database.schema.table"
    )
  ))
  .count(count.records(100))

// Simplified OpenMetadata with token
val task = json("my_json", "/path")
  .fields(metadataSource.openMetadataWithToken(
    "http://openmetadata-host:8585/api",
    "your_jwt_token"
  ))
  .count(count.records(100))

// From OpenAPI/Swagger (for HTTP APIs)
val task = http("my_http")
  .fields(metadataSource.openApi("/path/to/openapi.yaml"))
  .count(count.records(100))

// From JSON Schema
val task = json("my_json", "/path")
  .fields(metadataSource.jsonSchema("/path/to/schema.json"))
  .count(count.records(100))

// From Open Data Contract Standard (ODCS)
val task = csv("my_csv", "/path")
  .fields(metadataSource.openDataContractStandard(
    "/path/to/data-contract.yaml",
    "dataset_name"
  ))
  .count(count.records(100))

// From Data Contract CLI
val task = postgres("my_postgres", "jdbc:...")
  .fields(metadataSource.dataContractCli("/path/to/contract.yaml"))
  .count(count.records(100))

// From Confluent Schema Registry
val task = kafka("my_kafka", "localhost:9092")
  .topic("my-topic")
  .fields(metadataSource.confluentSchemaRegistry(
    "http://schema-registry:8081",
    "my-subject-value"  // Subject name
  ))
  .count(count.records(100))

// From Confluent Schema Registry by ID
val task = kafka("my_kafka", "localhost:9092")
  .topic("my-topic")
  .fields(metadataSource.confluentSchemaRegistry(
    "http://schema-registry:8081",
    12345  // Schema ID
  ))
  .count(count.records(100))

// Reuse existing YAML configurations
val task = csv("my_csv", "/path")
  .fields(metadataSource.yamlTask(
    "/path/to/existing-task.yaml",
    "task_name",
    "step_name"
  ))
  .count(count.records(100))
```

**Extract Validations from Metadata**
```scala
// From Great Expectations (validation rules only)
val task = postgres("my_postgres", "jdbc:...")
  .table("schema", "table")
  .validations(metadataSource.greatExpectations("/path/to/expectations.json"))
  .count(count.records(100))

// From OpenMetadata (includes test cases as validations)
val config = configuration
  .enableGenerateValidations(true)  // Enable validation extraction

val task = postgres("my_postgres", "jdbc:...")
  .fields(metadataSource.openMetadataWithToken(
    "http://openmetadata-host:8585/api",
    "token"
  ))
  .count(count.records(100))

// Auto-generate tasks and validations from metadata
val config = configuration
  .enableGeneratePlanAndTasks(true)      // Auto-generate tasks
  .enableGenerateValidations(true)       // Auto-generate validations

execute(config,
  postgres("my_postgres", "jdbc:postgresql://localhost/db", "user", "password")
    .fields(metadataSource.openDataContractStandard("/path/to/contract.yaml"))
)
```

**Combined Generation and Validation**
```scala
// Pull both schema and validations from OpenMetadata
val config = configuration
  .enableGeneratePlanAndTasks(true)
  .enableGenerateValidations(true)

val openMetadataSource = metadataSource.openMetadata(
  "http://openmetadata-host:8585/api",
  OPEN_METADATA_AUTH_TYPE_OPEN_METADATA,
  Map(
    OPEN_METADATA_JWT_TOKEN -> "token",
    OPEN_METADATA_TABLE_FQN -> "database.schema.table"
  )
)

val task = postgres("my_postgres", "jdbc:...")
  .fields(openMetadataSource)  // Extract schema
  .count(count.records(1000))

execute(config, task)  // Validations automatically extracted
```

### 9. Configuration Options

```scala
val config = configuration
  // Folder paths configuration
  .generatedReportsFolderPath("/path/to/reports")
  .generatedPlanAndTaskFolderPath("/path/to/generated")
  .recordTrackingFolderPath("/tmp/data/generated/recordTracking")

  // Configuration flags
  .enableCount(true)                   // Enable count operations
  .enableGenerateData(true)            // Enable data generation
  .enableValidation(true)              // Enable data validation
  .enableGeneratePlanAndTasks(true)    // Auto-generate from metadata
  .enableGenerateValidations(true)     // Auto-generate validations from metadata
  .enableRecordTracking(true)          // Track generated records
  .enableDeleteGeneratedRecords(true)  // Enable cleanup mode
  .enableFailOnError(true)             // Fail on first error
  .enableUniqueCheck(true)             // Check unique constraints
  .enableSinkMetadata(false)           // Save metadata alongside data
  .enableSaveReports(true)             // Save HTML/JSON reports
  .enableAlerts(false)                 // Enable alerting (Slack, etc.)
  .enableUniqueCheckOnlyInBatch(false) // Check uniqueness within batch only
  .enableFastGeneration(true)          // Use fast SQL-based generation

  // Generation configuration
  .numRecordsFromDataSource(10000) // Sample size from data source (default: 10000)
  .numRecordsForAnalysis(10000) // Records to analyze (default: 10000)
  .oneOfDistinctCountVsCountThreshold(0.1) // Threshold for detecting oneOf (default: 0.1)
  .oneOfMinCount(1000) // Min count for oneOf values (default: 1000)
  .numGeneratedSamples(10) // Number of sample generations (default: 10)
  .numRecordsPerBatch(1000000) // Batch size for generation (default: 1000000)
  .uniqueBloomFilterNumItems(1000000) // Bloom filter capacity (default: 1000000)
  .uniqueBloomFilterFalsePositiveProbability(0.01) // Bloom filter FP rate (default: 0.01)

  // Validation configuration
  .numSampleErrorRecords(5) // Number of error sample records to retrieve and display in generated HTML report (default: 5)
  .enableDeleteRecordTrackingFiles(true) // After validations are complete, delete record tracking files that were used for validation purposes (enabled via enableRecordTracking) (default: true)

  // Alert configuration
  .enableAlerts(true)
  .slackAlertToken("xoxb-your-token")
  .slackAlertChannels(List("#data-quality", "#alerts"))
```

### 10. Advanced Features

**Fast Generation Mode**
```scala
// Enable fast generation for better performance with regex patterns
val config = configuration.enableFastGeneration(true)

// Supported patterns in fast mode (converted to pure SQL):
field.name("account_id").regex("ACC[0-9]{8}")              // Fast
field.name("product_code").regex("[A-Z]{3}-[0-9]{4}")      // Fast
field.name("status").regex("(ACTIVE|INACTIVE|PENDING)")    // Fast

// Unsupported patterns automatically fall back to UDF:
field.name("complex").regex("(?=lookahead)pattern")        // UDF fallback
```

**Reference Mode (Use Existing Data)**
```scala
// Don't generate data, just reference existing data for foreign keys
val existingData = json("existing", "/path/to/existing/data")
  .enableReferenceMode(true)
```

**Partitioning**
```scala
val task = csv("partitioned_csv", "/path")
  .partitionBy("year", "month")
  .numPartitions(10)
  .fields(...)
  .count(count.records(10000))
```

**Rate Limiting (for HTTP/Kafka)**
```scala
val httpTask = http("my_http")
  .rowsPerSecond(10)  // Limit to 10 requests per second
  .fields(...)
```

**Define Variables for Reuse**
```scala
val startDate = Date.valueOf("2022-01-01")
val accountIdField = field.name("account_id").regex("ACC[0-9]{8}")
val nameField = field.name("name").expression("#{Name.name}")

// Use in multiple tasks
val task1 = postgres("conn1", "jdbc:...")
  .fields(accountIdField, nameField)

val task2 = json("conn2", "/path")
  .fields(accountIdField, nameField)
```

### 11. Delete Generated Data

Clean up test data after use, including downstream data consumed by services/jobs.

**Enable Record Tracking** (during data generation):
```scala
val config = configuration
  .enableRecordTracking(true)
  .recordTrackingFolderPath("/tmp/record_tracking")
  .generatedReportsFolderPath("/tmp/reports")

execute(config, tasks...)
```

**Delete Generated Data**:
```scala
val deleteConfig = configuration
  .enableRecordTracking(true)
  .enableDeleteGeneratedRecords(true)
  .enableGenerateData(false)  // Disable generation
  .recordTrackingFolderPath("/tmp/record_tracking")

execute(deleteConfig, tasks...)
```

**Delete with Foreign Keys** - Deletes in reverse order:
```scala
val accounts = postgres("my_postgres", "jdbc:...")
  .table("public.accounts")
  .fields(field.name("account_id"))

val transactions = postgres(accounts)
  .table("public.transactions")
  .fields(field.name("account_id"))

val deletePlan = plan.addForeignKeyRelationship(
  accounts, "account_id",
  List(transactions -> "account_id")
)

val deleteConfig = configuration
  .enableRecordTracking(true)
  .enableDeleteGeneratedRecords(true)
  .enableGenerateData(false)

execute(deletePlan, deleteConfig, accounts, transactions)
```

**Delete Downstream Data** - Delete data consumed by services:
```scala
// Source: Postgres accounts table
val accounts = postgres("my_postgres", "jdbc:...")
  .table("public.accounts")
  .fields(field.name("account_id"))

// Downstream: Parquet files created by a service
val downstreamParquet = parquet("downstream", "/path/to/parquet")

// Define deletion relationship
val deletePlan = plan.addForeignKeyRelationship(
  accounts, "account_id",
  List(),  // No generation relationships
  List(downstreamParquet -> "account_id")  // Delete relationships
)

val deleteConfig = configuration
  .enableRecordTracking(true)
  .enableDeleteGeneratedRecords(true)
  .enableGenerateData(false)

execute(deletePlan, deleteConfig, accounts)
```

**Supported Data Sources for Deletion**:
- JDBC databases (Postgres, MySQL, etc.)
- Cassandra
- Files (CSV, JSON, Parquet, ORC, Delta)

**Key Points**:
- Record tracking must be enabled during generation
- Same `recordTrackingFolderPath` must be used for both generation and deletion
- Foreign keys determine deletion order (reverse of insertion)
- Only tracked records are deleted - manually added data is preserved
- Can have separate classes for generation vs deletion

## Common Use Cases

### Use Case 1: Generate Test Data for Database Tables with Relationships

```scala
import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants._

class DatabaseTestDataPlan extends PlanRun {

  val startDate = Date.valueOf("2024-01-01")

  // Customer table
  val customerTask = postgres("customers_db", "jdbc:postgresql://localhost:5432/mydb", "user", "pass")
    .table("public", "customers")
    .fields(
      field.name("customer_id").regex("CUST[0-9]{8}").primaryKey(true),
      field.name("first_name").expression("#{Name.firstName}"),
      field.name("last_name").expression("#{Name.lastName}"),
      field.name("email").expression("#{Internet.emailAddress}").unique(true),
      field.name("created_at").`type`(TimestampType)
    )
    .count(count.records(100))

  // Orders table
  val orderTask = postgres(customerTask)
    .table("public", "orders")
    .fields(
      field.name("order_id").regex("ORD[0-9]{10}").primaryKey(true),
      field.name("customer_id").regex("CUST[0-9]{8}"),
      field.name("order_date").`type`(DateType).min(startDate),
      field.name("total_amount").`type`(DecimalType(10, 2)).min(10).max(5000)
    )
    .count(count.recordsPerFieldGenerator(generator.min(1).max(10), "customer_id"))

  // Foreign key relationship
  val myPlan = plan.addForeignKeyRelationship(
    customerTask, "customer_id",
    List(orderTask -> "customer_id")
  )

  val config = configuration
    .generatedReportsFolderPath("/tmp/reports")
    .enableUniqueCheck(true)

  execute(myPlan, config, customerTask, orderTask)
}
```

### Use Case 2: Generate and Validate API Test Data

```scala
import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants._

class ApiTestDataPlan extends PlanRun {

  val httpTask = http("user_api", Map("baseUrl" -> "http://api.example.com"))
    .fields(
      field.httpHeader("Content-Type", "application/json"),
      field.httpHeader("Authorization", "Bearer token123"),
      field.httpUrl(
        "http://api.example.com/users",
        HttpMethodEnum.POST,
        List(),
        List()
      ),
      field.httpBody(
        field.name("username").expression("#{Internet.username}"),
        field.name("email").expression("#{Internet.emailAddress}"),
        field.name("age").`type`(IntegerType).min(18).max(80),
        field.name("preferences").fields(
          field.name("newsletter").`type`(BooleanType),
          field.name("notifications").`type`(BooleanType)
        )
      )
    )
    .validations(
      validation.field("response.statusCode").isEqual(201),
      validation.field("response.timeTaken").lessThan(500),
      validation.field("response.headers.Content-Type").matches("application/json.*")
    )
    .count(count.records(50))

  val config = configuration
    .generatedReportsFolderPath("/tmp/api-test-reports")
    .enableValidation(true)

  execute(config, httpTask)
}
```

### Use Case 3: Generate Kafka Messages with Complex Structure

```scala
import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants._

class KafkaTestDataPlan extends PlanRun {

  val kafkaTask = kafka("events_kafka", "localhost:9092")
    .topic("user-events")
    .fields(
      field.name("key").sql("content.user_id"),
      field.messageHeaders(
        field.messageHeader("event-type", "content.event_type"),
        field.messageHeader("timestamp", "content.timestamp")
      ),
      field.messageBody(
        field.name("user_id").regex("USER[0-9]{8}"),
        field.name("event_type").oneOf("login", "logout", "purchase", "view"),
        field.name("timestamp").`type`(TimestampType),
        field.name("metadata").fields(
          field.name("ip_address").expression("#{Internet.ipV4Address}"),
          field.name("user_agent").expression("#{Internet.userAgentAny}"),
          field.name("session_id").regex("[a-z0-9]{32}")
        ),
        field.name("data").fields(
          field.name("product_id").regex("PROD[0-9]{6}"),
          field.name("amount").`type`(DoubleType).min(1).max(1000)
        )
      )
    )
    .count(count.records(1000))

  val config = configuration
    .generatedReportsFolderPath("/tmp/kafka-reports")

  execute(config, kafkaTask)
}
```

### Use Case 4: Multi-Source Data Pipeline

```scala
import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants._

class MultiSourcePipelinePlan extends PlanRun {

  val accountIdField = field.name("account_id").regex("ACC[0-9]{8}")
  val nameField = field.name("name").expression("#{Name.name}")
  val startDate = Date.valueOf("2022-01-01")

  // Postgres account table
  val postgresAccount = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer", "user", "pass")
    .table("account", "accounts")
    .fields(
      accountIdField,
      nameField,
      field.name("open_date").`type`(DateType).min(startDate),
      field.name("status").oneOf("open", "closed", "pending")
    )
    .count(count.records(10))

  // Postgres transaction table
  val postgresTxn = postgres(postgresAccount)
    .table("transaction", "transactions")
    .fields(
      accountIdField,
      nameField,
      field.name("txn_date").`type`(DateType).min(startDate),
      field.name("amount").`type`(DoubleType).min(1).max(1000)
    )
    .count(count.recordsPerField(5, "account_id"))

  // JSON file with nested arrays
  val jsonTask = json("account_json", "/tmp/json/accounts")
    .fields(
      accountIdField,
      nameField,
      field.name("txn_list").`type`(ArrayType).fields(
        field.name("id").sql("_holding_txn_id"),
        field.name("date").`type`(DateType).min(startDate),
        field.name("amount").`type`(DoubleType)
      ),
      field.name("_holding_txn_id").omit(true)
    )
    .count(count.records(10))

  // Multi-column foreign keys across data sources
  val accountPlan = plan
    .name("Create accounts and transactions across Postgres and JSON")
    .addForeignKeyRelationship(
      foreignField("customer_postgres", "accounts", "account_id"),
      foreignField("customer_postgres", "transactions", "account_id"),
      foreignField("account_json", "account_info", "account_id")
    )
    .addForeignKeyRelationship(
      foreignField("customer_postgres", "accounts", "name"),
      foreignField("customer_postgres", "transactions", "name"),
      foreignField("account_json", "account_info", "name")
    )

  val config = configuration
    .generatedReportsFolderPath("/tmp/reports")
    .enableFastGeneration(true)

  execute(accountPlan, config, postgresAccount, postgresTxn, jsonTask)
}
```

### Use Case 5: Generate CSV with Complex Validations

```scala
import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants._

class CsvWithValidationPlan extends PlanRun {

  val csvTask = csv("transactions", "/tmp/test-data/transactions.csv", Map("header" -> "true"))
    .fields(
      field.name("transaction_id").regex("TXN[0-9]{12}").unique(true),
      field.name("account_id").regex("ACC[0-9]{8}"),
      field.name("transaction_date").`type`(DateType)
        .min(Date.valueOf("2024-01-01"))
        .max(Date.valueOf("2024-12-31")),
      field.name("amount").`type`(DecimalType(10, 2)).min(0.01).max(10000),
      field.name("transaction_type").oneOf("debit", "credit"),
      field.name("status").oneOf("pending", "completed", "failed"),
      field.name("merchant").expression("#{Company.name}")
    )
    .validations(
      validation.field("amount").greaterThan(0),
      validation.expr("YEAR(transaction_date) = 2024"),
      validation.groupBy("account_id").count().greaterThan(0),
      validation.groupBy("transaction_type").sum("amount").lessThan(1000000),
      validation.unique("transaction_id")
    )
    .count(count.records(10000))

  val config = configuration
    .generatedReportsFolderPath("/tmp/csv-reports")
    .enableValidation(true)
    .enableUniqueCheck(true)
    .enableFastGeneration(true)

  execute(config, csvTask)
}
```

### Use Case 6: Data Generation with Cleanup

```scala
import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants._

class DataGenerationWithCleanup extends PlanRun {

  val postgresTask = postgres("test_db", "jdbc:postgresql://localhost:5432/testdb", "user", "pass")
    .table("public", "test_data")
    .fields(
      field.name("id").regex("TEST[0-9]{8}").primaryKey(true),
      field.name("data").expression("#{Lorem.paragraph}")
    )
    .count(count.records(1000))

  val config = configuration
    .generatedReportsFolderPath("/tmp/cleanup-reports")
    .enableRecordTracking(true)          // Track records for cleanup
    .enableGenerateData(true)            // Generate data first
    .enableDeleteGeneratedRecords(false) // Don't delete yet

  execute(config, postgresTask)

  // Later, run cleanup:
  // val cleanupConfig = config.enableGenerateData(false).enableDeleteGeneratedRecords(true)
  // execute(cleanupConfig, postgresTask)
}
```

## Key Patterns to Remember

1. **Always extend `PlanRun`** to get access to builder methods
2. **Use backticks for `type`** method: `.`type`(IntegerType)` (Scala keyword)
3. **Chain methods** for fluent API: `.fields(...).count(...).validations(...)`
4. **Define foreign keys in plan** not in individual tasks
5. **Reuse connections** by passing existing task: `postgres(existingTask).table(...)`
6. **Use proper data types** from `org.apache.spark.sql.types`
7. **Enable fast generation** for large datasets with regex patterns
8. **Add validations** to ensure data quality
9. **Configure error thresholds** for flexible validation rules
10. **Use metadata sources** to auto-generate schemas from existing definitions

## Faker Expressions Reference

Common Faker expressions for realistic test data:

```scala
// Names and identity
field.name("first_name").expression("#{Name.firstName}")
field.name("last_name").expression("#{Name.lastName}")
field.name("full_name").expression("#{Name.name}")
field.name("title").expression("#{Name.title}")

// Contact information
field.name("email").expression("#{Internet.emailAddress}")
field.name("phone").expression("#{PhoneNumber.phoneNumber}")
field.name("mobile").expression("#{PhoneNumber.cellPhone}")

// Address
field.name("street").expression("#{Address.streetAddress}")
field.name("city").expression("#{Address.city}")
field.name("state").expression("#{Address.state}")
field.name("zip").expression("#{Address.zipCode}")
field.name("country").expression("#{Address.country}")

// Business
field.name("company").expression("#{Company.name}")
field.name("job_title").expression("#{Job.title}")
field.name("department").expression("#{Job.field}")

// Internet
field.name("username").expression("#{Internet.username}")
field.name("domain").expression("#{Internet.domainName}")
field.name("url").expression("#{Internet.url}")
field.name("ip_address").expression("#{Internet.ipV4Address}")

// Financial
field.name("credit_card").expression("#{Finance.creditCard}")
field.name("iban").expression("#{Finance.iban}")

// Commerce
field.name("product").expression("#{Commerce.productName}")
field.name("color").expression("#{Commerce.color}")
field.name("department").expression("#{Commerce.department}")

// Lorem text
field.name("description").expression("#{Lorem.paragraph}")
field.name("sentence").expression("#{Lorem.sentence}")
field.name("word").expression("#{Lorem.word}")
```

## Running Your Plan

**From IDE (IntelliJ/Eclipse)**
```scala
// Right-click on your class file and select "Run"
// Or use SBT/Gradle:
// ./gradlew :example:runScalaPlan
```

**From Command Line**
```bash
# Using Gradle
./gradlew :example:build
scala -cp "example/build/libs/*" io.github.datacatering.plan.MyDataPlan

# Using SBT
sbt "example/runMain io.github.datacatering.plan.MyDataPlan"
```

**Using Docker**
```bash
# Build Docker image
docker build -t my-data-plan .

# Run
docker run -v /path/to/data:/opt/app/data my-data-plan
```

## Troubleshooting

**Issue: Unique constraint violations**
```scala
// Enable unique check in configuration
val config = configuration.enableUniqueCheck(true)

// Mark fields as unique
field.name("email").expression("#{Internet.emailAddress}").unique(true)
```

**Issue: Foreign key relationships not working**
```scala
// Ensure field names match exactly
// Make sure to define relationships in plan, not tasks
val myPlan = plan.addForeignKeyRelationship(
  sourceTask, "id",  // Source field
  List(targetTask -> "source_id")  // Target field
)
```

**Issue: Slow data generation**
```scala
// Enable fast generation mode
val config = configuration.enableFastGeneration(true)

// Use simple regex patterns that can be converted to SQL
field.name("code").regex("[A-Z]{3}[0-9]{5}")  // Fast
// Avoid complex patterns that require UDF fallback
```

**Issue: Validation failures**
```scala
// Add error thresholds for flexibility
validation.field("amount").greaterThan(0).errorThreshold(0.05)  // Allow 5% errors

// Check validation reports in configured folder
val config = configuration.generatedReportsFolderPath("/tmp/reports")
```

**Issue: Type keyword conflicts**
```scala
// Use backticks for Scala keywords
field.name("amount").`type`(DoubleType)  // Correct
field.name("amount").type(DoubleType)     // Error: 'type' is a keyword
```

## See Also

- Main project documentation: `/docs`
- Example Scala plans: `/example/src/main/scala/io/github/datacatering/plan`
- Java API guide: `/misc/llm/JAVA_GUIDE.md`
- YAML configuration guide: `/misc/llm/YAML_GUIDE.md`
