# Data Caterer Java API Guide for Code Assistants

This guide helps code assistants understand how to use Data Caterer's Java API to help users generate test data, validate data quality, and manage test data lifecycle.

## Quick Start Pattern

Every Data Caterer Java implementation follows this structure:

```java
import io.github.datacatering.datacaterer.java.api.PlanRun;
import static io.github.datacatering.datacaterer.java.api.DataCatererConfigurationBuilder.*;

public class MyDataPlan extends PlanRun {
    {
        var task1 = <dataSource>("name", "connection-details")
            .fields(...)
            .count(...);

        var config = configuration()
            .generatedReportsFolderPath("/path/to/reports");

        var plan = plan().addForeignKeyRelationship(...);

        execute(plan, config, task1, task2, ...);
    }
}
```

## Core Building Blocks

### 1. Data Sources

**CSV Files**
```java
var csvTask = csv("my_csv", "/path/to/csv", Map.of("header", "true"))
    .fields(...)
    .count(count().records(100));
```

**JSON Files**
```java
var jsonTask = json("my_json", "/path/to/json")
    .fields(...)
    .count(count().records(100));
```

**PostgreSQL**
```java
var postgresTask = postgres("my_postgres", "jdbc:postgresql://localhost:5432/mydb", "user", "password")
    .table("schema_name", "table_name")
    .fields(...)
    .count(count().records(100));
```

**MySQL**
```java
var mysqlTask = mysql("my_mysql", "jdbc:mysql://localhost:3306/mydb", "user", "password")
    .table("schema_name", "table_name")
    .fields(...)
    .count(count().records(100));
```

**Cassandra**
```java
var cassandraTask = cassandra("my_cassandra", "localhost:9042", "user", "password")
    .table("keyspace", "table_name")
    .fields(
        field().name("id").regex("ID[0-9]{8}").primaryKey(true),
        field().name("created_at").type(TimestampType.instance()).clusteringPosition(1)
    )
    .count(count().records(100));
```

**Kafka**
```java
var kafkaTask = kafka("my_kafka", "localhost:9092")
    .topic("topic_name")
    .fields(
        field().name("key").sql("body.id"),
        field().messageHeaders(
            field().messageHeader("correlation-id", "body.id")
        ),
        field().messageBody(
            field().name("id").regex("ID[0-9]{8}"),
            field().name("data").fields(...)
        )
    )
    .count(count().records(100));
```

**HTTP/REST APIs**
```java
var httpTask = http("my_http", Map.of(Constants.ROWS_PER_SECOND(), "1"))
    .fields(
        field().httpHeader("Content-Type").staticValue("application/json"),
        field().httpUrl(
            "http://api.example.com/resource/{id}",
            HttpMethodEnum.POST(),
            List.of(field().name("id").regex("ID[0-9]{5}")),  // Path parameters
            List.of(field().name("limit").type(IntegerType.instance()).min(1).max(100))  // Query parameters
        ),
        field().httpBody(
            field().name("account_id").regex("ACC[0-9]{8}"),
            field().name("details").fields(
                field().name("name").expression("#{Name.name}"),
                field().name("amount").type(DoubleType.instance()).min(1.0).max(1000.0)
            )
        )
    )
    .validations(
        validation().field("response.statusCode").isEqual(200),
        validation().field("response.timeTaken").lessThan(100)
    );
```

### 2. Field Definitions

**Basic Field with Regex Pattern**
```java
field().name("account_id").regex("ACC[0-9]{8}")
```

**Field with Faker Expression**
```java
field().name("customer_name").expression("#{Name.name}")
field().name("email").expression("#{Internet.emailAddress}")
field().name("city").expression("#{Address.city}")
field().name("phone").expression("#{PhoneNumber.phoneNumber}")
```

**Field with SQL Expression**
```java
// Reference other fields
field().name("full_name").sql("CONCAT(first_name, ' ', last_name)")

// Conditional logic
field().name("status_code").sql("CASE WHEN status = 'active' THEN 1 ELSE 0 END")

// Date operations
field().name("expiry_date").sql("DATE_ADD(created_date, 365)")
```

**Field with Constraints**
```java
field().name("age")
    .type(IntegerType.instance())
    .min(18)
    .max(65)
```

**Field with Discrete Values**
```java
// Equal probability for each value
field().name("status").oneOf("active", "inactive", "pending", "suspended")

// Weighted distribution (requires multiple field definitions)
var statusFields = field().name("status").oneOfWeighted(
    Map.entry("active", 0.6),      // 60% active
    Map.entry("inactive", 0.25),   // 25% inactive
    Map.entry("pending", 0.1),     // 10% pending
    Map.entry("suspended", 0.05)   // 5% suspended
);
// Returns list of fields including hidden weight field
```

**Unique Fields**
```java
field().name("email").expression("#{Internet.emailAddress}").unique(true)
```

**Primary Key Fields**
```java
field().name("id").regex("ID[0-9]{8}").primaryKey(true)
```

**Hidden/Computed Fields**
```java
// Generate field but don't include in output
field().name("_temp_value").expression("#{Name.name}").omit(true)
field().name("display_name").sql("_temp_value")
```

### 3. Data Types

```java
// Numeric types
field().name("quantity").type(IntegerType.instance()).min(1).max(1000)
field().name("price").type(DoubleType.instance()).min(0.01).max(999.99)
field().name("total").type(new DecimalType(10, 2)).min(0).max(10000)

// String type (default)
field().name("description").type(StringType.instance())

// Date and time
field().name("created_date").type(DateType.instance()).min(java.sql.Date.valueOf("2020-01-01"))
field().name("updated_at").type(TimestampType.instance())

// Boolean
field().name("is_active").type(BooleanType.instance())

// Complex types
field().name("metadata").type(StructType.instance())
field().name("tags").type(ArrayType.instance())
```

### 4. Nested Structures

**Nested Objects (Structs)**
```java
field().name("customer_details")
    .type(StructType.instance())
    .fields(
        field().name("first_name").expression("#{Name.firstName}"),
        field().name("last_name").expression("#{Name.lastName}"),
        field().name("contact").fields(
            field().name("email").expression("#{Internet.emailAddress}"),
            field().name("phone").expression("#{PhoneNumber.phoneNumber}")
        )
    )
```

**Arrays**
```java
field().name("transaction_list")
    .type(ArrayType.instance())
    .fields(
        field().name("txn_id").regex("TXN[0-9]{10}"),
        field().name("amount").type(DoubleType.instance()).min(1).max(1000),
        field().name("date").type(DateType.instance())
    )
```

### 5. Record Count Strategies

**Fixed Number of Records**
```java
.count(count().records(1000))
```

**Records Per Field Value**
```java
// Generate 1-5 records for each distinct customer_id
.count(count()
    .recordsPerFieldGenerator(generator().min(1).max(5), "customer_id")
)

// Generate exactly 3 records per customer_id
.count(count().recordsPerField(3, "customer_id"))
```

**Records Per Multiple Fields**
```java
// Generate records per combination of customer_id and product_id
.count(count()
    .recordsPerField(10, "customer_id", "product_id")
)
```

### 6. Foreign Key Relationships

**Single Column Foreign Key**
```java
var plan = plan()
    .addForeignKeyRelationship(
        accountTask, List.of("account_id"),
        List.of(
            Map.entry(transactionTask, List.of("account_id")),
            Map.entry(balanceTask, List.of("account_id"))
        )
    );
```

**Multi-Column Foreign Key**
```java
var plan = plan()
    .addForeignKeyRelationship(
        customerTask, List.of("customer_id", "region"),
        List.of(
            Map.entry(orderTask, List.of("customer_id", "customer_region"))
        )
    );
```

**Multiple Foreign Key Relationships**
```java
var plan = plan()
    .addForeignKeyRelationship(
        customerTask, List.of("customer_id"),
        List.of(Map.entry(accountTask, List.of("customer_id")))
    )
    .addForeignKeyRelationship(
        accountTask, List.of("account_id"),
        List.of(
            Map.entry(transactionTask, List.of("account_id")),
            Map.entry(balanceTask, List.of("account_id"))
        )
    );
```

### 7. Data Validations

**Field-Level Validations**
```java
.validations(
    // Value checks
    validation().field("amount").greaterThan(0),
    validation().field("amount").lessThan(10000),
    validation().field("amount").between(1, 9999),

    // Pattern matching
    validation().field("email").matches("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"),

    // Null checks
    validation().field("required_field").isNull(false),

    // Set membership
    validation().field("status").in("active", "inactive", "pending"),

    // Uniqueness
    validation().unique("account_id"),
    validation().unique("customer_id", "email")
)
```

**Expression-Based Validations**
```java
.validations(
    // SQL expressions
    validation().expr("amount > 0 AND amount < balance"),
    validation().expr("YEAR(created_date) >= 2020"),
    validation().expr("LENGTH(description) > 10")
)
```

**Aggregation Validations**
```java
.validations(
    // Total count check
    validation().groupBy().count().isEqual(1000),

    // Group by single field
    validation().groupBy("customer_id").count().greaterThan(0),
    validation().groupBy("region").sum("amount").lessThan(1000000),

    // Group by multiple fields
    validation().groupBy("customer_id", "product_type")
        .avg("price").between(10.0, 100.0),

    // Min/max checks
    validation().groupBy("category").min("price").greaterThan(0),
    validation().groupBy("category").max("price").lessThan(10000)
)
```

**Cross-Dataset Validations**
```java
.validations(
    // Join validation - check matching records
    validation().upstreamData(sourceTask)
        .joinFields("account_id")
        .joinType("inner")
        .validations(
            validation().field("source_task.balance")
                .isEqualField("current_task.balance")
        ),

    // Anti-join validation - check non-matching records
    validation().upstreamData(sourceTask)
        .joinFields("account_id")
        .joinType("anti")
        .validations(validation().count().isEqual(0))
)
```

**Validation with Error Thresholds**
```java
.validations(
    // Allow up to 10% errors
    validation().field("amount").greaterThan(0).errorThreshold(0.1),

    // Allow up to 5 errors
    validation().field("status").in("active", "inactive").errorThreshold(5)
)
```

**Array Size Validations**
```java
.validations(
    validation().field("transaction_list").greaterThanSize(0),
    validation().field("tags").lessThanSize(10)
)
```

**Pre-Filter Validations (Subset Validation)**
```java
// Run validations only on a subset of data that matches pre-filter conditions
.validations(
    // Check that all closed accounts have zero balance
    validation().preFilter(fieldPreFilter("status").isEqual("closed"))
        .field("balance").isEqual(0),

    // Multiple pre-filter conditions with AND
    validation().preFilter(
        preFilterBuilder(fieldPreFilter("status").isEqual("active"))
            .and(fieldPreFilter("country").isEqual("US"))
    ).field("balance").greaterThan(0),

    // Multiple pre-filter conditions with OR
    validation().preFilter(
        preFilterBuilder(fieldPreFilter("type").isEqual("savings"))
            .or(fieldPreFilter("type").isEqual("checking"))
    ).field("interest_rate").greaterThan(0),

    // Pre-filter with simple field check
    validation().preFilter(fieldPreFilter("account_id").startsWith("ACC"))
        .field("amount").greaterThan(100)
)
```

### 8. Metadata Source Integration

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
```java
// From Marquez (OpenLineage metadata catalog)
var task = csv("my_csv", "/path/to/csv")
    .fields(metadataSource().marquez(
        "http://marquez-host:5001",
        "namespace",
        "dataset_name"
    ))
    .count(count().records(100));

// From OpenMetadata (with authentication)
var task = json("my_json", "/path")
    .fields(metadataSource().openMetadata(
        "http://openmetadata-host:8585/api",
        Constants.OPEN_METADATA_AUTH_TYPE_OPEN_METADATA,
        Map.of(
            Constants.OPEN_METADATA_JWT_TOKEN, "your_token",
            Constants.OPEN_METADATA_TABLE_FQN, "database.schema.table"
        )
    ))
    .count(count().records(100));

// Simplified OpenMetadata with token
var task = json("my_json", "/path")
    .fields(metadataSource().openMetadataWithToken(
        "http://openmetadata-host:8585/api",
        "your_jwt_token"
    ))
    .count(count().records(100));

// From OpenAPI/Swagger (for HTTP APIs)
var task = http("my_http")
    .fields(metadataSource().openApi("/path/to/openapi.yaml"))
    .count(count().records(100));

// From JSON Schema
var task = json("my_json", "/path")
    .fields(metadataSource().jsonSchema("/path/to/schema.json"))
    .count(count().records(100));

// From Open Data Contract Standard (ODCS)
var task = csv("my_csv", "/path")
    .fields(metadataSource().openDataContractStandard(
        "/path/to/data-contract.yaml",
        "dataset_name"
    ))
    .count(count().records(100));

// From Data Contract CLI
var task = postgres("my_postgres", "jdbc:...")
    .fields(metadataSource().dataContractCli("/path/to/contract.yaml"))
    .count(count().records(100));

// From Confluent Schema Registry
var task = kafka("my_kafka", "localhost:9092")
    .topic("my-topic")
    .fields(metadataSource().confluentSchemaRegistry(
        "http://schema-registry:8081",
        "my-subject-value"  // Subject name
    ))
    .count(count().records(100));

// From Confluent Schema Registry by ID
var task = kafka("my_kafka", "localhost:9092")
    .topic("my-topic")
    .fields(metadataSource().confluentSchemaRegistry(
        "http://schema-registry:8081",
        12345  // Schema ID
    ))
    .count(count().records(100));

// Reuse existing YAML configurations
var task = csv("my_csv", "/path")
    .fields(metadataSource().yamlTask(
        "/path/to/existing-task.yaml",
        "task_name",
        "step_name"
    ))
    .count(count().records(100));
```

**Extract Validations from Metadata**
```java
// From Great Expectations (validation rules only)
var task = postgres("my_postgres", "jdbc:...")
    .table("schema", "table")
    .validations(metadataSource().greatExpectations("/path/to/expectations.json"))
    .count(count().records(100));

// From OpenMetadata (includes test cases as validations)
var config = configuration()
    .enableGenerateValidations(true);  // Enable validation extraction

var task = postgres("my_postgres", "jdbc:...")
    .fields(metadataSource().openMetadataWithToken(
        "http://openmetadata-host:8585/api",
        "token"
    ))
    .count(count().records(100));

// Auto-generate tasks and validations from metadata
var config = configuration()
    .enableGeneratePlanAndTasks(true)      // Auto-generate tasks
    .enableGenerateValidations(true);      // Auto-generate validations

execute(config,
    postgres("my_postgres", "jdbc:postgresql://localhost/db", "user", "password")
        .fields(metadataSource().openDataContractStandard("/path/to/contract.yaml"))
);
```

**Combined Generation and Validation**
```java
// Pull both schema and validations from OpenMetadata
var config = configuration()
    .enableGeneratePlanAndTasks(true)
    .enableGenerateValidations(true);

var openMetadataSource = metadataSource().openMetadata(
    "http://openmetadata-host:8585/api",
    Constants.OPEN_METADATA_AUTH_TYPE_OPEN_METADATA,
    Map.of(
        Constants.OPEN_METADATA_JWT_TOKEN, "token",
        Constants.OPEN_METADATA_TABLE_FQN, "database.schema.table"
    )
);

var task = postgres("my_postgres", "jdbc:...")
    .fields(openMetadataSource)  // Extract schema
    .count(count().records(1000));

execute(config, task);  // Validations automatically extracted
```

### 9. Configuration Options

```java
var config = configuration()
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
    .enableFastGeneration(true);         // Use fast SQL-based generation

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
    .slackAlertChannels(List.of("#data-quality", "#alerts"))
```

### 10. Custom Transformations

Apply custom logic to generated files after they are written ("last mile" transformation). Useful for converting to custom formats, restructuring data, or applying business-specific transformations.

**Per-Record Transformation** - Transform each line/record individually:
```java
var task = csv("my_csv", "/path/to/csv")
    .fields(
        field().name("account_id"),
        field().name("name")
    )
    .count(count().records(100))
    .transformationPerRecord(
        "com.example.MyPerRecordTransformer",
        "transformRecord"  // Method name (default: "transformRecord")
    )
    .transformationOptions(Map.of(
        "prefix", "BANK_",
        "suffix", "_VERIFIED"
    ));
```

**Whole-File Transformation** - Transform entire file as a unit:
```java
var task = json("my_json", "/path/to/json")
    .fields(
        field().name("id"),
        field().name("data")
    )
    .count(count().records(200))
    .transformationWholeFile(
        "com.example.JsonArrayWrapperTransformer",
        "transformFile"  // Method name (default: "transformFile")
    )
    .transformationOptions(Map.of("minify", "true"));
```

**Task-Level Transformation** - Apply to all steps in a task:
```java
var myTask = task("my_task", "csv")
    .transformation("com.example.MyTransformer")  // Defaults to whole-file mode
    .transformationOptions(Map.of("format", "custom"));
```

**Custom Output Path** - Save transformed file to different location:
```java
var task = csv("my_csv", "/path/to/original")
    .fields(...)
    .transformationPerRecord("com.example.Transformer")
    .transformationOutput(
        "/path/to/transformed/output.csv",
        true  // Delete original file after transformation
    );
```

**Enable/Disable Transformation**:
```java
var task = json("my_json", "/path")
    .fields(...)
    .transformationWholeFile("com.example.Transformer")
    .enableTransformation(Boolean.parseBoolean(
        System.getenv().getOrDefault("ENABLE_TRANSFORM", "false")
    ));
```

**Implementing Transformers**:
```java
// Per-Record Transformer
public class MyPerRecordTransformer {
    public String transformRecord(String record, Map<String, String> options) {
        var prefix = options.getOrDefault("prefix", "");
        var suffix = options.getOrDefault("suffix", "");
        return prefix + record + suffix;
    }
}

// Whole-File Transformer
public class MyWholeFileTransformer {
    public void transformFile(String inputPath, String outputPath, Map<String, String> options) {
        var content = Files.readString(Path.of(inputPath));
        var transformed = // ... apply transformation ...
        Files.writeString(Path.of(outputPath), transformed);
    }
}
```

### 11. Advanced Features

**Fast Generation Mode**
```java
// Enable fast generation for better performance and less accurate data
var config = configuration().enableFastGeneration(true);

// Supported patterns in fast mode (converted to pure SQL):
field().name("account_id").regex("ACC[0-9]{8}")              // Fast
field().name("product_code").regex("[A-Z]{3}-[0-9]{4}")      // Fast
field().name("status").regex("(ACTIVE|INACTIVE|PENDING)")    // Fast

// Unsupported patterns automatically fall back to UDF:
field().name("complex").regex("(?=lookahead)pattern")        // UDF fallback
```

**Reference Mode (Use Existing Data)**
```java
// Don't generate data, just reference existing data for foreign keys
var existingData = json("existing", "/path/to/existing/data")
    .enableReferenceMode(true);
```

**Partitioning**
```java
var task = csv("partitioned_csv", "/path")
    .partitionBy("year", "month")
    .numPartitions(10)
    .fields(...)
    .count(count().records(10000));
```

### 12. Delete Generated Data

Clean up test data after use, including downstream data consumed by services/jobs.

**Enable Record Tracking** (during data generation):
```java
var config = configuration()
    .enableRecordTracking(true)
    .recordTrackingFolderPath("/tmp/record_tracking")
    .generatedReportsFolderPath("/tmp/reports");

execute(config, tasks...);
```

**Delete Generated Data**:
```java
var deleteConfig = configuration()
    .enableRecordTracking(true)
    .enableDeleteGeneratedRecords(true)
    .enableGenerateData(false)  // Disable generation
    .recordTrackingFolderPath("/tmp/record_tracking");

execute(deleteConfig, tasks...);
```

**Delete with Foreign Keys** - Deletes in reverse order:
```java
var accounts = postgres("my_postgres", "jdbc:...")
    .table("public", "accounts")
    .fields(field().name("account_id"));

var transactions = postgres(accounts)
    .table("public", "transactions")
    .fields(field().name("account_id"));

var deletePlan = plan().addForeignKeyRelationship(
    accounts, List.of("account_id"),
    List.of(Map.entry(transactions, List.of("account_id")))
);

var deleteConfig = configuration()
    .enableRecordTracking(true)
    .enableDeleteGeneratedRecords(true)
    .enableGenerateData(false);

execute(deletePlan, deleteConfig, accounts, transactions);
```

**Delete Downstream Data** - Delete data consumed by services:
```java
// Source: Postgres accounts table
var accounts = postgres("my_postgres", "jdbc:...")
    .table("public", "accounts")
    .fields(field().name("account_id"));

// Downstream: Parquet files created by a service
var downstreamParquet = parquet("downstream", "/path/to/parquet");

// Define deletion relationship
var deletePlan = plan().addForeignKeyRelationship(
    accounts, List.of("account_id"),
    List.of(),  // No generation relationships
    List.of(Map.entry(downstreamParquet, List.of("account_id")))  // Delete relationships
);

var deleteConfig = configuration()
    .enableRecordTracking(true)
    .enableDeleteGeneratedRecords(true)
    .enableGenerateData(false);

execute(deletePlan, deleteConfig, accounts);
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

```java
public class DatabaseTestDataPlan extends PlanRun {
    {
        // Customer table
        var customerTask = postgres("customers_db", "jdbc:postgresql://localhost:5432/mydb", "user", "pass")
            .table("public", "customers")
            .fields(
                field().name("customer_id").regex("CUST[0-9]{8}").primaryKey(true),
                field().name("first_name").expression("#{Name.firstName}"),
                field().name("last_name").expression("#{Name.lastName}"),
                field().name("email").expression("#{Internet.emailAddress}").unique(true),
                field().name("created_at").type(TimestampType.instance())
            )
            .count(count().records(100));

        // Orders table
        var orderTask = postgres(customerTask)
            .table("public", "orders")
            .fields(
                field().name("order_id").regex("ORD[0-9]{10}").primaryKey(true),
                field().name("customer_id").regex("CUST[0-9]{8}"),
                field().name("order_date").type(DateType.instance())
                    .min(java.sql.Date.valueOf("2024-01-01")),
                field().name("total_amount").type(new DecimalType(10, 2)).min(10).max(5000)
            )
            .count(count().recordsPerFieldGenerator(generator().min(1).max(10), "customer_id"));

        // Foreign key relationship
        var plan = plan().addForeignKeyRelationship(
            customerTask, List.of("customer_id"),
            List.of(Map.entry(orderTask, List.of("customer_id")))
        );

        var config = configuration()
            .generatedReportsFolderPath("/tmp/reports")
            .enableUniqueCheck(true);

        execute(plan, config, customerTask, orderTask);
    }
}
```

### Use Case 2: Generate and Validate API Test Data

```java
public class ApiTestDataPlan extends PlanRun {
    {
        var httpTask = http("user_api", Map.of("baseUrl", "http://api.example.com"))
            .fields(
                field().httpHeader("Content-Type").staticValue("application/json"),
                field().httpHeader("Authorization").staticValue("Bearer token123"),
                field().httpUrl(
                    "http://api.example.com/users",
                    HttpMethodEnum.POST(),
                    List.of(),
                    List.of()
                ),
                field().httpBody(
                    field().name("username").expression("#{Internet.username}"),
                    field().name("email").expression("#{Internet.emailAddress}"),
                    field().name("age").type(IntegerType.instance()).min(18).max(80),
                    field().name("preferences").fields(
                        field().name("newsletter").type(BooleanType.instance()),
                        field().name("notifications").type(BooleanType.instance())
                    )
                )
            )
            .validations(
                validation().field("response.statusCode").isEqual(201),
                validation().field("response.timeTaken").lessThan(500),
                validation().field("response.headers.Content-Type").matches("application/json.*")
            )
            .count(count().records(50));

        var config = configuration()
            .generatedReportsFolderPath("/tmp/api-test-reports")
            .enableValidation(true);

        execute(config, httpTask);
    }
}
```

### Use Case 3: Generate Kafka Messages with Complex Structure

```java
public class KafkaTestDataPlan extends PlanRun {
    {
        var kafkaTask = kafka("events_kafka", "localhost:9092")
            .topic("user-events")
            .fields(
                field().name("key").sql("content.user_id"),
                field().messageHeaders(
                    field().messageHeader("event-type", "content.event_type"),
                    field().messageHeader("timestamp", "content.timestamp")
                ),
                field().messageBody(
                    field().name("user_id").regex("USER[0-9]{8}"),
                    field().name("event_type").oneOf("login", "logout", "purchase", "view"),
                    field().name("timestamp").type(TimestampType.instance()),
                    field().name("metadata").fields(
                        field().name("ip_address").expression("#{Internet.ipV4Address}"),
                        field().name("user_agent").expression("#{Internet.userAgentAny}"),
                        field().name("session_id").regex("[a-z0-9]{32}")
                    ),
                    field().name("data").fields(
                        field().name("product_id").regex("PROD[0-9]{6}"),
                        field().name("amount").type(DoubleType.instance()).min(1).max(1000)
                    )
                )
            )
            .count(count().records(1000));

        var config = configuration()
            .generatedReportsFolderPath("/tmp/kafka-reports");

        execute(config, kafkaTask);
    }
}
```

### Use Case 4: Generate CSV Files with Validation

```java
public class CsvWithValidationPlan extends PlanRun {
    {
        var csvTask = csv("transactions", "/tmp/test-data/transactions.csv", Map.of("header", "true"))
            .fields(
                field().name("transaction_id").regex("TXN[0-9]{12}").unique(true),
                field().name("account_id").regex("ACC[0-9]{8}"),
                field().name("transaction_date").type(DateType.instance())
                    .min(java.sql.Date.valueOf("2024-01-01"))
                    .max(java.sql.Date.valueOf("2024-12-31")),
                field().name("amount").type(new DecimalType(10, 2)).min(0.01).max(10000),
                field().name("transaction_type").oneOf("debit", "credit"),
                field().name("status").oneOf("pending", "completed", "failed"),
                field().name("merchant").expression("#{Company.name}")
            )
            .validations(
                validation().field("amount").greaterThan(0),
                validation().field("transaction_date").expr("YEAR(transaction_date) = 2024"),
                validation().groupBy("account_id").count().greaterThan(0),
                validation().groupBy("transaction_type")
                    .sum("amount").lessThan(1000000),
                validation().unique("transaction_id")
            )
            .count(count().records(10000));

        var config = configuration()
            .generatedReportsFolderPath("/tmp/csv-reports")
            .enableValidation(true)
            .enableUniqueCheck(true)
            .enableFastGeneration(true);

        execute(config, csvTask);
    }
}
```

### Use Case 5: Multi-Source Data Pipeline with Metadata

```java
public class MultiSourcePipelinePlan extends PlanRun {
    {
        // Generate source data in CSV
        var csvSource = csv("source_csv", "/tmp/source/customers.csv", Map.of("header", "true"))
            .fields(
                field().name("id").regex("ID[0-9]{8}").unique(true),
                field().name("name").expression("#{Name.name}"),
                field().name("email").expression("#{Internet.emailAddress}"),
                field().name("country").expression("#{Address.country}")
            )
            .count(count().records(1000));

        // Generate data into PostgreSQL
        var postgresTask = postgres("target_db", "jdbc:postgresql://localhost:5432/analytics", "user", "pass")
            .table("staging", "customers")
            .fields(
                field().name("customer_id"),
                field().name("full_name"),
                field().name("email_address"),
                field().name("country_code")
            )
            .count(count().records(1000));

        // Generate data into JSON
        var jsonTask = json("enriched_json", "/tmp/target/enriched.json")
            .fields(
                field().name("id"),
                field().name("name"),
                field().name("email"),
                field().name("metadata").fields(
                    field().name("country"),
                    field().name("signup_date").type(DateType.instance()),
                    field().name("status").oneOf("active", "inactive")
                )
            )
            .count(count().records(1000));

        var plan = plan()
            .addForeignKeyRelationship(
                csvSource, List.of("id"),
                List.of(
                    Map.entry(postgresTask, List.of("customer_id")),
                    Map.entry(jsonTask, List.of("id"))
                )
            );

        var config = configuration()
            .generatedReportsFolderPath("/tmp/pipeline-reports")
            .enableFastGeneration(true);

        execute(plan, config, csvSource, postgresTask, jsonTask);
    }
}
```

## Key Patterns to Remember

1. **Always extend `PlanRun`** and use instance initialization block `{}`
2. **Use static imports** for cleaner syntax: `import static io.github.datacatering.datacaterer.java.api.DataCatererConfigurationBuilder.*;`
3. **Chain methods** for fluent API: `.fields(...).count(...).validations(...)`
4. **Define foreign keys in plan** not in individual tasks
5. **Reuse connections** by passing existing task: `postgres(existingTask).table(...)`
6. **Use proper data types** with `Type.instance()` methods
7. **Enable fast generation** for large datasets with regex patterns
8. **Add validations** to ensure data quality
9. **Configure error thresholds** for flexible validation rules
10. **Use metadata sources** to auto-generate schemas from existing definitions

## Faker Expressions Reference

Common Faker expressions for realistic test data:

```java
// Names and identity
field().name("first_name").expression("#{Name.firstName}")
field().name("last_name").expression("#{Name.lastName}")
field().name("full_name").expression("#{Name.name}")
field().name("title").expression("#{Name.title}")

// Contact information
field().name("email").expression("#{Internet.emailAddress}")
field().name("phone").expression("#{PhoneNumber.phoneNumber}")
field().name("mobile").expression("#{PhoneNumber.cellPhone}")

// Address
field().name("street").expression("#{Address.streetAddress}")
field().name("city").expression("#{Address.city}")
field().name("state").expression("#{Address.state}")
field().name("zip").expression("#{Address.zipCode}")
field().name("country").expression("#{Address.country}")

// Business
field().name("company").expression("#{Company.name}")
field().name("job_title").expression("#{Job.title}")
field().name("department").expression("#{Job.field}")

// Internet
field().name("username").expression("#{Internet.username}")
field().name("domain").expression("#{Internet.domainName}")
field().name("url").expression("#{Internet.url}")
field().name("ip_address").expression("#{Internet.ipV4Address}")

// Financial
field().name("credit_card").expression("#{Finance.creditCard}")
field().name("iban").expression("#{Finance.iban}")

// Commerce
field().name("product").expression("#{Commerce.productName}")
field().name("color").expression("#{Commerce.color}")
field().name("department").expression("#{Commerce.department}")

// Lorem text
field().name("description").expression("#{Lorem.paragraph}")
field().name("sentence").expression("#{Lorem.sentence}")
field().name("word").expression("#{Lorem.word}")
```

## Running Your Plan

**From IDE (IntelliJ/Eclipse)**
```java
// Right-click on your class file and select "Run"
// Or use Gradle:
// ./gradlew :example:runJavaPlan
```

**From Command Line**
```bash
# Compile and run
./gradlew :example:build
java -cp "example/build/libs/*" io.github.datacatering.plan.MyDataPlan
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
```java
// Enable unique check in configuration
var config = configuration().enableUniqueCheck(true);

// Mark fields as unique
field().name("email").expression("#{Internet.emailAddress}").unique(true)
```

**Issue: Foreign key relationships not working**
```java
// Ensure field names match exactly
// Make sure to define relationships in plan, not tasks
var plan = plan().addForeignKeyRelationship(
    sourceTask, List.of("id"),  // Source field
    List.of(Map.entry(targetTask, List.of("source_id")))  // Target field
);
```

**Issue: Slow data generation**
```java
// Enable fast generation mode
var config = configuration().enableFastGeneration(true);

// Use simple regex patterns that can be converted to SQL
field().name("code").regex("[A-Z]{3}[0-9]{5}")  // Fast
// Avoid complex patterns that require UDF fallback
```

**Issue: Validation failures**
```java
// Add error thresholds for flexibility
validation().field("amount").greaterThan(0).errorThreshold(0.05)  // Allow 5% errors

// Check validation reports in configured folder
var config = configuration().generatedReportsFolderPath("/tmp/reports");
```

## See Also

- Main project documentation: `/docs`
- Example Java plans: `/example/src/main/java/io/github/datacatering/plan`
- Scala API guide: `/misc/llm/SCALA_GUIDE.md`
- YAML configuration guide: `/misc/llm/YAML_GUIDE.md`
