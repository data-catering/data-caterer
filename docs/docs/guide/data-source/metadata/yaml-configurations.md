---
title: "YAML Configuration Files"
description: "Use existing YAML configuration files as metadata sources within the Java/Scala API using YamlBuilder."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# YAML Configuration Files

Data Caterer allows you to reference and extend existing YAML configuration files within your Java/Scala API code using the YamlBuilder. This hybrid approach enables you to maintain configuration in YAML files while leveraging the full power of the programmatic API for customization.

## Overview

Data Caterer provides two powerful ways to work with YAML configurations:

1. **YamlBuilder API**: Load YAML configurations into Java/Scala code for programmatic customization
2. **YAML Metadata Source**: Reference YAML configurations from within other YAML files

The YamlBuilder provides a bridge between YAML configurations and programmatic APIs, allowing you to:

- **Load base configurations** from YAML files
- **Override specific settings** programmatically 
- **Environment-specific customizations** without modifying YAML files
- **Filter tasks and steps** by name when loading from multi-task YAML files
- **Maintain separation** between configuration and business logic

The YAML Metadata Source enables YAML-to-YAML composition, allowing you to:

- **Reference step configurations** from other YAML task files
- **Create reusable step libraries** that can be imported across tasks
- **Compose complex tasks** from smaller, modular YAML components
- **Maintain DRY principles** within YAML configurations

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/scala/io/github/datacatering/plan/YamlReferenceExamplePlan.scala) | [:material-coffee: Java Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/java/io/github/datacatering/plan/YamlReferenceExampleJavaPlan.java)

## Requirements

- YAML plan or task files defined according to Data Caterer schema
- Java 17+ or Scala 2.12+
- Data Caterer 0.17.0+

## YAML Metadata Source (Programmatic References)

The YAML metadata source allows you to reference existing YAML task and plan files from within your **Java/Scala code**. This provides a way to reuse field definitions and configurations from YAML files while still having the flexibility of programmatic customization.

### Basic Usage

**Existing Unified YAML File (customer-fields.yaml):**
```yaml
name: "customer_data"

dataSources:
  - name: "customer_data"
    connection:
      type: "csv"
      options:
        path: "/opt/app/data/customers.csv"
    steps:
      - name: "customers"
        fields:
          - name: "customer_id"
            type: "string"
            options:
              regex: "CUST[0-9]{8}"
              isUnique: "true"
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
```

**Programmatic Usage:**

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun
    
    class CustomerDataPlan extends PlanRun {
      // Reference YAML field definitions in a different data source
      val postgresTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
        .table("customers")
        .fields(metadataSource.yamlTask(
          "config/customer-fields.yaml",
          "customer_data",
          "customers"
        ))
        .count(count.records(10000))  // Override record count
      
      execute(postgresTask)
    }
    ```

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.javaapi.api.PlanRun;
    
    public class CustomerDataPlan extends PlanRun {
        {
            // Reference YAML field definitions in a different data source
            var postgresTask = postgres("customer_postgres", "jdbc:postgresql://localhost:5432/customer")
                .table("customers")
                .fields(metadataSource().yamlTask(
                    "config/customer-fields.yaml",
                    "customer_data", 
                    "customers"
                ))
                .count(count().records(10000));  // Override record count
            
            execute(postgresTask);
        }
    }
    ```

### Referencing Multiple YAML Sources

**Account Fields YAML (account-fields.yaml):**
```yaml
name: "account_data"

dataSources:
  - name: "account_data"
    connection:
      type: "json"
      options: {}
    steps:
      - name: "checking_accounts"
        options:
          path: "/opt/app/data/checking.json"
        fields:
          - name: "account_id"
            type: "string"
            options:
              regex: "CHK[0-9]{10}"
              isUnique: "true"
          - name: "balance"
            type: "double"
            options:
              min: 0.0
              max: 50000.0

      - name: "savings_accounts"
        options:
          path: "/opt/app/data/savings.json"
        fields:
          - name: "account_id"
            type: "string"
            options:
              regex: "SAV[0-9]{10}"
              isUnique: "true"
          - name: "balance"
            type: "double"
            options:
              min: 100.0
              max: 1000000.0
          - name: "interest_rate"
            type: "double"
            options:
              min: 0.01
              max: 0.05
```

**Creating Multiple Tasks from Same YAML:**

=== "Scala"

    ```scala
    class BankingDataPlan extends PlanRun {
      // Use the same YAML source for different output formats
      val csvCheckingAccounts = csv("csv_checking", "/tmp/checking.csv")
        .fields(metadataSource.yamlTask(
          "config/account-fields.yaml",
          "account_data",
          "checking_accounts"
        ))
        .count(count.records(5000))
      
      val parquetSavingsAccounts = parquet("parquet_savings", "/tmp/savings.parquet")  
        .fields(metadataSource.yamlTask(
          "config/account-fields.yaml",
          "account_data", 
          "savings_accounts"
        ))
        .count(count.records(2000))
      
      execute(plan.tasks(csvCheckingAccounts, parquetSavingsAccounts))
    }
    ```

=== "Java"

    ```java
    public class BankingDataPlan extends PlanRun {
        {
            // Use the same YAML source for different output formats  
            var csvCheckingAccounts = csv("csv_checking", "/tmp/checking.csv")
                .fields(metadataSource().yamlTask(
                    "config/account-fields.yaml",
                    "account_data",
                    "checking_accounts"
                ))
                .count(count().records(5000));
            
            var parquetSavingsAccounts = parquet("parquet_savings", "/tmp/savings.parquet")
                .fields(metadataSource().yamlTask(
                    "config/account-fields.yaml", 
                    "account_data",
                    "savings_accounts"
                ))
                .count(count().records(2000));
            
            execute(plan().tasks(csvCheckingAccounts, parquetSavingsAccounts));
        }
    }
    ```

### Field Override Patterns

You can reference YAML field definitions and then override or add specific fields:

=== "Scala"

    ```scala
    class CustomizedDataPlan extends PlanRun {
      val customTask = json("custom_customers", "/tmp/custom-customers.json")
        .fields(metadataSource.yamlTask(
          "config/customer-fields.yaml",
          "customer_data",
          "customers"
        ))
        // Override specific fields from YAML
        .fields(
          field.name("customer_id").regex("CUSTOM[0-9]{6}"),  // Different ID pattern
          field.name("created_at").`type`(TimestampType).min(Timestamp.valueOf("2024-01-01 00:00:00")),  // Add new field
          field.name("tier").oneOf("bronze", "silver", "gold", "platinum")  // Add customer tier
        )
        .count(count.records(25000))
      
      execute(customTask)
    }
    ```

=== "Java"

    ```java
    public class CustomizedDataPlan extends PlanRun {
        {
            var customTask = json("custom_customers", "/tmp/custom-customers.json")
                .fields(metadataSource().yamlTask(
                    "config/customer-fields.yaml",
                    "customer_data", 
                    "customers"
                ))
                // Override specific fields from YAML
                .fields(
                    field().name("customer_id").regex("CUSTOM[0-9]{6}"),  // Different ID pattern
                    field().name("created_at").type(TimestampType.instance()).min(Timestamp.valueOf("2024-01-01 00:00:00")),  // Add new field
                    field().name("tier").oneOf("bronze", "silver", "gold", "platinum")  // Add customer tier
                )
                .count(count().records(25000));
            
            execute(customTask);
        }
    }
    ```

### Environment-Specific YAML Usage

=== "Scala"

    ```scala
    class EnvironmentSpecificPlan extends PlanRun {
      val env = sys.env.getOrElse("ENVIRONMENT", "dev")
      val yamlFile = s"config/${env}-customer-config.yaml"
      
      val environmentTask = postgres(s"${env}_postgres", getConnectionUrl(env))
        .table("customers")
        .fields(metadataSource.yamlTask(yamlFile, "customer_data"))
        .count(getRecordCount(env))
      
      private def getConnectionUrl(env: String): String = env match {
        case "prod" => "jdbc:postgresql://prod-db:5432/customer"
        case "staging" => "jdbc:postgresql://staging-db:5432/customer"
        case _ => "jdbc:postgresql://dev-db:5432/customer"
      }
      
      private def getRecordCount(env: String) = env match {
        case "prod" => count.records(1000000)
        case "staging" => count.records(100000)
        case _ => count.records(1000)
      }
      
      execute(environmentTask)
    }
    ```

=== "Java"

    ```java
    public class EnvironmentSpecificPlan extends PlanRun {
        {
            var env = System.getenv().getOrDefault("ENVIRONMENT", "dev");
            var yamlFile = "config/" + env + "-customer-config.yaml";
            
            var environmentTask = postgres(env + "_postgres", getConnectionUrl(env))
                .table("customers")
                .fields(metadataSource().yamlTask(yamlFile, "customer_data"))
                .count(getRecordCount(env));
            
            execute(environmentTask);
        }
        
        private String getConnectionUrl(String env) {
            return switch (env) {
                case "prod" -> "jdbc:postgresql://prod-db:5432/customer";
                case "staging" -> "jdbc:postgresql://staging-db:5432/customer";
                default -> "jdbc:postgresql://dev-db:5432/customer";
            };
        }
        
        private CountBuilder getRecordCount(String env) {
            return switch (env) {
                case "prod" -> count().records(1000000);
                case "staging" -> count().records(100000);
                default -> count().records(1000);
            };
        }
    }
    ```

## YamlBuilder API (YAML-to-Code Integration)

Beyond YAML-to-YAML references, you can also load YAML configurations into your Java/Scala code for programmatic customization.

## Basic Usage

### Loading a Complete YAML Plan

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.YamlBuilder
    
    class MyPlanRun extends PlanRun {
      val myPlan = YamlBuilder()
        .plan("src/main/resources/plans/customer-data-plan.yaml")
        .name("Production Customer Data Generation")  // Override plan name
        .description("Generated for production environment")
        .execute()
    }
    ```

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.javaapi.api.YamlBuilder;
    
    public class MyPlanRun extends PlanRun {
        {
            var myPlan = new YamlBuilder()
                .plan("src/main/resources/plans/customer-data-plan.yaml")
                .name("Production Customer Data Generation")  // Override plan name
                .description("Generated for production environment")
                .execute();
        }
    }
    ```

### Loading Individual YAML Tasks

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.YamlBuilder

    class MyPlanRun extends PlanRun {
      // Load base task configuration from YAML
      val customerTask = YamlBuilder()
        .taskByFile("src/main/resources/tasks/customer-base-task.yaml")
        .count(count.records(100000))  // Override for production scale
        .option("saveMode", "append")   // Environment-specific option

      val myPlan = plan
        .name("Production Data Generation")
        .tasks(customerTask)
        .execute()
    }
    ```

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.javaapi.api.YamlBuilder;

    public class MyPlanRun extends PlanRun {
        {
            // Load base task configuration from YAML
            var customerTask = new YamlBuilder()
                .taskByFile("src/main/resources/tasks/customer-base-task.yaml")
                .count(count().records(100000))  // Override for production scale
                .option("saveMode", "append");    // Environment-specific option

            var myPlan = plan()
                .name("Production Data Generation")
                .tasks(customerTask)
                .execute();
        }
    }
    ```

### Filtering Tasks by Name

When working with YAML files containing multiple tasks, you can load specific tasks by name:

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.YamlBuilder

    class MyPlanRun extends PlanRun {
      // Load only the "customer_accounts" task from multi-task YAML file
      val accountTask = YamlBuilder()
        .taskByName("customer_accounts")
        .count(count.records(50000))
        .fields(
          field.name("balance").min(1000).max(100000),  // Override field constraints
          field.name("status").oneOf("active", "suspended")  // Add new field
        )

      val myPlan = plan
        .name("Account Data Generation")
        .tasks(accountTask)
        .execute()
    }
    ```

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.javaapi.api.YamlBuilder;

    public class MyPlanRun extends PlanRun {
        {
            // Load only the "customer_accounts" task from multi-task YAML file
            var accountTask = new YamlBuilder()
                .taskByName("customer_accounts")
                .count(count().records(50000))
                .fields(
                    field().name("balance").min(1000).max(100000),  // Override field constraints
                    field().name("status").oneOf("active", "suspended")  // Add new field
                );

            var myPlan = plan()
                .name("Account Data Generation")
                .tasks(accountTask)
                .execute();
        }
    }
    ```

### Filtering Steps by Name

For fine-grained control, you can also filter by specific step names within tasks:

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.YamlBuilder

    class MyPlanRun extends PlanRun {
      // Load specific step from task
      val specificStep = YamlBuilder()
        .stepByFileAndName("config/postgres-tasks.yaml", "customer_task", "customer_profiles")
        .count(count.records(25000))  // Environment-specific record count
        .option("batchSize", "5000")   // Performance tuning for environment

      val myPlan = plan
        .name("Profile Data Generation")
        .tasks(specificStep)
        .execute()
    }
    ```

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.javaapi.api.YamlBuilder;

    public class MyPlanRun extends PlanRun {
        {
            // Load specific step from task
            var specificStep = new YamlBuilder()
                .stepByFileAndName("config/postgres-tasks.yaml", "customer_task", "customer_profiles")
                .count(count().records(25000))  // Environment-specific record count
                .option("batchSize", "5000");    // Performance tuning for environment

            var myPlan = plan()
                .name("Profile Data Generation")
                .tasks(specificStep)
                .execute();
        }
    }
    ```

## Advanced Usage Patterns

### Environment-Specific Configurations

Create different configurations for different environments while reusing the same base YAML:

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.YamlBuilder

    class MyPlanRun extends PlanRun {
      // Base configuration in YAML
      val baseCustomerTask = YamlBuilder().taskByFile("config/customer-base-task.yaml")
      
      // Environment-specific customizations
      val devTask = baseCustomerTask
        .count(count.records(1000))      // Small dataset for dev
        .option("url", "jdbc:postgresql://dev-db:5432/testdb")
      
      val stagingTask = baseCustomerTask
        .count(count.records(10000))     // Medium dataset for staging
        .option("url", "jdbc:postgresql://staging-db:5432/stagingdb")
        .enableUniqueCheck(true)         // Enable validation in staging
      
      val prodTask = baseCustomerTask
        .count(count.records(1000000))   // Large dataset for production
        .option("url", "jdbc:postgresql://prod-db:5432/proddb")
        .option("batchSize", "10000")    // Production optimization
        .enableUniqueCheck(true)
      
      // Select task based on environment
      val environmentTask = sys.env.get("ENVIRONMENT") match {
        case Some("dev") => devTask
        case Some("staging") => stagingTask
        case _ => prodTask
      }
      
      val myPlan = plan
        .name(s"Data Generation - ${sys.env.getOrElse("ENVIRONMENT", "prod")}")
        .tasks(environmentTask)
        .execute()
    }
    ```

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.javaapi.api.YamlBuilder;

    public class MyPlanRun extends PlanRun {
        {
            // Base configuration in YAML
            var baseCustomerTask = new YamlBuilder().taskByFile("config/customer-base-task.yaml");
            
            // Environment-specific customizations
            var devTask = baseCustomerTask
                .count(count().records(1000))      // Small dataset for dev
                .option("url", "jdbc:postgresql://dev-db:5432/testdb");
            
            var stagingTask = baseCustomerTask
                .count(count().records(10000))     // Medium dataset for staging
                .option("url", "jdbc:postgresql://staging-db:5432/stagingdb")
                .enableUniqueCheck(true);          // Enable validation in staging
            
            var prodTask = baseCustomerTask
                .count(count().records(1000000))   // Large dataset for production
                .option("url", "jdbc:postgresql://prod-db:5432/proddb")
                .option("batchSize", "10000")      // Production optimization
                .enableUniqueCheck(true);
            
            // Select task based on environment
            var environment = System.getenv().getOrDefault("ENVIRONMENT", "prod");
            var environmentTask = switch (environment) {
                case "dev" -> devTask;
                case "staging" -> stagingTask;
                default -> prodTask;
            };
            
            var myPlan = plan()
                .name("Data Generation - " + environment)
                .tasks(environmentTask)
                .execute();
        }
    }
    ```

### Mixed YAML and Programmatic Tasks

Combine YAML-loaded tasks with purely programmatic tasks:

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.YamlBuilder

    class MyPlanRun extends PlanRun {
      // Load some tasks from YAML
      val customerTask = YamlBuilder()
        .taskByFile("config/customer-task.yaml")
        .count(count.records(50000))

      val accountTask = YamlBuilder()
        .taskByName("savings_accounts")
        .count(count.recordsPerField("customer_id", 1, 3))
      
      // Define other tasks programmatically  
      val transactionTask = postgres("transactions", "jdbc:postgresql://localhost:5432/bank")
        .table("transactions")
        .count(count.recordsPerField("account_id", 10, 100))
        .fields(
          field.name("transaction_id").regex("TXN[0-9]{10}").unique(true),
          field.name("amount").`type`(DoubleType).min(1.0).max(10000.0),
          field.name("transaction_date").`type`(DateType).min(Date.valueOf("2023-01-01"))
        )
      
      val myPlan = plan
        .name("Complete Banking Data Generation")
        .tasks(customerTask, accountTask, transactionTask)
        .execute()
    }
    ```

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.javaapi.api.YamlBuilder;

    public class MyPlanRun extends PlanRun {
        {
            // Load some tasks from YAML
            var customerTask = new YamlBuilder()
                .taskByFile("config/customer-task.yaml")
                .count(count().records(50000));

            var accountTask = new YamlBuilder()
                .taskByName("savings_accounts")
                .count(count().recordsPerField("customer_id", 1, 3));
            
            // Define other tasks programmatically
            var transactionTask = postgres("transactions", "jdbc:postgresql://localhost:5432/bank")
                .table("transactions")
                .count(count().recordsPerField("account_id", 10, 100))
                .fields(
                    field().name("transaction_id").regex("TXN[0-9]{10}").isUnique(true),
                    field().name("amount").type(DoubleType.instance()).min(1.0).max(10000.0),
                    field().name("transaction_date").type(DateType.instance()).min(Date.valueOf("2023-01-01"))
                );
            
            var myPlan = plan()
                .name("Complete Banking Data Generation")
                .tasks(customerTask, accountTask, transactionTask)
                .execute();
        }
    }
    ```

### Plan-Level YAML Loading with Task Overrides

Load a complete plan from YAML but override specific tasks:

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.YamlBuilder

    class MyPlanRun extends PlanRun {
      val myPlan = YamlBuilder()
        .plan("config/base-banking-plan.yaml")
        .name("Custom Banking Plan")
        .tasks(
          // Override specific tasks from the loaded plan
          YamlBuilder().taskByFile("config/customer-task.yaml")
            .count(count.records(75000)),
          YamlBuilder().taskByName("account_task")
            .count(count.recordsPerField("customer_id", 2, 5))
            .fields(
              field.name("interest_rate").`type`(DoubleType).min(0.01).max(0.05)
            )
        )
        .execute()
    }
    ```

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.javaapi.api.YamlBuilder;

    public class MyPlanRun extends PlanRun {
        {
            var myPlan = new YamlBuilder()
                .plan("config/base-banking-plan.yaml")
                .name("Custom Banking Plan")
                .tasks(
                    // Override specific tasks from the loaded plan
                    new YamlBuilder().taskByFile("config/customer-task.yaml")
                        .count(count().records(75000)),
                    new YamlBuilder().taskByName("account_task")
                        .count(count().recordsPerField("customer_id", 2, 5))
                        .fields(
                            field().name("interest_rate").type(DoubleType.instance()).min(0.01).max(0.05)
                        )
                )
                .execute();
        }
    }
    ```

## Connection Configuration Integration

YamlBuilder automatically integrates with connection configurations defined in `application.conf`:

**application.conf:**
```hocon
postgres_customer {
  format = "postgres"
  url = "jdbc:postgresql://localhost:5432/customer"
  user = "postgres"
  password = "password"
}

csv_output {
  format = "csv"
  path = "/opt/app/data/output"
  header = "true"
}
```

**Unified YAML (customer-task.yaml):**
```yaml
name: "customer_data"

dataSources:
  - name: "postgres_customer"
    connection:
      type: "postgres"
      options:
        url: "jdbc:postgresql://localhost:5432/customer"
        user: "postgres"
        password: "password"
    steps:
      - name: "customers"
        options:
          dbtable: "customers"
        fields:
          - name: "customer_id"
            type: "string"
            options:
              regex: "CUST[0-9]{8}"
```

**Java/Scala Code:**
=== "Scala"

    ```scala
    // Connection details automatically merged from application.conf
    val customerTask = YamlBuilder()
      .taskByFile("config/customer-task.yaml")
      .count(count.records(50000))  // Only need to override what's different
    ```

=== "Java"

    ```java
    // Connection details automatically merged from application.conf
    var customerTask = new YamlBuilder()
        .taskByFile("config/customer-task.yaml")
        .count(count().records(50000));  // Only need to override what's different
    ```

## Benefits

### Separation of Concerns
- **Configuration in YAML**: Field definitions, base connection settings, validation rules
- **Logic in Code**: Environment-specific overrides, dynamic calculations, business rules

### Environment Management
- **Single YAML source**: Maintain one set of field definitions and schemas
- **Environment-specific code**: Override only what changes between environments
- **Version control**: YAML configs and code can be versioned independently

### Team Collaboration
- **Non-developers**: Can maintain YAML configurations and field definitions
- **Developers**: Focus on business logic and environment-specific customizations
- **Shared understanding**: YAML serves as documentation of data structure

### Reusability
- **Base configurations**: Share common YAML files across multiple projects
- **Modular approach**: Mix and match YAML tasks as needed
- **Template system**: Create reusable YAML templates for common patterns

## Best Practices

1. **Keep field definitions in YAML**: Structure, data types, basic constraints
2. **Use code for environment differences**: Record counts, connection URLs, performance settings
3. **Version control separately**: YAML configs and code can evolve independently  
4. **Document YAML schemas**: Include examples and expected formats
5. **Test YAML configurations**: Validate YAML syntax and schema compliance
6. **Use meaningful names**: Task and step names should be descriptive for filtering

## Migration Strategy

### From Pure Programmatic to Hybrid

1. **Extract common configurations** to YAML files
2. **Keep environment-specific settings** in code
3. **Migrate incrementally** - start with stable, reusable tasks
4. **Maintain backward compatibility** during transition

### From Pure YAML to Hybrid

1. **Identify customization points** that vary by environment
2. **Extract variable settings** to code parameters
3. **Keep stable schemas** in YAML
4. **Add programmatic overrides** as needed

This hybrid approach provides the best of both worlds: the simplicity and readability of YAML configurations with the power and flexibility of programmatic APIs.