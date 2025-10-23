package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.List;

/**
 * Example demonstrating how to reuse a single YAML metadata source to create multiple outputs
 * with different customizations in Java. This pattern is useful when you want to:
 * - Generate data for different environments (dev, test, prod)
 * - Create datasets with varying sizes
 * - Apply environment-specific field customizations
 * - Generate multiple variations from a single schema definition
 */
public class YamlMetadataReuseMultipleOutputsJava extends PlanRun {

    {
        // Reference the YAML task schema once
        // This YAML file defines the base schema with fields like account_id, name, balance, etc.
        var baseYamlSchema = metadataSource().yamlTask(
                "app/src/test/resources/sample/task/file/simple-json-task.yaml",
                "simple_json",
                "file_account"
        );

        // Task 1: Development environment - small dataset with test data
        var devJsonTask = json("dev_accounts", "/tmp/yaml-reuse/dev/accounts.json")
                .fields(baseYamlSchema)
                .fields(
                        // Override specific fields for dev environment
                        field().name("account_id").regex("DEV-[0-9]{8}"),  // Dev-specific ID pattern
                        field().name("status").sql("'ACTIVE'")  // All dev accounts active
                )
                .count(count().records(100));  // Small dataset for development

        // Task 2: Test environment - medium dataset with varied test scenarios
        var testJsonTask = json("test_accounts", "/tmp/yaml-reuse/test/accounts.json")
                .fields(baseYamlSchema)
                .fields(
                        field().name("account_id").regex("TEST-[0-9]{8}"),  // Test-specific ID pattern
                        field().name("balance").min(0.0).max(10000.0),  // Test with moderate balances
                        field().name("status").oneOf("ACTIVE", "PENDING", "SUSPENDED")  // Various statuses for testing
                )
                .count(count().records(1000));  // Medium dataset for testing

        // Task 3: UAT environment - larger dataset with production-like data
        var uatJsonTask = json("uat_accounts", "/tmp/yaml-reuse/uat/accounts.json")
                .fields(baseYamlSchema)
                .fields(
                        field().name("account_id").regex("UAT-[0-9]{8}"),  // UAT-specific ID pattern
                        field().name("balance").min(100.0).max(1000000.0),  // Production-like balance range
                        field().name("created_date").sql("date_sub(current_date(), cast(rand() * 365 as int))")  // Dates within last year
                )
                .count(count().records(10000));  // Large dataset for UAT

        // Task 4: CSV output for data analytics team
        var analyticsCSVTask = csv("analytics_accounts", "/tmp/yaml-reuse/analytics/accounts.csv")
                .fields(baseYamlSchema)
                .fields(
                        field().name("account_id").regex("ANAL-[0-9]{8}"),
                        field().name("balance").min(1000.0).max(500000.0),  // Focus on mid-range accounts
                        field().name("account_type").oneOf("CHECKING", "SAVINGS", "INVESTMENT")  // Analytics categories
                )
                .count(count().records(50000));  // Large dataset for analytics

        // Task 5: Parquet output for data lake
        var dataLakeParquetTask = parquet("datalake_accounts", "/tmp/yaml-reuse/datalake/accounts.parquet")
                .fields(baseYamlSchema)
                .fields(
                        field().name("account_id").regex("DL-[0-9]{10}"),  // Data lake ID pattern
                        field().name("balance").min(0.0).max(10000000.0)  // Full range of balances
                )
                .count(count().records(100000));  // Very large dataset for data lake

        // Configuration to enable all tasks
        var config = configuration()
                .enableGeneratePlanAndTasks(true)
                .enableGenerateData(true);

        // Execute all 5 tasks
        execute(
                config,
                devJsonTask,
                testJsonTask,
                uatJsonTask,
                analyticsCSVTask,
                dataLakeParquetTask
        );
    }
}

/**
 * Alternative example showing reuse with step-level customization
 */
class YamlMetadataReuseWithStepCustomizationJava extends PlanRun {

    record RegionConfig(String regionName, String idPrefix, long recordCount) {}

    {
        // Load the YAML schema
        var yamlSchema = metadataSource().yamlTask(
                "app/src/test/resources/sample/task/file/simple-json-task.yaml",
                "simple_json",
                "file_account"
        );

        // Region-specific outputs with different characteristics
        var regions = List.of(
                new RegionConfig("us_east", "US-EAST-", 25000),
                new RegionConfig("us_west", "US-WEST-", 30000),
                new RegionConfig("eu_central", "EU-CENT-", 20000),
                new RegionConfig("asia_pacific", "APAC-", 15000),
                new RegionConfig("south_america", "LATAM-", 10000)
        );

        // Generate tasks for each region with customizations
        var usEastTask = json("us_east_accounts", "/tmp/yaml-reuse/regions/us_east/accounts.json")
                .fields(yamlSchema)
                .fields(
                        field().name("account_id").regex("US-EAST-[0-9]{8}"),
                        field().name("region").sql("'us_east'")
                )
                .count(count().records(25000));

        var usWestTask = json("us_west_accounts", "/tmp/yaml-reuse/regions/us_west/accounts.json")
                .fields(yamlSchema)
                .fields(
                        field().name("account_id").regex("US-WEST-[0-9]{8}"),
                        field().name("region").sql("'us_west'")
                )
                .count(count().records(30000));

        var euCentralTask = json("eu_central_accounts", "/tmp/yaml-reuse/regions/eu_central/accounts.json")
                .fields(yamlSchema)
                .fields(
                        field().name("account_id").regex("EU-CENT-[0-9]{8}"),
                        field().name("region").sql("'eu_central'")
                )
                .count(count().records(20000));

        var asiaPacificTask = json("asia_pacific_accounts", "/tmp/yaml-reuse/regions/asia_pacific/accounts.json")
                .fields(yamlSchema)
                .fields(
                        field().name("account_id").regex("APAC-[0-9]{8}"),
                        field().name("region").sql("'asia_pacific'")
                )
                .count(count().records(15000));

        var southAmericaTask = json("south_america_accounts", "/tmp/yaml-reuse/regions/south_america/accounts.json")
                .fields(yamlSchema)
                .fields(
                        field().name("account_id").regex("LATAM-[0-9]{8}"),
                        field().name("region").sql("'south_america'")
                )
                .count(count().records(10000));

        var config = configuration()
                .enableGeneratePlanAndTasks(true)
                .enableGenerateData(true);

        // Execute all regional tasks
        execute(
                config,
                usEastTask,
                usWestTask,
                euCentralTask,
                asiaPacificTask,
                southAmericaTask
        );
    }
}
