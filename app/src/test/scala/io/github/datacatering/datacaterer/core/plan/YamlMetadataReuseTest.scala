package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.spark.sql.functions.col

/**
 * Test demonstrating the YAML metadata reuse pattern where a single YAML schema
 * is used to create multiple output tasks with different customizations.
 * This validates the real-world use case of generating data for multiple regions/environments
 * from a single base schema definition.
 */
class YamlMetadataReuseTest extends SparkSuite {

  ignore("Can reuse YAML metadata to create multiple customized outputs") {
    class YamlMetadataReusePlan extends PlanRun {
      val yamlTaskPath = getClass.getClassLoader.getResource("sample/task/file/account-reuse-task.yaml").getPath

      // Region configurations - using smaller record counts for fast tests
      val regions = List(
        ("us_east", "US-EAST-", 10),
        ("us_west", "US-WEST-", 15),
        ("eu_central", "EU-CENT-", 12)
      )

      // Generate a task for each region with customizations
      val regionTasks = regions.map { case (regionName, idPrefix, recordCount) =>
        json(s"${regionName}_accounts", s"/tmp/yaml-reuse/test/$regionName/accounts.json",
          Map("saveMode" -> "overwrite", "numPartitions" -> "1"))
          .fields(metadataSource.yamlTask(
            yamlTaskPath,
            "account_base_task",
            "accounts"
          ))
          .fields(
            // Override the account_id pattern to be region-specific
            field.name("account_id").regex(s"$idPrefix[0-9]{8}"),
            // Override the region field to match the region name
            field.name("region").sql(s"'$regionName'")
          )
          .count(count.records(recordCount))
      }

      val conf = configuration
        .enableGeneratePlanAndTasks(true)
        .generatedReportsFolderPath("/tmp/data-caterer/reports")

      // Execute all regional tasks
      execute(conf, regionTasks)
    }

    // Execute the plan
    PlanProcessor.determineAndExecutePlan(Some(new YamlMetadataReusePlan()))

    // Verify US East data
    val usEastData = sparkSession.read.json("/tmp/yaml-reuse/test/us_east/accounts.json")
    assert(usEastData.count() == 10, "Expected 10 records for US East")
    assert(usEastData.columns.length == 6, "Expected 6 fields from YAML schema")

    // Verify all account IDs have US-EAST prefix
    val usEastIds = usEastData.select("account_id").collect().map(_.getString(0))
    assert(usEastIds.forall(_.startsWith("US-EAST-")), "All US East account IDs should have US-EAST- prefix")

    // Verify region field is set correctly
    val usEastRegions = usEastData.select("region").distinct().collect()
    assert(usEastRegions.length == 1, "Should only have one region value")
    assert(usEastRegions.head.getString(0) == "us_east", "Region should be 'us_east'")

    // Verify US West data
    val usWestData = sparkSession.read.json("/tmp/yaml-reuse/test/us_west/accounts.json")
    assert(usWestData.count() == 15, "Expected 15 records for US West")

    val usWestIds = usWestData.select("account_id").collect().map(_.getString(0))
    assert(usWestIds.forall(_.startsWith("US-WEST-")), "All US West account IDs should have US-WEST- prefix")

    val usWestRegions = usWestData.select("region").distinct().collect()
    assert(usWestRegions.head.getString(0) == "us_west", "Region should be 'us_west'")

    // Verify EU Central data
    val euCentralData = sparkSession.read.json("/tmp/yaml-reuse/test/eu_central/accounts.json")
    assert(euCentralData.count() == 12, "Expected 12 records for EU Central")

    val euCentralIds = euCentralData.select("account_id").collect().map(_.getString(0))
    assert(euCentralIds.forall(_.startsWith("EU-CENT-")), "All EU Central account IDs should have EU-CENT- prefix")

    val euCentralRegions = euCentralData.select("region").distinct().collect()
    assert(euCentralRegions.head.getString(0) == "eu_central", "Region should be 'eu_central'")

    // Verify all datasets have the same schema (inherited from YAML)
    assert(usEastData.schema == usWestData.schema, "US East and US West should have same schema")
    assert(usWestData.schema == euCentralData.schema, "US West and EU Central should have same schema")

    // Verify base fields from YAML are present and working
    val allData = List(usEastData, usWestData, euCentralData)
    allData.foreach { data =>
      // Check that balance field is within expected range (0.0 to 10000.0 from YAML)
      val balances = data.select("balance").collect().map(_.getDouble(0))
      assert(balances.forall(b => b >= 0.0 && b <= 10000.0), "Balance should be within YAML-defined range")

      // Check that status field has expected values
      val statuses = data.select("status").distinct().collect().map(_.getString(0))
      assert(statuses.forall(s => Set("ACTIVE", "PENDING", "CLOSED").contains(s)),
        "Status should be one of the values defined in YAML")

      // Check that name field is populated (from faker expression)
      val names = data.select("name").collect().map(_.getString(0))
      assert(names.forall(_.nonEmpty), "All names should be non-empty")

      // Check that created_date field is populated
      val dates = data.select("created_date").collect()
      assert(dates.forall(!_.isNullAt(0)), "All created dates should be non-null")
    }
  }

  test("Can reuse YAML metadata with different output formats") {
    class YamlMetadataMultiFormatPlan extends PlanRun {
      val yamlTaskPath = getClass.getClassLoader.getResource("sample/task/file/account-reuse-task.yaml").getPath
      val yamlMetadata = metadataSource.yamlTask(yamlTaskPath, "account_base_task", "accounts")

      // Create outputs in different formats using the same schema
      val jsonTask = json("accounts_json", "/tmp/yaml-reuse/format-test/accounts.json",
        Map("saveMode" -> "overwrite", "numPartitions" -> "1"))
        .fields(yamlMetadata)
        .fields(field.name("account_id").regex("JSON-[0-9]{8}"))
        .count(count.records(5))

      val csvTask = csv("accounts_csv", "/tmp/yaml-reuse/format-test/accounts.csv",
        Map("saveMode" -> "overwrite", "numPartitions" -> "1", "header" -> "true"))
        .fields(yamlMetadata)
        .fields(field.name("account_id").regex("CSV-[0-9]{8}"))
        .count(count.records(5))

      val conf = configuration
        .generatedReportsFolderPath("/tmp/data-caterer/reports")
        .numRecordsPerBatch(2)
        .enableGeneratePlanAndTasks(true)

      execute(conf, jsonTask, csvTask)
    }

    // Execute the plan
    PlanProcessor.determineAndExecutePlan(Some(new YamlMetadataMultiFormatPlan()))

    // Verify JSON output
    val jsonData = sparkSession.read.json("/tmp/yaml-reuse/format-test/accounts.json")
    assert(jsonData.count() == 5, "Expected 5 JSON records")
    val jsonIds = jsonData.select("account_id").collect().map(_.getString(0))
    assert(jsonIds.forall(_.startsWith("JSON-")), "JSON records should have JSON- prefix")

    // Verify CSV output
    val csvData = sparkSession.read.option("header", "true").csv("/tmp/yaml-reuse/format-test/accounts.csv")
    assert(csvData.count() == 5, "Expected 5 CSV records")
    val csvIds = csvData.select("account_id").collect().map(_.getString(0))
    assert(csvIds.forall(_.startsWith("CSV-")), "CSV records should have CSV- prefix")

    // Verify all formats have the same fields (from YAML schema)
    val jsonFields = jsonData.columns.sorted
    val csvFields = csvData.columns.sorted
    val expectedFields = List("account_id", "balance", "created_date", "email", "name", "status")

    assert(csvFields.sameElements(expectedFields), "CSV fields should be the same as the YAML schema")
    assert(jsonFields.sameElements(csvFields), "JSON and CSV should have same fields")
  }

  test("Can override nested structure fields from YAML metadata") {
    class YamlNestedStructureOverridePlan extends PlanRun {
      val yamlTaskPath = getClass.getClassLoader.getResource("sample/task/file/nested-structure-reuse-task.yaml").getPath
      val yamlMetadata = metadataSource.yamlTask(yamlTaskPath, "nested_structure_base_task", "user_orders")

      // Create a task that reuses YAML metadata but overrides nested structure fields
      // Note: Nested field overrides must use hierarchical structure, not dot notation
      val ordersTask = json("orders_with_overrides", "/tmp/yaml-reuse/nested-test/orders.json",
        Map("saveMode" -> "overwrite", "numPartitions" -> "1"))
        .fields(yamlMetadata)
        .fields(
          // Override top-level field
          field.name("order_id").regex("CUSTOM-ORD-[0-9]{6}"),
          // Override nested fields in customer struct using hierarchical structure
          field.name("customer").fields(
            field.name("customer_id").regex("OVERRIDE-[0-9]{10}"),
            field.name("name").sql("'TestCustomer'"),
            // Override deeply nested field in customer.contact struct
            field.name("contact").fields(
              field.name("email").sql("'custom@test.com'"),
              field.name("phone").regex("\\+1-[0-9]{3}-[0-9]{3}-[0-9]{4}")
            )
          ),
          // Override nested fields in shipping_address struct
          field.name("shipping_address").fields(
            field.name("country").sql("'CustomCountry'"),
            field.name("city").sql("'CustomCity'")
          ),
          // Override array element fields (items is an array, so we override the element structure)
          field.name("items").fields(
            field.name("product_id").regex("CUSTOM-PROD-[0-9]{8}"),
            field.name("quantity").sql("99"),
            field.name("price").sql("1234.56")
          )
        )
        .count(count.records(3))

      val conf = configuration
        .generatedReportsFolderPath("/tmp/data-caterer/reports")
        .enableGeneratePlanAndTasks(true)

      execute(conf, ordersTask)
    }

    // Execute the plan
    PlanProcessor.determineAndExecutePlan(Some(new YamlNestedStructureOverridePlan()))

    // Verify the output
    val ordersData = sparkSession.read.json("/tmp/yaml-reuse/nested-test/orders.json")
    assert(ordersData.count() == 3, "Expected 3 records")

    // Verify top-level override
    val orderIds = ordersData.select("order_id").collect().map(_.getString(0))
    assert(orderIds.forall(_.startsWith("CUSTOM-ORD-")), "Order IDs should have custom prefix")
    assert(orderIds.forall(_.matches("CUSTOM-ORD-[0-9]{6}")), "Order IDs should match custom pattern")

    // Verify nested struct overrides (customer level)
    val customerIds = ordersData.select("customer.customer_id").collect().map(_.getString(0))
    assert(customerIds.forall(_.startsWith("OVERRIDE-")), "Customer IDs should have override prefix")
    assert(customerIds.forall(_.matches("OVERRIDE-[0-9]{10}")), "Customer IDs should match override pattern")

    val customerNames = ordersData.select("customer.name").collect().map(_.getString(0))
    assert(customerNames.forall(_ == "TestCustomer"), "Customer names should be overridden to TestCustomer")

    // Verify deeply nested struct overrides (customer.contact level)
    val emails = ordersData.select("customer.contact.email").collect().map(_.getString(0))
    assert(emails.forall(_ == "custom@test.com"), "Emails should be overridden")

    val phones = ordersData.select("customer.contact.phone").collect().map(_.getString(0))
    assert(phones.forall(_.matches("\\+1-[0-9]{3}-[0-9]{3}-[0-9]{4}")), "Phones should match custom pattern")

    // Verify shipping_address nested overrides
    val countries = ordersData.select("shipping_address.country").collect().map(_.getString(0))
    assert(countries.forall(_ == "CustomCountry"), "Countries should be overridden")

    val cities = ordersData.select("shipping_address.city").collect().map(_.getString(0))
    assert(cities.forall(_ == "CustomCity"), "Cities should be overridden")

    // Verify that non-overridden nested fields still work from YAML
    val streets = ordersData.select("shipping_address.street").collect().map(_.getString(0))
    assert(streets.forall(_.nonEmpty), "Streets should still be generated from YAML faker expression")

    val postalCodes = ordersData.select("shipping_address.postal_code").collect().map(_.getString(0))
    assert(postalCodes.forall(_.nonEmpty), "Postal codes should still be generated from YAML")

    // Verify array element overrides
    import org.apache.spark.sql.functions.explode
    val itemsExploded = ordersData.select(explode(col("items")).as("item"))

    // Check product IDs have custom pattern
    val productIds = itemsExploded.select("item.product_id").collect().map(_.getString(0))
    assert(productIds.forall(_.startsWith("CUSTOM-PROD-")), "Product IDs should have custom prefix")
    assert(productIds.forall(_.matches("CUSTOM-PROD-[0-9]{8}")), "Product IDs should match custom pattern")

    // Check quantities are overridden
    val quantities = itemsExploded.select("item.quantity").collect().map(_.getLong(0))
    assert(quantities.forall(_ == 99), "Quantities should be overridden to 99")

    // Check prices are overridden
    val prices = itemsExploded.select("item.price").collect().map(_.getDouble(0))
    assert(prices.forall(p => Math.abs(p - 1234.56) < 0.01), "Prices should be overridden to 1234.56")

    // Verify that non-overridden array field still works from YAML
    val productNames = itemsExploded.select("item.product_name").collect().map(_.getString(0))
    assert(productNames.forall(_.nonEmpty), "Product names should still be generated from YAML faker expression")

    // Verify the overall schema structure is preserved from YAML
    val expectedTopLevelFields = Set("order_id", "order_date", "customer", "shipping_address", "items")
    assert(ordersData.columns.toSet == expectedTopLevelFields, "Top-level fields should match YAML schema")
  }
}
