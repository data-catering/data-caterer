package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.{DoubleType, LongType, TimestampType}

/**
 * Example demonstrating how to programmatically generate different numbers of records
 * per company based on a configuration map.
 *
 * Use case: Generate payment JSON files where each company gets a specific number of records.
 *
 * This approach is useful when:
 * - You have dynamic requirements (e.g., from CSV, database, config file)
 * - Different entities (companies, users, etc.) need different record counts
 * - You want to control the exact distribution programmatically
 */
class DynamicCompanyPaymentsPlan extends PlanRun {

  // Configuration: Map of company name -> number of records to generate
  // In a real scenario, this could be loaded from:
  // - CSV file: scala.io.Source.fromFile("companies.csv")
  // - Database query
  // - Configuration file
  // - Environment variables
  val companyRecordCounts = Map(
    "Acme Corp" -> 1000L,
    "TechStart Inc" -> 500L,
    "Global Solutions" -> 750L,
    "SmallBiz LLC" -> 250L
  )

  // Calculate total records and weighted distribution
  val totalRecords = companyRecordCounts.values.sum

  // Convert to weighted distribution format: "CompanyName->0.4" (weight based on proportion)
  val weightedCompanies = companyRecordCounts.map { case (company, count) =>
    val weight = count.toDouble / totalRecords
    s"$company->$weight"
  }.toList

  // Define the payment generation task
  // Note: For exact distribution by company, we use oneOf with repeated values based on proportions
  val companyNames = companyRecordCounts.keys.toList

  val paymentTask = json("company_payments", "/opt/app/data/payments")
    .count(count.records(totalRecords))
    .fields(
      // Company name field - will be distributed according to the oneOf selection
      field.name("company_name").oneOf("Acme Corp", "TechStart Inc", "Global Solutions", "SmallBiz LLC"),

      // Payment ID - unique identifier for each payment
      field.name("payment_id").regex("PAY-[0-9]{10}"),

      // Transaction ID - UUID format
      field.name("transaction_id").uuid(),

      // Payment amount - between $10 and $10,000
      field.name("amount").`type`(DoubleType).min(10.0).max(10000.0),

      // Payment timestamp
      field.name("payment_timestamp").`type`(TimestampType)
        .min(java.sql.Date.valueOf("2024-01-01")),

      // Payment method - randomly selected
      field.name("payment_method")
        .oneOf("CREDIT_CARD", "DEBIT_CARD", "BANK_TRANSFER", "PAYPAL", "CRYPTOCURRENCY"),

      // Status - weighted distribution (most successful)
      field.name("status_random").`type`(DoubleType).min(0.0).max(1.0).omit(true),
      field.name("status").sql("""
        CASE
          WHEN status_random < 0.85 THEN 'SUCCESS'
          WHEN status_random < 0.95 THEN 'PENDING'
          ELSE 'FAILED'
        END
      """),

      // Currency based on company (using SQL expression)
      field.name("currency")
        .sql("""
          CASE
            WHEN company_name LIKE '%Global%' THEN 'EUR'
            WHEN company_name LIKE '%Tech%' THEN 'GBP'
            ELSE 'USD'
          END
        """),

      // Metadata as nested structure
      field.name("metadata").fields(
        field.name("ip_address").expression("#{Internet.ipV4Address}"),
        field.name("user_agent").oneOf("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15"),
        field.name("session_id").uuid()
      )
    )

  // Configuration
  val conf = configuration
    .generatedReportsFolderPath("/opt/app/data/report")
    .enableUniqueCheck(true)

  // Print summary for visibility
  println("=" * 80)
  println("Dynamic Company Payments Generation Plan")
  println("=" * 80)
  companyRecordCounts.foreach { case (company, count) =>
    val percentage = (count.toDouble / totalRecords * 100).formatted("%.1f")
    println(f"  $company%-30s: $count%,6d records ($percentage%5s%%)")
  }
  println("-" * 80)
  println(f"  Total records to generate: $totalRecords%,d")
  println("=" * 80)

  execute(conf, paymentTask)
}

/**
 * Alternative approach: Generate separate files for each company
 * This creates one JSON file per company with exact record counts
 */
class DynamicCompanyPaymentsSeparateFilesPlan extends PlanRun {

  // Same configuration map
  val companyRecordCounts = Map(
    "Acme Corp" -> 1000L,
    "TechStart Inc" -> 500L,
    "Global Solutions" -> 750L,
    "SmallBiz LLC" -> 250L
  )

  // Dynamically create a task for each company
  val companyTasks = companyRecordCounts.map { case (companyName, recordCount) =>
    // Sanitize company name for file path
    val sanitizedName = companyName.toLowerCase.replaceAll("[^a-z0-9]", "_")

    json(s"payments_$sanitizedName", s"/opt/app/data/payments/$sanitizedName")
      .count(count.records(recordCount))
      .fields(
        // Static company name for this file
        field.name("company_name").sql(s"'$companyName'"),

        // Payment ID with company prefix
        field.name("payment_id")
          .sql(s"CONCAT('${sanitizedName.toUpperCase}', '-', LPAD(CAST(RAND() * 1000000000 AS BIGINT), 10, '0'))"),

        field.name("transaction_id").uuid(),

        field.name("amount").`type`(DoubleType).min(10.0).max(10000.0),

        field.name("payment_timestamp").`type`(TimestampType)
          .min(java.sql.Date.valueOf("2024-01-01")),

        field.name("payment_method")
          .oneOf("CREDIT_CARD", "DEBIT_CARD", "BANK_TRANSFER", "PAYPAL"),

        // Status - use SQL for weighted distribution
        field.name("status_random_sep").`type`(DoubleType).min(0.0).max(1.0).omit(true),
        field.name("status").sql("""
          CASE
            WHEN status_random_sep < 0.85 THEN 'SUCCESS'
            WHEN status_random_sep < 0.95 THEN 'PENDING'
            ELSE 'FAILED'
          END
        """)
      )
  }.toList

  val conf = configuration
    .generatedReportsFolderPath("/opt/app/data/report")

  println("=" * 80)
  println("Dynamic Company Payments - Separate Files")
  println("=" * 80)
  companyRecordCounts.foreach { case (company, count) =>
    println(f"  $company%-30s: $count%,6d records")
  }
  println("=" * 80)

  // Execute with all dynamically created tasks
  execute(conf, companyTasks.head, companyTasks.tail: _*)
}

/**
 * Advanced approach: Load from external source
 * This example shows how you might load company data from a CSV file
 */
class DynamicCompanyPaymentsFromCsvPlan extends PlanRun {

  // Example: Load from CSV (commented out, showing the pattern)
  /*
  import scala.io.Source

  val companyRecordCounts = {
    val source = Source.fromFile("/path/to/companies.csv")
    try {
      source.getLines()
        .drop(1) // Skip header
        .map { line =>
          val Array(companyName, recordCount) = line.split(",")
          companyName.trim -> recordCount.trim.toLong
        }
        .toMap
    } finally {
      source.close()
    }
  }
  */

  // For demonstration, using hardcoded map
  val companyRecordCounts = Map(
    "Acme Corp" -> 1000L,
    "TechStart Inc" -> 500L
  )

  val totalRecords = companyRecordCounts.values.sum
  val weightedCompanies = companyRecordCounts.map { case (company, count) =>
    s"$company->${count.toDouble / totalRecords}"
  }.toList

  val companyNamesCsv = companyRecordCounts.keys.toList

  val paymentTask = json("company_payments", "/opt/app/data/payments")
    .count(count.records(totalRecords))
    .fields(
      field.name("company_name").oneOf(companyNamesCsv.head, companyNamesCsv.tail: _*),
      field.name("payment_id").uuid(),
      field.name("amount").`type`(DoubleType).min(10.0).max(1000.0)
    )

  execute(configuration, paymentTask)
}
