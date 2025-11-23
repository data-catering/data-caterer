package io.github.datacatering.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.IntegerType

class CsvMultipleRelationshipsPlan extends PlanRun {

  val numCustomers = 10
  val numAccounts = 30
  val maxNumRolesPerCustomer = 5

  val customerTask = csv("customers", "/opt/app/data/customer/customers", Map("header" -> "true", "partitions" -> "1", "saveMode" -> "overwrite"))
    .fields(
      field.name("customer_id").uuid().incremental(),
      field.name("first_name").expression("#{Name.firstName}"),
      field.name("last_name").expression("#{Name.lastName}")
    )
    .count(count.records(numCustomers))

  val accountTask = csv("customer_accounts", "/opt/app/data/customer/accounts", Map("header" -> "true", "partitions" -> "1", "saveMode" -> "overwrite"))
    .fields(
      field.name("products_id_int").`type`(IntegerType).unique(true).omit(true),
      field.name("products_id").uuid().incremental(),
      field.name("source_id").uuid(),
      field.name("country_code").expression("#{Address.countryCode}"),
      field.name("name").expression("#{Name.name}")
    )
    .count(count.records(numAccounts))

  val customerAccessTask = csv("customer_access", "/opt/app/data/customer/access", Map("header" -> "true", "partitions" -> "1", "saveMode" -> "overwrite"))
    .fields(
      field.name("customer_products_id").uuid().incremental(),
      field.name("products_id_int").`type`(IntegerType).min(1).max(numAccounts).omit(true),
      field.name("products_id").uuid("products_id_int"),
      field.name("party_id").uuid()
    )
    .count(count.recordsPerFieldGenerator(numCustomers, generator.min(1).max(maxNumRolesPerCustomer), "customer_products_id"))

  val config = configuration
    .generatedReportsFolderPath("/opt/app/data/report")

  execute(config, customerTask, accountTask, customerAccessTask)
}
