package io.github.datacatering.datacaterer.core.util

import java.sql.Date

/**
 * Test case classes used across multiple test files.
 *
 * These were originally defined in ForeignKeyUtilTest.scala but are now
 * shared here since they're used by multiple test classes.
 */
case class Account(
  account_id: String = "acc123",
  name: String = "peter",
  open_date: Date = Date.valueOf("2023-01-31"),
  age: Int = 10,
  debitCredit: String = "D"
)

case class Transaction(
  account_id: String,
  name: String,
  transaction_id: String,
  created_date: Date,
  amount: Double,
  links: List[String] = List()
)
