package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.util.PlanImplicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Test to reproduce the foreign key + perField count multiplication bug
 */
class ForeignKeyPerFieldTest extends AnyFunSuite with Matchers {

  test("RecordCountUtil calculate correct record counts with foreign keys and perField") {
    // Simulate the YAML configuration from the failing test
    val balancesStep = Step(
      name = "balances",
      count = Count(records = Some(1000), perField = None)
    )

    val transactionsStep = Step(
      name = "transactions",
      count = Count(
        records = None,  // This will be set by FK logic
        perField = Some(PerFieldCount(
          fieldNames = List("account_number"),
          count = Some(5)
        ))
      )
    )

    val task = Task(
      name = "jdbc_customer_balance_and_transactions",
      steps = List(balancesStep, transactionsStep)
    )

    val taskSummary = TaskSummary(
      name = "jdbc_customer_balance_and_transactions",
      dataSourceName = "postgres"
    )

    val foreignKey = ForeignKey(
      source = ForeignKeyRelation(
        dataSource = "postgres",
        step = "balances",
        fields = List("account_number")
      ),
      generate = List(ForeignKeyRelation(
        dataSource = "postgres",
        step = "transactions",
        fields = List("account_number")
      ))
    )

    val executableTasks = List((taskSummary, task))
    val generationConfig = GenerationConfig()

    // Calculate counts
    val countsPerStep = RecordCountUtil.getCountPerStep(
      List(foreignKey),
      executableTasks,
      generationConfig
    ).toMap

    println("Counts per step:")
    countsPerStep.foreach { case (step, count) =>
      println(s"  $step: $count")
    }

    // Expected:
    // - balances: 1000 (simple count)
    // - transactions: 1000 * 5 = 5000 (FK should set records=1000, then perField multiplies by 5)
    val balancesCount = countsPerStep("jdbc_customer_balance_and_transactions_balances")
    val transactionsCount = countsPerStep("jdbc_customer_balance_and_transactions_transactions")

    balancesCount shouldBe 1000
    transactionsCount shouldBe 5000  // This is what we WANT
  }

  test("Count.numRecords correctly calculate for perField with explicit records") {
    // Test case 1: perField with records explicitly set (like FK does)
    val count1 = Count(
      records = Some(1000),
      perField = Some(PerFieldCount(
        fieldNames = List("account_number"),
        count = Some(5)
      ))
    )

    val result1 = count1.numRecords
    println(s"Count with records=1000 and perField.count=5: $result1")
    result1 shouldBe 5000

    // Test case 2: perField without records (original YAML form)
    val count2 = Count(
      records = None,
      perField = Some(PerFieldCount(
        fieldNames = List("account_number"),
        count = Some(5)
      ))
    )

    val result2 = count2.numRecords
    println(s"Count with records=None and perField.count=5: $result2")
    // When records is None, it defaults to DEFAULT_COUNT_RECORDS (1000)
    result2 shouldBe 1000
  }
}
