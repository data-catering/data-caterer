package io.github.datacatering.datacaterer.core.foreignkey.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InsertOrderCalculatorTest extends AnyFunSuite with Matchers {

  test("Simple linear chain: A -> B -> C") {
    val dependencies = List(
      ("A", List("B")),
      ("B", List("C"))
    )

    val result = InsertOrderCalculator.getInsertOrder(dependencies)

    // A should come first (no dependencies), then B (depends on A), then C (depends on B)
    result shouldBe List("A", "B", "C")
  }

  test("Multiple children: A -> [B, C, D]") {
    val dependencies = List(
      ("A", List("B", "C", "D"))
    )

    val result = InsertOrderCalculator.getInsertOrder(dependencies)

    // A should come first, B/C/D can be in any order after A
    result.head shouldBe "A"
    result should contain allOf("B", "C", "D")
    result should have size 4
  }

  test("Diamond dependency: A -> [B, C] -> D") {
    val dependencies = List(
      ("A", List("B", "C")),
      ("B", List("D")),
      ("C", List("D"))
    )

    val result = InsertOrderCalculator.getInsertOrder(dependencies)

    // A should be first, D should be last, B and C should be between A and D
    result.head shouldBe "A"
    result.last shouldBe "D"
    result should contain allOf("A", "B", "C", "D")
    result should have size 4

    // B and C should come before D
    val bIndex = result.indexOf("B")
    val cIndex = result.indexOf("C")
    val dIndex = result.indexOf("D")
    bIndex should be < dIndex
    cIndex should be < dIndex
  }

  test("Complex multi-level graph") {
    val dependencies = List(
      ("accounts", List("transactions", "profiles")),
      ("transactions", List("payments")),
      ("profiles", List("preferences"))
    )

    val result = InsertOrderCalculator.getInsertOrder(dependencies)

    // Verify ordering constraints
    result.head shouldBe "accounts"

    val accountsIndex = result.indexOf("accounts")
    val transactionsIndex = result.indexOf("transactions")
    val paymentsIndex = result.indexOf("payments")
    val profilesIndex = result.indexOf("profiles")
    val preferencesIndex = result.indexOf("preferences")

    // accounts -> transactions -> payments
    accountsIndex should be < transactionsIndex
    transactionsIndex should be < paymentsIndex

    // accounts -> profiles -> preferences
    accountsIndex should be < profilesIndex
    profilesIndex should be < preferencesIndex
  }

  test("Empty foreign keys list returns empty order") {
    val result = InsertOrderCalculator.getInsertOrder(List())
    result shouldBe List()
  }

  test("Single data source with no dependencies") {
    val dependencies = List(
      ("A", List("B"))
    )

    val result = InsertOrderCalculator.getInsertOrder(dependencies)

    // A should come before B
    result shouldBe List("A", "B")
  }

  test("Circular dependency: A -> B -> A should throw exception") {
    val dependencies = List(
      ("A", List("B")),
      ("B", List("A"))
    )

    val exception = intercept[IllegalStateException] {
      InsertOrderCalculator.getInsertOrder(dependencies)
    }

    exception.getMessage should include("Circular dependency detected")
    exception.getMessage should include("A")
    exception.getMessage should include("B")
  }

  test("Circular dependency: A -> B -> C -> A should throw exception") {
    val dependencies = List(
      ("A", List("B")),
      ("B", List("C")),
      ("C", List("A"))
    )

    val exception = intercept[IllegalStateException] {
      InsertOrderCalculator.getInsertOrder(dependencies)
    }

    exception.getMessage should include("Circular dependency detected")
    // The cycle should include nodes in the cycle
    val message = exception.getMessage
    message should (include("A") or include("B") or include("C"))
  }

  test("Self-referencing foreign key: A -> A should throw exception") {
    val dependencies = List(
      ("A", List("A"))
    )

    val exception = intercept[IllegalStateException] {
      InsertOrderCalculator.getInsertOrder(dependencies)
    }

    exception.getMessage should include("Circular dependency detected")
    exception.getMessage should include("A")
  }

  test("Delete order is reverse of insert order: A -> B -> C") {
    val dependencies = List(
      ("A", List("B")),
      ("B", List("C"))
    )

    val insertOrder = InsertOrderCalculator.getInsertOrder(dependencies)
    val deleteOrder = InsertOrderCalculator.getDeleteOrder(dependencies)

    // Delete order should be exact reverse of insert order
    deleteOrder shouldBe insertOrder.reverse
    deleteOrder shouldBe List("C", "B", "A")
  }

  test("Delete order for complex graph") {
    val dependencies = List(
      ("accounts", List("transactions")),
      ("transactions", List("payments"))
    )

    val deleteOrder = InsertOrderCalculator.getDeleteOrder(dependencies)

    // Verify delete order: children before parents
    val accountsIndex = deleteOrder.indexOf("accounts")
    val transactionsIndex = deleteOrder.indexOf("transactions")
    val paymentsIndex = deleteOrder.indexOf("payments")

    // payments -> transactions -> accounts (reverse of insert)
    paymentsIndex should be < transactionsIndex
    transactionsIndex should be < accountsIndex
  }

  test("Disconnected components: [A -> B] and [C -> D]") {
    val dependencies = List(
      ("A", List("B")),
      ("C", List("D"))
    )

    val result = InsertOrderCalculator.getInsertOrder(dependencies)

    // Should have all 4 data sources
    result should have size 4
    result should contain allOf("A", "B", "C", "D")

    // Within each component, ordering should be correct
    val aIndex = result.indexOf("A")
    val bIndex = result.indexOf("B")
    val cIndex = result.indexOf("C")
    val dIndex = result.indexOf("D")

    aIndex should be < bIndex
    cIndex should be < dIndex
  }

  test("Multiple foreign keys from same source to different targets") {
    val dependencies = List(
      ("users", List("orders")),
      ("users", List("reviews")),
      ("users", List("addresses"))
    )

    val result = InsertOrderCalculator.getInsertOrder(dependencies)

    // users should be first
    result.head shouldBe "users"

    // All other tables should come after users
    result should contain allOf("orders", "reviews", "addresses")
    result should have size 4
  }

  test("Composite dependency structure with merging") {
    val dependencies = List(
      ("A", List("B")),
      ("B", List("C"))
    )

    val result = InsertOrderCalculator.getInsertOrder(dependencies)

    // Should still work correctly
    result shouldBe List("A", "B", "C")
  }

  test("Wide dependency tree: one parent, many levels of children") {
    val dependencies = List(
      ("root", List("level1_a", "level1_b")),
      ("level1_a", List("level2_a1", "level2_a2")),
      ("level1_b", List("level2_b1"))
    )

    val result = InsertOrderCalculator.getInsertOrder(dependencies)

    // root should be first
    result.head shouldBe "root"

    // Verify level ordering
    val rootIdx = result.indexOf("root")
    val level1aIdx = result.indexOf("level1_a")
    val level1bIdx = result.indexOf("level1_b")
    val level2a1Idx = result.indexOf("level2_a1")
    val level2a2Idx = result.indexOf("level2_a2")
    val level2b1Idx = result.indexOf("level2_b1")

    // Level 1 nodes should come after root
    rootIdx should be < level1aIdx
    rootIdx should be < level1bIdx

    // Level 2 nodes should come after their level 1 parents
    level1aIdx should be < level2a1Idx
    level1aIdx should be < level2a2Idx
    level1bIdx should be < level2b1Idx
  }

  test("Complex cycle detection: Multi-node cycle") {
    val dependencies = List(
      ("A", List("B")),
      ("B", List("C")),
      ("C", List("D")),
      ("D", List("B"))  // Creates cycle: B -> C -> D -> B
    )

    val exception = intercept[IllegalStateException] {
      InsertOrderCalculator.getInsertOrder(dependencies)
    }

    exception.getMessage should include("Circular dependency detected")
    // Should mention nodes involved in the cycle
    val message = exception.getMessage
    message should (include("B") or include("C") or include("D"))
  }

  test("Child order preservation") {
    val dependencies = List(
      ("parent", List("child1", "child2", "child3"))
    )

    val result = InsertOrderCalculator.getInsertOrder(dependencies)

    // Parent should be first
    result.head shouldBe "parent"

    // Children should appear in the order they were specified
    val child1Idx = result.indexOf("child1")
    val child2Idx = result.indexOf("child2")
    val child3Idx = result.indexOf("child3")

    child1Idx should be < child2Idx
    child2Idx should be < child3Idx
  }

  test("Delete order handles disconnected components") {
    val dependencies = List(
      ("A", List("B")),
      ("C", List("D"))
    )

    val deleteOrder = InsertOrderCalculator.getDeleteOrder(dependencies)

    // Should have all 4 data sources
    deleteOrder should have size 4
    deleteOrder should contain allOf("A", "B", "C", "D")

    // Within each component, children should come before parents
    val aIdx = deleteOrder.indexOf("A")
    val bIdx = deleteOrder.indexOf("B")
    val cIdx = deleteOrder.indexOf("C")
    val dIdx = deleteOrder.indexOf("D")

    bIdx should be < aIdx
    dIdx should be < cIdx
  }
}
