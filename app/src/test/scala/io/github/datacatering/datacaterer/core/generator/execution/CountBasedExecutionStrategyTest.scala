package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Count, GenerationConfig, Plan, Step, Task, TaskSummary}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CountBasedExecutionStrategyTest extends AnyFunSuite with Matchers {

  private val defaultGenerationConfig = GenerationConfig(numRecordsPerBatch = 1000)

  private def createStrategy(
    recordsPerStep: List[Int],
    generationConfig: GenerationConfig = defaultGenerationConfig
  ): CountBasedExecutionStrategy = {
    val plan = Plan(name = "test_plan")
    val steps = recordsPerStep.zipWithIndex.map { case (records, idx) =>
      Step(name = s"step$idx", count = Count(records = Some(records)))
    }
    val task = Task(name = "test_task", steps = steps)
    val executableTasks = List((TaskSummary("test_task", "test_ds"), task))
    new CountBasedExecutionStrategy(plan, executableTasks, generationConfig)
  }

  test("calculateNumBatches for single step with records under batch size") {
    val strategy = createStrategy(List(500))
    strategy.calculateNumBatches shouldBe 1
  }

  test("calculateNumBatches for single step with records equal to batch size") {
    val strategy = createStrategy(List(1000))
    strategy.calculateNumBatches shouldBe 1
  }

  test("calculateNumBatches for single step with records over batch size") {
    val strategy = createStrategy(List(2500))
    strategy.calculateNumBatches shouldBe 3 // ceil(2500 / 1000)
  }

  test("calculateNumBatches for multiple steps") {
    val strategy = createStrategy(List(1000, 2000, 500))
    // Sum of records across steps is 3500, so ceil(3500 / 1000) = 4 batches
    strategy.calculateNumBatches shouldBe 4
  }

  test("calculateNumBatches with custom batch size") {
    val config = GenerationConfig(numRecordsPerBatch = 500)
    val strategy = createStrategy(List(1750), config)
    strategy.calculateNumBatches shouldBe 4 // ceil(1750 / 500)
  }

  test("calculateNumBatches with large record count") {
    val strategy = createStrategy(List(100000))
    strategy.calculateNumBatches shouldBe 100 // ceil(100000 / 1000)
  }

  test("calculateNumBatches with zero records") {
    val strategy = createStrategy(List(0))
    // Even with 0 records, returns at least 1 batch
    strategy.calculateNumBatches shouldBe 1
  }

  test("shouldContinue returns true for batch 1 when numBatches is 1") {
    val strategy = createStrategy(List(500))
    strategy.shouldContinue(1) shouldBe true
  }

  test("shouldContinue returns false for batch 2 when numBatches is 1") {
    val strategy = createStrategy(List(500))
    strategy.shouldContinue(2) shouldBe false
  }

  test("shouldContinue returns true for batches within range") {
    val strategy = createStrategy(List(3000))
    strategy.shouldContinue(1) shouldBe true
    strategy.shouldContinue(2) shouldBe true
    strategy.shouldContinue(3) shouldBe true
  }

  test("shouldContinue returns false for batches beyond range") {
    val strategy = createStrategy(List(3000))
    strategy.shouldContinue(4) shouldBe false
    strategy.shouldContinue(5) shouldBe false
  }

  test("shouldContinue boundary test at exact batch count") {
    val strategy = createStrategy(List(5000)) // 5 batches
    strategy.shouldContinue(5) shouldBe true
    strategy.shouldContinue(6) shouldBe false
  }

  test("getMetrics returns None for count-based strategy") {
    val strategy = createStrategy(List(1000))
    strategy.getMetrics shouldBe None
  }

  test("getGenerationMode returns Batched") {
    val strategy = createStrategy(List(1000))
    strategy.getGenerationMode shouldBe GenerationMode.Batched
  }

  test("handles empty steps list") {
    val plan = Plan(name = "test_plan")
    val task = Task(name = "test_task", steps = List())
    val executableTasks = List((TaskSummary("test_task", "test_ds"), task))
    val strategy = new CountBasedExecutionStrategy(plan, executableTasks, defaultGenerationConfig)

    // Returns at least 1 batch even with empty steps
    strategy.calculateNumBatches shouldBe 1
    strategy.shouldContinue(1) shouldBe true
  }

  test("handles multiple tasks with different record counts") {
    val plan = Plan(name = "test_plan")
    val task1 = Task(name = "task1", steps = List(
      Step(name = "step1", count = Count(records = Some(1500)))
    ))
    val task2 = Task(name = "task2", steps = List(
      Step(name = "step1", count = Count(records = Some(2500)))
    ))
    val executableTasks = List(
      (TaskSummary("task1", "ds1"), task1),
      (TaskSummary("task2", "ds2"), task2)
    )
    val strategy = new CountBasedExecutionStrategy(plan, executableTasks, defaultGenerationConfig)

    // Should sum records across all tasks/steps: 1500 + 2500 = 4000, ceil(4000 / 1000) = 4
    strategy.calculateNumBatches shouldBe 4
  }

  test("onBatchStart does not throw exception") {
    val strategy = createStrategy(List(1000))
    strategy.onBatchStart(1)
    succeed
  }

  test("onBatchEnd does not throw exception") {
    val strategy = createStrategy(List(1000))
    strategy.onBatchEnd(1, 1000)
    succeed
  }

  test("backward compatibility with existing record count logic") {
    // This test ensures count-based strategy maintains backward compatibility
    val strategy = createStrategy(List(10000))
    val numBatches = strategy.calculateNumBatches

    numBatches shouldBe 10
    (1 to numBatches).foreach { batch =>
      strategy.shouldContinue(batch) shouldBe true
    }
    strategy.shouldContinue(numBatches + 1) shouldBe false
  }

  test("handles very large batch sizes") {
    val config = GenerationConfig(numRecordsPerBatch = 1000000)
    val strategy = createStrategy(List(500000), config)
    strategy.calculateNumBatches shouldBe 1
  }

  test("handles very small batch sizes") {
    val config = GenerationConfig(numRecordsPerBatch = 10)
    val strategy = createStrategy(List(100), config)
    strategy.calculateNumBatches shouldBe 10
  }

  test("multiple steps with varying record counts") {
    val strategy = createStrategy(List(100, 5000, 300, 2000))
    // Sum is 7400, so ceil(7400 / 1000) = 8 batches
    strategy.calculateNumBatches shouldBe 8
  }

  test("shouldContinue handles negative batch numbers gracefully") {
    val strategy = createStrategy(List(1000))
    // Batch numbers <= numBatches return true
    strategy.shouldContinue(0) shouldBe true // Edge case: 0 <= 1
    strategy.shouldContinue(-1) shouldBe true // Edge case: -1 <= 1
  }

  test("strategy with plan containing foreign keys") {
    val plan = Plan(
      name = "test_plan",
      sinkOptions = Some(io.github.datacatering.datacaterer.api.model.SinkOptions(
        foreignKeys = List()
      ))
    )
    val task = Task(name = "test_task", steps = List(
      Step(name = "step1", count = Count(records = Some(3000)))
    ))
    val executableTasks = List((TaskSummary("test_task", "test_ds"), task))
    val strategy = new CountBasedExecutionStrategy(plan, executableTasks, defaultGenerationConfig)

    strategy.calculateNumBatches shouldBe 3
  }

  test("consistent behavior across multiple shouldContinue calls") {
    val strategy = createStrategy(List(2000))

    // Multiple calls with same batch number should return same result
    strategy.shouldContinue(1) shouldBe true
    strategy.shouldContinue(1) shouldBe true
    strategy.shouldContinue(2) shouldBe true
    strategy.shouldContinue(2) shouldBe true
    strategy.shouldContinue(3) shouldBe false
    strategy.shouldContinue(3) shouldBe false
  }

  test("full execution cycle simulation") {
    val strategy = createStrategy(List(2500))
    val expectedBatches = 3

    strategy.calculateNumBatches shouldBe expectedBatches

    var currentBatch = 1
    var executedBatches = 0

    while (strategy.shouldContinue(currentBatch)) {
      strategy.onBatchStart(currentBatch)
      strategy.onBatchEnd(currentBatch, 1000)
      executedBatches += 1
      currentBatch += 1
    }

    executedBatches shouldBe expectedBatches
  }
}
