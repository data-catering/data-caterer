package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Task, TaskSummary}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class WeightedTaskSelectorTest extends AnyFunSuite with Matchers {

  test("No weights defined") {
    val tasks = List(
      (TaskSummary("task1", "ds1"), Task("task1")),
      (TaskSummary("task2", "ds2"), Task("task2"))
    )

    val selector = new WeightedTaskSelector(tasks)
    selector.hasWeights shouldBe false
  }

  test("All tasks have weights") {
    val tasks = List(
      (TaskSummary("task1", "ds1", weight = Some(7)), Task("task1")),
      (TaskSummary("task2", "ds2", weight = Some(3)), Task("task2"))
    )

    val selector = new WeightedTaskSelector(tasks)
    selector.hasWeights shouldBe true
  }

  test("Select task - verify weight distribution") {
    val tasks = List(
      (TaskSummary("task1", "ds1", weight = Some(7)), Task("task1")),
      (TaskSummary("task2", "ds2", weight = Some(3)), Task("task2"))
    )

    val selector = new WeightedTaskSelector(tasks)

    // Select many tasks and verify distribution
    val selections = (1 to 1000).map(_ => selector.selectTask())
    val task1Count = selections.count(_._1.name == "task1")
    val task2Count = selections.count(_._1.name == "task2")

    // Should be approximately 70% task1, 30% task2 (with some variance)
    val task1Percentage = task1Count.toDouble / 1000
    val task2Percentage = task2Count.toDouble / 1000

    task1Percentage should be(0.7 +- 0.1) // 70% ± 10%
    task2Percentage should be(0.3 +- 0.1) // 30% ± 10%
  }

  test("Select multiple tasks") {
    val tasks = List(
      (TaskSummary("task1", "ds1", weight = Some(5)), Task("task1")),
      (TaskSummary("task2", "ds2", weight = Some(5)), Task("task2"))
    )

    val selector = new WeightedTaskSelector(tasks)
    val selected = selector.selectTasks(100)

    selected should have size 100
    // Should have roughly 50/50 split (with variance)
    val task1Count = selected.count(_._1.name == "task1")
    task1Count should be(50 +- 20)
  }

  test("Get expected distribution") {
    val tasks = List(
      (TaskSummary("task1", "ds1", weight = Some(7)), Task("task1")),
      (TaskSummary("task2", "ds2", weight = Some(3)), Task("task2"))
    )

    val selector = new WeightedTaskSelector(tasks)
    val distribution = selector.getExpectedDistribution

    distribution("task1") shouldBe 0.7
    distribution("task2") shouldBe 0.3
  }

  test("Get expected counts") {
    val tasks = List(
      (TaskSummary("read", "ds1", weight = Some(7)), Task("read")),
      (TaskSummary("write", "ds2", weight = Some(3)), Task("write"))
    )

    val selector = new WeightedTaskSelector(tasks)
    val counts = selector.getExpectedCounts(100)

    counts("read") shouldBe 70
    counts("write") shouldBe 30
  }

  test("Three-way weight split") {
    val tasks = List(
      (TaskSummary("task1", "ds1", weight = Some(5)), Task("task1")),
      (TaskSummary("task2", "ds2", weight = Some(3)), Task("task2")),
      (TaskSummary("task3", "ds3", weight = Some(2)), Task("task3"))
    )

    val selector = new WeightedTaskSelector(tasks)
    val distribution = selector.getExpectedDistribution

    distribution("task1") shouldBe 0.5
    distribution("task2") shouldBe 0.3
    distribution("task3") shouldBe 0.2
  }

  test("Validate weight configuration - all positive") {
    val tasks = List(
      (TaskSummary("task1", "ds1", weight = Some(7)), Task("task1")),
      (TaskSummary("task2", "ds2", weight = Some(3)), Task("task2"))
    )

    val selector = new WeightedTaskSelector(tasks)
    selector.validate() shouldBe empty
  }

  test("Validate weight configuration - negative weight") {
    val tasks = List(
      (TaskSummary("task1", "ds1", weight = Some(-5)), Task("task1"))
    )

    val selector = new WeightedTaskSelector(tasks)
    val errors = selector.validate()

    errors should not be empty
    errors.head should include("invalid weight")
  }

  test("Validate weight configuration - zero weight") {
    val tasks = List(
      (TaskSummary("task1", "ds1", weight = Some(0)), Task("task1"))
    )

    val selector = new WeightedTaskSelector(tasks)
    val errors = selector.validate()

    errors should not be empty
    errors.head should include("invalid weight")
  }

  test("Get summary") {
    val tasks = List(
      (TaskSummary("read", "ds1", weight = Some(7)), Task("read")),
      (TaskSummary("write", "ds2", weight = Some(3)), Task("write"))
    )

    val selector = new WeightedTaskSelector(tasks)
    val summary = selector.getSummary

    summary should include("Weighted execution")
    summary should include("read=70%")
    summary should include("write=30%")
  }

  test("Get summary - no weights") {
    val tasks = List(
      (TaskSummary("task1", "ds1"), Task("task1"))
    )

    val selector = new WeightedTaskSelector(tasks)
    val summary = selector.getSummary

    summary should include("No weighted execution")
  }

  test("hasWeightedTasks utility - true") {
    val tasks = List(
      (TaskSummary("task1", "ds1", weight = Some(5)), Task("task1"))
    )
    WeightedTaskSelector.hasWeightedTasks(tasks) shouldBe true
  }

  test("hasWeightedTasks utility - false") {
    val tasks = List(
      (TaskSummary("task1", "ds1"), Task("task1"))
    )
    WeightedTaskSelector.hasWeightedTasks(tasks) shouldBe false
  }

  test("separateTasks utility") {
    val weightedTask = (TaskSummary("weighted", "ds1", weight = Some(5)), Task("weighted"))
    val normalTask = (TaskSummary("normal", "ds2"), Task("normal"))

    val tasks = List(weightedTask, normalTask)
    val (weighted, nonWeighted) = WeightedTaskSelector.separateTasks(tasks)

    weighted should have size 1
    weighted.head._1.name shouldBe "weighted"

    nonWeighted should have size 1
    nonWeighted.head._1.name shouldBe "normal"
  }

  test("Select task fails when no weights") {
    val tasks = List(
      (TaskSummary("task1", "ds1"), Task("task1"))
    )

    val selector = new WeightedTaskSelector(tasks)

    assertThrows[IllegalStateException] {
      selector.selectTask()
    }
  }
}
