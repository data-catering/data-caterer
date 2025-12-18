package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Count, GenerationConfig, Plan, Step, Task, TaskSummary}
import org.scalatest.funsuite.AnyFunSuite

class ExecutionStrategyFactoryTest extends AnyFunSuite {

  val generationConfig = GenerationConfig(numRecordsPerBatch = 1000)

  test("Create count-based strategy for traditional record count") {
    val plan = Plan(name = "test_plan")
    val task = Task(name = "test_task", steps = List(
      Step(name = "step1", count = Count(records = Some(1000)))
    ))
    val executableTasks = List((TaskSummary("test_task", "test_ds"), task))

    val strategy = ExecutionStrategyFactory.create(plan, executableTasks, generationConfig)

    assert(strategy.isInstanceOf[CountBasedExecutionStrategy])
  }

  test("Create duration-based strategy when duration is specified") {
    val plan = Plan(name = "test_plan")
    val task = Task(name = "test_task", steps = List(
      Step(name = "step1", count = Count(
        records = None,
        duration = Some("5m"),
        rate = Some(100),
        rateUnit = Some("1s")
      ))
    ))
    val executableTasks = List((TaskSummary("test_task", "test_ds"), task))

    val strategy = ExecutionStrategyFactory.create(plan, executableTasks, generationConfig)

    assert(strategy.isInstanceOf[DurationBasedExecutionStrategy])
  }

  test("Create count-based strategy when no duration or pattern specified") {
    val plan = Plan(name = "test_plan")
    val task = Task(name = "test_task", steps = List(
      Step(name = "step1", count = Count(records = Some(500)))
    ))
    val executableTasks = List((TaskSummary("test_task", "test_ds"), task))

    val strategy = ExecutionStrategyFactory.create(plan, executableTasks, generationConfig)

    assert(strategy.isInstanceOf[CountBasedExecutionStrategy])
  }

  test("Throw exception when both duration and pattern are specified") {
    val plan = Plan(name = "test_plan")
    val task = Task(name = "test_task", steps = List(
      Step(name = "step1", count = Count(
        duration = Some("5m"),
        pattern = Some(io.github.datacatering.datacaterer.api.model.LoadPattern("ramp"))
      ))
    ))
    val executableTasks = List((TaskSummary("test_task", "test_ds"), task))

    assertThrows[IllegalArgumentException] {
      ExecutionStrategyFactory.create(plan, executableTasks, generationConfig)
    }
  }
}
