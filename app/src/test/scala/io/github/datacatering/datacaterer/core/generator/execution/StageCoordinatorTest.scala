package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Task, TaskSummary}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StageCoordinatorTest extends AnyFunSuite with Matchers {

  test("Single stage (no stage defined)") {
    val tasks = List(
      (TaskSummary("task1", "ds1"), Task("task1")),
      (TaskSummary("task2", "ds2"), Task("task2"))
    )

    val coordinator = new StageCoordinator(tasks)

    coordinator.hasMultipleStages shouldBe false
    coordinator.availableStages should contain only "execution"
    coordinator.getStageTaskCount("execution") shouldBe 2
  }

  test("Multiple stages defined") {
    val tasks = List(
      (TaskSummary("setup_task", "ds1", stage = Some("setup")), Task("setup_task")),
      (TaskSummary("exec_task", "ds2", stage = Some("execution")), Task("exec_task")),
      (TaskSummary("teardown_task", "ds3", stage = Some("teardown")), Task("teardown_task"))
    )

    val coordinator = new StageCoordinator(tasks)

    coordinator.hasMultipleStages shouldBe true
    coordinator.availableStages should contain allOf("setup", "execution", "teardown")
    coordinator.hasSetupStage shouldBe true
    coordinator.hasExecutionStage shouldBe true
    coordinator.hasTeardownStage shouldBe true
  }

  test("Get tasks for specific stage") {
    val setupTask = (TaskSummary("setup_task", "ds1", stage = Some("setup")), Task("setup_task"))
    val execTask = (TaskSummary("exec_task", "ds2", stage = Some("execution")), Task("exec_task"))

    val tasks = List(setupTask, execTask)
    val coordinator = new StageCoordinator(tasks)

    val setupTasks = coordinator.getTasksForStage("setup")
    setupTasks should have size 1
    setupTasks.head._1.name shouldBe "setup_task"

    val executionTasks = coordinator.getTasksForStage("execution")
    executionTasks should have size 1
    executionTasks.head._1.name shouldBe "exec_task"
  }

  test("Get tasks for non-existent stage") {
    val tasks = List(
      (TaskSummary("task1", "ds1", stage = Some("execution")), Task("task1"))
    )
    val coordinator = new StageCoordinator(tasks)

    coordinator.getTasksForStage("setup") shouldBe empty
  }

  test("Tasks in stage execution order") {
    val setupTask = (TaskSummary("setup_task", "ds1", stage = Some("setup")), Task("setup_task"))
    val execTask = (TaskSummary("exec_task", "ds2", stage = Some("execution")), Task("exec_task"))
    val teardownTask = (TaskSummary("teardown_task", "ds3", stage = Some("teardown")), Task("teardown_task"))

    // Add in random order
    val tasks = List(execTask, teardownTask, setupTask)
    val coordinator = new StageCoordinator(tasks)

    val orderedTasks = coordinator.getTasksInStageOrder
    orderedTasks.map(_._1) shouldBe List("setup", "execution", "teardown")
  }

  test("Check stage existence") {
    val tasks = List(
      (TaskSummary("setup_task", "ds1", stage = Some("setup")), Task("setup_task")),
      (TaskSummary("exec_task", "ds2", stage = Some("execution")), Task("exec_task"))
    )
    val coordinator = new StageCoordinator(tasks)

    coordinator.hasStage("setup") shouldBe true
    coordinator.hasStage("execution") shouldBe true
    coordinator.hasStage("teardown") shouldBe false
  }

  test("Get stage task count") {
    val tasks = List(
      (TaskSummary("setup1", "ds1", stage = Some("setup")), Task("setup1")),
      (TaskSummary("setup2", "ds2", stage = Some("setup")), Task("setup2")),
      (TaskSummary("exec1", "ds3", stage = Some("execution")), Task("exec1"))
    )
    val coordinator = new StageCoordinator(tasks)

    coordinator.getStageTaskCount("setup") shouldBe 2
    coordinator.getStageTaskCount("execution") shouldBe 1
    coordinator.getStageTaskCount("teardown") shouldBe 0
  }

  test("Get stage summary - multi-stage") {
    val tasks = List(
      (TaskSummary("setup_task", "ds1", stage = Some("setup")), Task("setup_task")),
      (TaskSummary("exec_task", "ds2", stage = Some("execution")), Task("exec_task")),
      (TaskSummary("teardown_task", "ds3", stage = Some("teardown")), Task("teardown_task"))
    )
    val coordinator = new StageCoordinator(tasks)

    val summary = coordinator.getStageSummary
    summary should include("Multi-stage execution")
    summary should include("setup=1")
    summary should include("execution=1")
    summary should include("teardown=1")
  }

  test("Get stage summary - single stage") {
    val tasks = List(
      (TaskSummary("task1", "ds1"), Task("task1")),
      (TaskSummary("task2", "ds2"), Task("task2"))
    )
    val coordinator = new StageCoordinator(tasks)

    val summary = coordinator.getStageSummary
    summary should include("Single-stage execution")
    summary should include("2 task")
  }

  test("Validate stage configuration - valid") {
    val tasks = List(
      (TaskSummary("setup_task", "ds1", stage = Some("setup")), Task("setup_task")),
      (TaskSummary("exec_task", "ds2", stage = Some("execution")), Task("exec_task"))
    )
    val coordinator = new StageCoordinator(tasks)

    coordinator.validate() shouldBe empty
  }

  test("Validate stage configuration - unknown stage") {
    val tasks = List(
      (TaskSummary("invalid_task", "ds1", stage = Some("invalid_stage")), Task("invalid_task"))
    )
    val coordinator = new StageCoordinator(tasks)

    val errors = coordinator.validate()
    errors should not be empty
    errors.head should include("Unknown stage")
  }

  test("isMultiStageExecution utility") {
    val singleStageTasks = List(
      (TaskSummary("task1", "ds1"), Task("task1"))
    )
    StageCoordinator.isMultiStageExecution(singleStageTasks) shouldBe false

    val multiStageTasks = List(
      (TaskSummary("task1", "ds1", stage = Some("setup")), Task("task1"))
    )
    StageCoordinator.isMultiStageExecution(multiStageTasks) shouldBe true
  }

  test("Default stage order") {
    StageCoordinator.DEFAULT_STAGE_ORDER shouldBe List("setup", "execution", "teardown")
  }

  test("Mixed staged and non-staged tasks") {
    val tasks = List(
      (TaskSummary("setup_task", "ds1", stage = Some("setup")), Task("setup_task")),
      (TaskSummary("normal_task", "ds2"), Task("normal_task"))
    )
    val coordinator = new StageCoordinator(tasks)

    coordinator.hasMultipleStages shouldBe true
    coordinator.getStageTaskCount("setup") shouldBe 1
    coordinator.getStageTaskCount("execution") shouldBe 1 // Non-staged task defaults to execution
  }
}
