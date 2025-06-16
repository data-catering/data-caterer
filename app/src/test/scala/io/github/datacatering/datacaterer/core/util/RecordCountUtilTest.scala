package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.{MAXIMUM, MINIMUM}
import io.github.datacatering.datacaterer.api.model.{Count, ForeignKey, ForeignKeyRelation, GenerationConfig, PerFieldCount, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.api.{CountBuilder, GeneratorBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class RecordCountUtilTest extends AnyFunSuite {

  private val generationConfig = GenerationConfig(100, None)
  private val taskSummary = TaskSummary("my_task_sum", "my_data_source")

  test("Set number of batches to 0 when no tasks defined") {
    val result = RecordCountUtil.calculateNumBatches(List(), List(), GenerationConfig())

    assertResult(0)(result._1)
    assert(result._2.isEmpty)
  }

  test("Set number of batches to 1 when records from task is less than num records per batch from config") {
    val task = (taskSummary, Task("my_task", List(Step("my_step", count = Count(Some(10))))))
    val result = RecordCountUtil.calculateNumBatches(List(), List(task), generationConfig)

    assertResult(1)(result._1)
    assertResult(1)(result._2.size)
    assertResult("my_task_my_step")(result._2.head._1)
    assertResult(10)(result._2.head._2.numTotalRecords)
    assertResult(0)(result._2.head._2.currentNumRecords)
    assertResult(10)(result._2.head._2.numRecordsPerBatch)
  }

  test("Set number of batches to 2 when records from task is more than num records per batch from config") {
    val task = (taskSummary, Task("my_task", List(Step("my_step", count = Count(Some(200))))))
    val result = RecordCountUtil.calculateNumBatches(List(), List(task), generationConfig)

    assertResult(2)(result._1)
    assertResult(1)(result._2.size)
    assertResult("my_task_my_step")(result._2.head._1)
    assertResult(200)(result._2.head._2.numTotalRecords)
    assertResult(0)(result._2.head._2.currentNumRecords)
    assertResult(100)(result._2.head._2.numRecordsPerBatch)
  }

  test("Can calculate number of batches and number of records per batch foreach task when multiple tasks defined") {
    val task = (taskSummary, Task("my_task", List(
      Step("my_step", count = Count(Some(100))),
      Step("my_step_2", count = Count(Some(100))),
    )))
    val result = RecordCountUtil.calculateNumBatches(List(), List(task), generationConfig)

    assertResult(2)(result._1)
    assertResult(2)(result._2.size)
    assert(result._2.forall(_._2.numTotalRecords == 100))
    assert(result._2.forall(_._2.currentNumRecords == 0))
    assert(result._2.forall(_._2.numRecordsPerBatch == 50))
  }

  test("Can calculate average record count if generator defined for count") {
    val task = (taskSummary, Task("my_task", List(
      Step(
        "my_step",
        count = new CountBuilder().generator(new GeneratorBuilder().min(50).max(150)).count)
    )))
    val result = RecordCountUtil.calculateNumBatches(List(), List(task), generationConfig)

    assertResult(1)(result._1)
    assertResult(1)(result._2.size)
    assertResult("my_task_my_step")(result._2.head._1)
    assertResult(100)(result._2.head._2.numTotalRecords)
    assertResult(0)(result._2.head._2.currentNumRecords)
    assertResult(100)(result._2.head._2.numRecordsPerBatch)
  }

  test("Can calculate record count based on per field count, task records per batch should be the pre-records per field count") {
    val task = (taskSummary, Task("my_task", List(
      Step(
        "my_step",
        count = new CountBuilder().records(100).recordsPerField(10, "account_id").count
      ))))
    val result = RecordCountUtil.calculateNumBatches(List(), List(task), generationConfig)

    assertResult(10)(result._1)
    assertResult(1)(result._2.size)
    assertResult("my_task_my_step")(result._2.head._1)
    assertResult(1000)(result._2.head._2.numTotalRecords)
    assertResult(0)(result._2.head._2.currentNumRecords)
    assertResult(10)(result._2.head._2.numRecordsPerBatch)
  }

  test("Can calculate average record count based on per field generator count, task records per batch should be the pre-records per field count") {
    val task = (taskSummary, Task("my_task", List(
      Step(
        "my_step",
        count = new CountBuilder()
          .recordsPerFieldGenerator(
            100,
            new GeneratorBuilder().min(5).max(15),
            "account_id"
          ).count
    ))))
    val result = RecordCountUtil.calculateNumBatches(List(), List(task), generationConfig)

    assertResult(10)(result._1)
    assertResult(1)(result._2.size)
    assertResult("my_task_my_step")(result._2.head._1)
    assertResult(1000)(result._2.head._2.numTotalRecords)
    assertResult(0)(result._2.head._2.currentNumRecords)
    assertResult(10)(result._2.head._2.numRecordsPerBatch)
  }

  test("Can override record count per step from config") {
    val generationConfig = GenerationConfig(100, Some(10))
    val task = (taskSummary, Task("my_task", List(
      Step(
        "my_step",
        count = new CountBuilder().records(10000).count
    ))))
    val result = RecordCountUtil.calculateNumBatches(List(), List(task), generationConfig)

    assertResult(1)(result._1)
    assertResult(1)(result._2.size)
    assertResult("my_task_my_step")(result._2.head._1)
    assertResult(10)(result._2.head._2.numTotalRecords)
    assertResult(0)(result._2.head._2.currentNumRecords)
    assertResult(10)(result._2.head._2.numRecordsPerBatch)
  }

  test("Can override record count per step from config but still preserve per field count") {
    val generationConfig = GenerationConfig(100, Some(10))
    val task = (taskSummary, Task("my_task", List(
      Step(
        "my_step",
        count = new CountBuilder().records(10000).recordsPerField(5, "account_id").count
    ))))
    val result = RecordCountUtil.calculateNumBatches(List(), List(task), generationConfig)

    assertResult(1)(result._1)
    assertResult(1)(result._2.size)
    assertResult("my_task_my_step")(result._2.head._1)
    assertResult(50)(result._2.head._2.numTotalRecords)
    assertResult(0)(result._2.head._2.currentNumRecords)
    assertResult(10)(result._2.head._2.numRecordsPerBatch)
  }

  test("Can return correct count per step when numRecordsPerStep is defined in generationConfig") {
    val tasks = List((taskSummary, Task(name = "task1", steps = List(Step(name = "step1", count = Count(records = Some(500)))))))
    val generationConfig = GenerationConfig(numRecordsPerStep = Some(1000))
    val countPerStep = RecordCountUtil.getCountPerStep(List(), tasks, generationConfig)
    countPerStep should contain ("task1_step1" -> 1000)
  }

  test("Can return correct count per step when numRecordsPerStep is not defined in generationConfig") {
    val tasks = List((taskSummary, Task(name = "task1", steps = List(Step(name = "step1", count = Count(records = Some(500)))))))
    val generationConfig = GenerationConfig()
    val countPerStep = RecordCountUtil.getCountPerStep(List(), tasks, generationConfig)
    countPerStep should contain ("task1_step1" -> 500)
  }

  test("Can return correct count per step when records set with min and max") {
    val count = Count(None, None, Map(MINIMUM -> "1", MAXIMUM -> "9"))
    val tasks = List((taskSummary, Task(name = "task1", steps = List(Step(name = "step1", count = count)))))
    val generationConfig = GenerationConfig()
    val countPerStep = RecordCountUtil.getCountPerStep(List(), tasks, generationConfig)
    countPerStep should contain ("task1_step1" -> 5)
  }

  test("Can return correct count per step when records set with perField records") {
    val count = Count(Some(10), Some(PerFieldCount(List("account_id"), Some(5))))
    val tasks = List((taskSummary, Task(name = "task1", steps = List(Step(name = "step1", count = count)))))
    val generationConfig = GenerationConfig()
    val countPerStep = RecordCountUtil.getCountPerStep(List(), tasks, generationConfig)
    countPerStep should contain ("task1_step1" -> 50)
  }

  test("Can return correct count per step when records set with perField min and max") {
    val count = Count(Some(10), Some(PerFieldCount(List("account_id"), options = Map(MINIMUM -> "1", MAXIMUM -> "9"))))
    val tasks = List((taskSummary, Task(name = "task1", steps = List(Step(name = "step1", count = count)))))
    val generationConfig = GenerationConfig()
    val countPerStep = RecordCountUtil.getCountPerStep(List(), tasks, generationConfig)
    countPerStep should contain ("task1_step1" -> 50)
  }

  test("Can return correct count per step when records min max with perField min and max") {
    val count = Count(None, Some(PerFieldCount(List("account_id"), options = Map(MINIMUM -> "1", MAXIMUM -> "9"))), Map(MINIMUM -> "1", MAXIMUM -> "3"))
    val tasks = List((taskSummary, Task(name = "task1", steps = List(Step(name = "step1", count = count)))))
    val generationConfig = GenerationConfig()
    val countPerStep = RecordCountUtil.getCountPerStep(List(), tasks, generationConfig)
    countPerStep should contain ("task1_step1" -> 10)
  }

  test("Can return empty list when tasks list is empty") {
    val tasks = List.empty
    val generationConfig = GenerationConfig()
    val countPerStep = RecordCountUtil.getCountPerStep(List(), tasks, generationConfig)
    countPerStep shouldBe Matchers.empty
  }

  test("Can return correct count per step when foreign key is defined for generation") {
    val task1 = (TaskSummary("my_task_sum", "my_data_source"), Task(name = "task1", steps = List(Step(name = "step1", count = Count(Some(10))))))
    val task2 = (TaskSummary("my_task_sum1", "my_other_source"), Task(name = "task2", steps = List(Step(name = "step1", count = Count(Some(5))))))
    val foreignKeys = List(ForeignKey(source = ForeignKeyRelation("my_data_source", "step1"), generate = List(ForeignKeyRelation("my_other_source", "step1"))))
    val tasks = List(task1, task2)
    val generationConfig = GenerationConfig()
    val countPerStep = RecordCountUtil.getCountPerStep(foreignKeys, tasks, generationConfig)
    countPerStep should contain ("task1_step1" -> 10)
    countPerStep should contain ("task2_step1" -> 10)
  }

  test("Can return correct count per step when foreign key is defined for generation with records per step for generation step") {
    val task1 = (TaskSummary("my_task_sum", "my_data_source"), Task(name = "task1", steps = List(Step(name = "step1", count = Count(Some(10))))))
    val task2 = (TaskSummary("my_task_sum1", "my_other_source"), Task(name = "task2", steps = List(Step(name = "step1", count = Count(perField = Some(PerFieldCount(List("account_id"), Some(5))))))))
    val foreignKeys = List(ForeignKey(source = ForeignKeyRelation("my_data_source", "step1"), generate = List(ForeignKeyRelation("my_other_source", "step1"))))
    val tasks = List(task1, task2)
    val generationConfig = GenerationConfig()
    val countPerStep = RecordCountUtil.getCountPerStep(foreignKeys, tasks, generationConfig)
    countPerStep should contain ("task1_step1" -> 10)
    countPerStep should contain ("task2_step1" -> 50)
  }
}
