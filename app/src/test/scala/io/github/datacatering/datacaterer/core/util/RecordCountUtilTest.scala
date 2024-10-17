package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.{Count, GenerationConfig, Step, Task}
import io.github.datacatering.datacaterer.api.{CountBuilder, GeneratorBuilder}
import org.scalatest.funsuite.AnyFunSuite

class RecordCountUtilTest extends AnyFunSuite {

  private val generationConfig = GenerationConfig(100, None)

  test("Set number of batches to 0 when no tasks defined") {
    val result = RecordCountUtil.calculateNumBatches(List(), GenerationConfig())

    assert(result._1 == 0)
    assert(result._2.isEmpty)
  }

  test("Set number of batches to 1 when records from task is less than num records per batch from config") {
    val task = Task("my_task", List(Step("my_step", count = Count(Some(10)))))
    val result = RecordCountUtil.calculateNumBatches(List(task), generationConfig)

    assert(result._1 == 1)
    assert(result._2.size == 1)
    assert(result._2.head._1 == "my_task_my_step")
    assert(result._2.head._2.numTotalRecords == 10)
    assert(result._2.head._2.currentNumRecords == 0)
    assert(result._2.head._2.numRecordsPerBatch == 10)
  }

  test("Set number of batches to 2 when records from task is more than num records per batch from config") {
    val task = Task("my_task", List(Step("my_step", count = Count(Some(200)))))
    val result = RecordCountUtil.calculateNumBatches(List(task), generationConfig)

    assert(result._1 == 2)
    assert(result._2.size == 1)
    assert(result._2.head._1 == "my_task_my_step")
    assert(result._2.head._2.numTotalRecords == 200)
    assert(result._2.head._2.currentNumRecords == 0)
    assert(result._2.head._2.numRecordsPerBatch == 100)
  }

  test("Can calculate number of batches and number of records per batch foreach task when multiple tasks defined") {
    val task = Task("my_task", List(
      Step("my_step", count = Count(Some(100))),
      Step("my_step_2", count = Count(Some(100))),
    ))
    val result = RecordCountUtil.calculateNumBatches(List(task), generationConfig)

    assert(result._1 == 2)
    assert(result._2.size == 2)
    assert(result._2.forall(_._2.numTotalRecords == 100))
    assert(result._2.forall(_._2.currentNumRecords == 0))
    assert(result._2.forall(_._2.numRecordsPerBatch == 50))
  }

  test("Can calculate average record count if generator defined for count") {
    val task = Task("my_task", List(
      Step(
        "my_step",
        count = new CountBuilder().generator(new GeneratorBuilder().min(50).max(150)).count)
    ))
    val result = RecordCountUtil.calculateNumBatches(List(task), generationConfig)

    assert(result._1 == 1)
    assert(result._2.size == 1)
    assert(result._2.head._1 == "my_task_my_step")
    assert(result._2.head._2.numTotalRecords == 100)
    assert(result._2.head._2.currentNumRecords == 0)
    assert(result._2.head._2.numRecordsPerBatch == 100)
  }

  test("Can calculate record count based on per column count, task records per batch should be the pre-records per column count") {
    val task = Task("my_task", List(
      Step(
        "my_step",
        count = new CountBuilder().records(100).recordsPerColumn(10, "account_id").count
      )))
    val result = RecordCountUtil.calculateNumBatches(List(task), generationConfig)

    assert(result._1 == 10)
    assert(result._2.size == 1)
    assert(result._2.head._1 == "my_task_my_step")
    assert(result._2.head._2.numTotalRecords == 1000)
    assert(result._2.head._2.currentNumRecords == 0)
    assert(result._2.head._2.numRecordsPerBatch == 10)
  }

  test("Can calculate average record count based on per column generator count, task records per batch should be the pre-records per column count") {
    val task = Task("my_task", List(
      Step(
        "my_step",
        count = new CountBuilder()
          .recordsPerColumnGenerator(
            100,
            new GeneratorBuilder().min(5).max(15),
            "account_id"
          ).count
    )))
    val result = RecordCountUtil.calculateNumBatches(List(task), generationConfig)

    assert(result._1 == 10)
    assert(result._2.size == 1)
    assert(result._2.head._1 == "my_task_my_step")
    assert(result._2.head._2.numTotalRecords == 1000)
    assert(result._2.head._2.currentNumRecords == 0)
    assert(result._2.head._2.numRecordsPerBatch == 10)
  }

  test("Can override record count per step from config") {
    val generationConfig = GenerationConfig(100, Some(10))
    val task = Task("my_task", List(
      Step(
        "my_step",
        count = new CountBuilder().records(10000).count
    )))
    val result = RecordCountUtil.calculateNumBatches(List(task), generationConfig)

    assert(result._1 == 1)
    assert(result._2.size == 1)
    assert(result._2.head._1 == "my_task_my_step")
    assert(result._2.head._2.numTotalRecords == 10)
    assert(result._2.head._2.currentNumRecords == 0)
    assert(result._2.head._2.numRecordsPerBatch == 10)
  }

  test("Can override record count per step from config but still preserve per column count") {
    val generationConfig = GenerationConfig(100, Some(10))
    val task = Task("my_task", List(
      Step(
        "my_step",
        count = new CountBuilder().records(10000).recordsPerColumn(5, "account_id").count
    )))
    val result = RecordCountUtil.calculateNumBatches(List(task), generationConfig)

    assert(result._1 == 1)
    assert(result._2.size == 1)
    assert(result._2.head._1 == "my_task_my_step")
    assert(result._2.head._2.numTotalRecords == 50)
    assert(result._2.head._2.currentNumRecords == 0)
    assert(result._2.head._2.numRecordsPerBatch == 10)
  }
}
