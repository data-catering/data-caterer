package io.github.datacatering.datacaterer.core.generator.delete

import io.github.datacatering.datacaterer.api.model.Constants.{FORMAT, PATH}
import io.github.datacatering.datacaterer.api.model.{ForeignKey, ForeignKeyRelation, Plan, SinkOptions, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.spark.sql.{Encoder, Encoders, SaveMode}
import org.apache.spark.sql.delta.implicits.stringEncoder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

class DeleteRecordProcessorTest extends SparkSuite with MockFactory with Matchers {

  private implicit val encoder: Encoder[SampleData] = Encoders.kryo[SampleData]
  private val recordTrackingFolderPath: String = "/tmp/recordTracking"
  private val dataSource1Path = s"$recordTrackingFolderPath/dataSource1"
  private val dataSource2Path = s"$recordTrackingFolderPath/dataSource2"
  private val dataSource1RecordTrackingPath = s"$recordTrackingFolderPath/default_plan/csv/dataSource1/tmp/recordTracking/dataSource1"
  private val dataSource2RecordTrackingPath = s"$recordTrackingFolderPath/default_plan/csv/dataSource2/tmp/recordTracking/dataSource2"
  private val connectionConfigsByName: Map[String, Map[String, String]] = Map(
    "dataSource1" -> Map(FORMAT -> "csv", PATH -> dataSource1Path, "header" -> "true"),
    "dataSource2" -> Map(FORMAT -> "csv", PATH -> dataSource2Path, "header" -> "true")
  )
  private val sampleData = Seq(
    SampleData("1", "John Doe", 30),
    SampleData("2", "Jane Smith", 25),
    SampleData("3", "Bob Johnson", 40)
  )

  private val processor = new DeleteRecordProcessor(connectionConfigsByName, recordTrackingFolderPath)

  test("deleteGeneratedRecords should delete records with foreign keys in reverse order") {
    createSampleCsv(dataSource1Path)
    createSampleCsv(dataSource2Path)
    createSampleParquet(dataSource1RecordTrackingPath)
    createSampleParquet(dataSource2RecordTrackingPath)
    val plan = Plan(sinkOptions = Some(SinkOptions(foreignKeys = List(ForeignKey(
      source = ForeignKeyRelation("dataSource1", "step1"),
      delete = List(ForeignKeyRelation("dataSource2", "step2"))
    )))))
    val stepsByName = Map("step1" -> Step(), "step2" -> Step())
    val summaryWithTask = List(
      (TaskSummary("my-task", "dataSource1"), Task("my-task", List(Step("step1")))),
      (TaskSummary("my-task2", "dataSource2"), Task("my-task2", List(Step("step2")))),
    )

    processor.deleteGeneratedRecords(plan, stepsByName, summaryWithTask)

    // Verify the deletion logic
    val resultDataSource1 = sparkSession.read.option("header", "true").csv(dataSource1Path)
    resultDataSource1.count() shouldBe 0
    val resultDataSource2 = sparkSession.read.option("header", "true").csv(dataSource2Path)
    resultDataSource2.count() shouldBe 0
  }

  private def createSampleParquet(path: String): Unit = {
    sparkSession.createDataFrame(sampleData).write.mode(SaveMode.Overwrite).parquet(path)
  }

  private def createSampleCsv(path: String): Unit = {
    sparkSession.createDataFrame(sampleData).write.mode(SaveMode.Overwrite).option("header", "true").csv(path)
  }
}

case class SampleData(id: String, name: String, age: Int)
