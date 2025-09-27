package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Constants.{DELTA, FORMAT, ICEBERG, PATH, SAVE_MODE, TABLE}
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, FoldersConfig, MetadataConfig, Step}
import io.github.datacatering.datacaterer.core.util.{SparkSuite, Transaction}

import java.io.File
import java.sql.Date
import java.time.LocalDateTime
import scala.reflect.io.Directory

class SinkFactoryTest extends SparkSuite {

  private val sampleData = Seq(
    Transaction("acc123", "peter", "txn1", Date.valueOf("2020-01-01"), 10.0),
    Transaction("acc123", "peter", "txn2", Date.valueOf("2020-01-01"), 50.0),
    Transaction("acc123", "peter", "txn3", Date.valueOf("2020-01-01"), 200.0),
    Transaction("acc123", "peter", "txn4", Date.valueOf("2020-01-01"), 500.0)
  )
  private val df = sparkSession.createDataFrame(sampleData)

  test("Can save data in Iceberg format") {
    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> ICEBERG, TABLE -> "local.account.transactions"))
    val res = sinkFactory.pushToSink(df, "iceberg-data-source", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(4)(res.count)
    assertResult(ICEBERG)(res.format)
    assert(res.exception.isEmpty)
  }

  test("Can save data in Delta Lake format") {
    val path = "/tmp/delta-test"
    new Directory(new File(path)).deleteRecursively()
    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val step = Step(options = Map(FORMAT -> DELTA, PATH -> path))
    val res = sinkFactory.pushToSink(df, "delta-data-source", step, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(4)(res.count)
    assertResult(DELTA)(res.format)
    assert(res.exception.isEmpty)
  }

  test("Should provide helpful error message when format is missing from step options") {
    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val stepWithoutFormat = Step(options = Map(PATH -> "/tmp/test-path", SAVE_MODE -> "overwrite"))
    
    val exception = intercept[IllegalArgumentException] {
      sinkFactory.pushToSink(df, "test-data-source", stepWithoutFormat, LocalDateTime.now())
    }
    
    assert(exception.getMessage.contains("No format specified for data source: test-data-source"))
    assert(exception.getMessage.contains("step: "))
    assert(exception.getMessage.contains("Available options: path, saveMode"))
  }

  ignore("Can overwrite existing Iceberg data") {
    sparkSession.sql("DELETE FROM local.account.transactions_overwrite").count()
    val sinkFactory = new SinkFactory(FlagsConfig(), MetadataConfig(), FoldersConfig())
    val options = Map(FORMAT -> ICEBERG, TABLE -> "local.account.transactions_overwrite")
    val step = Step(options = options)
    val existingDataRes = sinkFactory.pushToSink(df, "iceberg-data-source", step, LocalDateTime.now())

    assert(existingDataRes.isSuccess)
    assertResult(4)(existingDataRes.count)
    assertResult(ICEBERG)(existingDataRes.format)
    assert(existingDataRes.exception.isEmpty)
    assertResult(4)(sparkSession.table("local.account.transactions_overwrite").count())

    val newStep = Step(options = options ++ Map(SAVE_MODE -> "overwrite"))
    val res = sinkFactory.pushToSink(df, "iceberg-data-source", newStep, LocalDateTime.now())

    assert(res.isSuccess)
    assertResult(4)(res.count)
    assertResult(ICEBERG)(res.format)
    assert(res.exception.isEmpty)
    assertResult(4)(sparkSession.table("local.account.transactions_overwrite").count())
  }
}
