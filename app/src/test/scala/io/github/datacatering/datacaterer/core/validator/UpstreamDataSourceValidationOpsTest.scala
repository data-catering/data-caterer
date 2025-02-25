package io.github.datacatering.datacaterer.core.validator

import io.github.datacatering.datacaterer.api.model.Constants.CSV
import io.github.datacatering.datacaterer.api.{ConnectionConfigWithTaskBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.api.model.UpstreamDataSourceValidation
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.spark.sql.SaveMode
import org.scalatest.matchers.should.Matchers

class UpstreamDataSourceValidationOpsTest extends SparkSuite with Matchers {

  test("UpstreamDataSourceValidationOps can validate dataframe with upstream data validation") {
    val df = sparkSession.createDataFrame(Seq(
      (1, "foo", 1.0),
      (2, "bar", 2.0)
    )).toDF("id", "name", "value").repartition(1)
    val upstreamDf = sparkSession.createDataFrame(Seq(
      (1, "foo1", 3.0),
      (2, "bar1", 4.0)
    )).toDF("id", "name", "value").repartition(1)
    val csvPath = "/tmp/data-caterer-upstream-data-test-csv-1"
    val recordTrackingPath = "/tmp/data-caterer-upstream-data-test-record-tracking"
    upstreamDf.write.option("header", "true").mode(SaveMode.Overwrite).csv(csvPath)
    val upstreamDataSource = ConnectionConfigWithTaskBuilder().file("my-csv", CSV, csvPath, Map("header" -> "true"))
    val upstreamDataValidation = UpstreamDataSourceValidation(
      List(ValidationBuilder().unique("id")),
      upstreamDataSource,
      Map("path" -> csvPath, "format" -> CSV, "header" -> "true"),
      List("id")
    )

    val result = new UpstreamDataSourceValidationOps(upstreamDataValidation, recordTrackingPath).validate(df, 2, 0)

    result.length shouldBe 1
    result.head.sampleErrorValues shouldBe None
    result.head.isSuccess shouldBe true
    result.head.total shouldBe 2
  }

  test("UpstreamDataSourceValidationOps can validate dataframe with upstream data validation with join expression") {
    val df = sparkSession.createDataFrame(Seq(
      (1, "foo", 1.0),
      (2, "bar", 2.0)
    )).toDF("id", "name", "value").repartition(1)
    val upstreamDf = sparkSession.createDataFrame(Seq(
      (1, "foo1", 3.0),
      (2, "bar1", 4.0)
    )).toDF("id", "name", "value").repartition(1)
    val csvPath = "/tmp/data-caterer-upstream-data-test-csv-2"
    val recordTrackingPath = "/tmp/data-caterer-upstream-data-test-record-tracking"
    upstreamDf.write.option("header", "true").mode(SaveMode.Overwrite).csv(csvPath)
    val upstreamDataSource = ConnectionConfigWithTaskBuilder().file("my_csv", CSV, csvPath, Map("header" -> "true"))
    val upstreamDataValidation = UpstreamDataSourceValidation(
      List(ValidationBuilder().unique("id")),
      upstreamDataSource,
      Map("path" -> csvPath, "format" -> CSV, "header" -> "true"),
      List("expr:SUBSTRING(my_csv_name, 1, 3) == name")
    )

    val result = new UpstreamDataSourceValidationOps(upstreamDataValidation, recordTrackingPath).validate(df, 2, 0)

    result.length shouldBe 1
    result.head.sampleErrorValues shouldBe None
    result.head.isSuccess shouldBe true
    result.head.total shouldBe 2
  }

}
