package io.github.datacatering.datacaterer.core.util

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

trait SparkSuite extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit lazy val sparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("spark tests")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override protected def beforeAll(): Unit = {
    sparkSession
  }

  override protected def afterAll(): Unit = {
    sparkSession.close()
  }

  override protected def afterEach(): Unit = {
    sparkSession.catalog.clearCache()
  }

  def getSparkSession: SparkSession = sparkSession
}
