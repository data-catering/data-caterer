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
      //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //used for hudi
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
//      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
//      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
//      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
//      .config("spark.sql.catalog.local.type", "hadoop")
//      .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg/warehouse")
//      .config("spark.sql.defaultCatalog", "local")
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
