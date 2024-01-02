package io.github.datacatering.datacaterer.core.util

import org.apache.spark.sql.SparkSession

class SparkProvider(master: String, config: Map[String, String]) {

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master(master)
      .appName("data-caterer")
      .config(config)
      .getOrCreate()
  }

}
