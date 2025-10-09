package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.FORMAT
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, FlagsConfig, FoldersConfig}
import io.github.datacatering.datacaterer.core.util.SparkSuite

import java.io.File
import scala.reflect.io.Directory

class DataGeneratorProcessorTest extends SparkSuite {

  test("Can parse plan and tasks, then execute data generation") {
    val basePath = "src/test/resources/sample/data"
    val config = DataCatererConfiguration(
      flagsConfig = FlagsConfig(false, true, false, false, enableValidation = false),
      foldersConfig = FoldersConfig("sample/plan/simple-json-plan.yaml", "sample/task", basePath, recordTrackingFolderPath = s"$basePath/recordTracking"),
      connectionConfigByName = Map("json" -> Map(FORMAT -> "json"))
    )
    val dataGeneratorProcessor = new DataGeneratorProcessor(config)

    dataGeneratorProcessor.generateData()

    val generatedData = sparkSession.read
      .json(s"$basePath/generated/json/account-gen")
    val generatedCount = generatedData.count()
    assert(generatedCount > 0)
    new Directory(new File(basePath)).deleteRecursively()
  }

}
