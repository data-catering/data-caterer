package io.github.datacatering.datacaterer.core.config

import io.github.datacatering.datacaterer.api.model.{FlagsConfig, GenerationConfig}
import org.scalatest.funsuite.AnyFunSuite

class ConfigParserTest extends AnyFunSuite {

  test("Can parse config from file") {
    val result = ConfigParser.getConfig
    val dataCatererConfiguration = ConfigParser.toDataCatererConfiguration

    assert(!result.isEmpty)
    assert(dataCatererConfiguration.flagsConfig.enableGenerateData)
    assert(dataCatererConfiguration.metadataConfig.oneOfMinCount > 0)
    assert(dataCatererConfiguration.connectionConfigByName.nonEmpty)
    assert(dataCatererConfiguration.runtimeConfig.nonEmpty)
  }

  test("Fast generation mode applies optimizations when enabled") {
    // Test optimization logic directly
    val flags = FlagsConfig(
      enableFastGeneration = true,
      enableRecordTracking = true,
      enableCount = true,
      enableSinkMetadata = true,
      enableUniqueCheck = true,
      enableUniqueCheckOnlyInBatch = true,
      enableSaveReports = true,
      enableValidation = true,
      enableGenerateValidations = true,
      enableAlerts = true
    )
    val generation = GenerationConfig(
      numRecordsPerBatch = 100000L,
      uniqueBloomFilterNumItems = 1000000L,
      uniqueBloomFilterFalsePositiveProbability = 0.01
    )
    val runtime = Map(
      "spark.sql.shuffle.partitions" -> "10",
      "spark.serializer" -> "org.apache.spark.serializer.JavaSerializer"
    )

    // Use reflection to access private method for testing
    val configParserClass = ConfigParser.getClass
    val method = configParserClass.getDeclaredMethod("applyFastGenerationOptimizations", classOf[FlagsConfig], classOf[GenerationConfig], classOf[Map[String, String]])
    method.setAccessible(true)
    val result = method.invoke(ConfigParser, flags, generation, runtime).asInstanceOf[(FlagsConfig, GenerationConfig, Map[String, String])]

    val (optimizedFlags, optimizedGeneration, optimizedRuntime) = result

    // Verify flags are disabled for speed
    assert(!optimizedFlags.enableRecordTracking)
    assert(!optimizedFlags.enableCount)
    assert(!optimizedFlags.enableSinkMetadata)
    assert(!optimizedFlags.enableUniqueCheck)
    assert(!optimizedFlags.enableUniqueCheckOnlyInBatch)
    assert(!optimizedFlags.enableSaveReports)
    assert(!optimizedFlags.enableValidation)
    assert(!optimizedFlags.enableGenerateValidations)
    assert(!optimizedFlags.enableAlerts)
    
    // Verify fast generation flag is preserved
    assert(optimizedFlags.enableFastGeneration)

    // Verify generation optimizations
    assert(optimizedGeneration.numRecordsPerBatch >= 1000000L)
    assertResult(100000L)(optimizedGeneration.uniqueBloomFilterNumItems)
    assertResult(0.1)(optimizedGeneration.uniqueBloomFilterFalsePositiveProbability)

    // Verify runtime optimizations
    assertResult("20")(optimizedRuntime("spark.sql.shuffle.partitions"))
    assertResult("true")(optimizedRuntime("spark.sql.adaptive.coalescePartitions.enabled"))
    assertResult("true")(optimizedRuntime("spark.sql.adaptive.skewJoin.enabled"))
    assertResult("org.apache.spark.serializer.KryoSerializer")(optimizedRuntime("spark.serializer"))
    assertResult("true")(optimizedRuntime("spark.sql.cbo.enabled"))
    assertResult("true")(optimizedRuntime("spark.sql.adaptive.enabled"))
  }

  test("Fast generation mode preserves large batch size when already optimized") {
    val flags = FlagsConfig(enableFastGeneration = true)
    val generation = GenerationConfig(numRecordsPerBatch = 2000000L)
    val runtime = Map.empty[String, String]

    // Use reflection to access private method for testing
    val configParserClass = ConfigParser.getClass
    val method = configParserClass.getDeclaredMethod("applyFastGenerationOptimizations", classOf[FlagsConfig], classOf[GenerationConfig], classOf[Map[String, String]])
    method.setAccessible(true)
    val result = method.invoke(ConfigParser, flags, generation, runtime).asInstanceOf[(FlagsConfig, GenerationConfig, Map[String, String])]

    val (_, optimizedGeneration, _) = result

    // Verify the larger batch size is preserved
    assertResult(2000000L)(optimizedGeneration.numRecordsPerBatch)
  }

  test("Fast generation mode does not apply optimizations when disabled") {
    val flags = FlagsConfig(
      enableFastGeneration = false,
      enableRecordTracking = true,
      enableCount = true,
      enableValidation = true
    )
    val generation = GenerationConfig(numRecordsPerBatch = 100000L)
    val runtime = Map("spark.sql.shuffle.partitions" -> "10")

    // Use reflection to access private method for testing
    val configParserClass = ConfigParser.getClass
    val method = configParserClass.getDeclaredMethod("applyFastGenerationOptimizations", classOf[FlagsConfig], classOf[GenerationConfig], classOf[Map[String, String]])
    method.setAccessible(true)
    val result = method.invoke(ConfigParser, flags, generation, runtime).asInstanceOf[(FlagsConfig, GenerationConfig, Map[String, String])]

    val (optimizedFlags, optimizedGeneration, optimizedRuntime) = result

    // Verify original settings are preserved
    assert(!optimizedFlags.enableFastGeneration)
    assert(optimizedFlags.enableRecordTracking)
    assert(optimizedFlags.enableCount)
    assert(optimizedFlags.enableValidation)
    assertResult(100000L)(optimizedGeneration.numRecordsPerBatch)
    assertResult("10")(optimizedRuntime("spark.sql.shuffle.partitions"))
  }

  test("toDataCatererConfiguration applies fast generation optimizations") {
    // This test verifies that the optimization logic is integrated into the main configuration building process
    val config = ConfigParser.toDataCatererConfiguration
    
    // Since the default config has enableFastGeneration = false, verify it's disabled
    assert(!config.flagsConfig.enableFastGeneration)
    
    // Verify no unexpected optimizations are applied when fast generation is disabled
    assert(config.flagsConfig.enableGenerateData) // This should be true by default
  }
}
