package com.github.pflooky.datagen.core.config

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
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
}
