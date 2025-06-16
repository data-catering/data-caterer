package io.github.datacatering.datacaterer.core.config

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
}
