package io.github.datacatering.datacaterer.core.ui.config

import org.apache.log4j.Logger

import java.nio.file.{Path, Paths}

object UiConfiguration {

  private val LOGGER = Logger.getLogger(getClass.getName)

  val INSTALL_DIRECTORY: String = getInstallDirectory

  def getInstallDirectory: String = {
    val osName = System.getProperty("os.name").toLowerCase
    if (osName.contains("win")) {
      val appDataDir = System.getenv("APPDATA")
      s"$appDataDir/DataCaterer"
    } else if (osName.contains("nix") || osName.contains("nux") || osName.contains("aix")) {
      "/opt/DataCaterer"
    } else if (osName.contains("mac")) {
      "/Library/DataCaterer"
    } else {
      LOGGER.warn(s"Unknown operating system name, defaulting install directory to '/tmp/DataCaterer', os.name=$osName")
      "/tmp/DataCaterer"
    }
  }

}
