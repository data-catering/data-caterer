package io.github.datacatering.datacaterer.core.ui.config

import org.apache.log4j.Logger

object UiConfiguration {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getInstallDirectory: String = {
    val osName = System.getProperty("os.name").toLowerCase
    if (osName.contains("win")) {
      val appDataDir = System.getenv("APPDATA")
      s"$appDataDir/DataCaterer"
    } else if (osName.contains("nix") || osName.contains("nux") || osName.contains("aix")) {
      "/opt/DataCaterer"
    } else if (osName.contains("mac")) {
      "/Applications/DataCaterer.app"
    } else {
      LOGGER.warn(s"Unknown operating system name, defaulting install directory to '/tmp/DataCaterer', os.name=$osName")
      "/tmp/DataCaterer"
    }
  }

  lazy val INSTALL_DIRECTORY = getInstallDirectory

}
