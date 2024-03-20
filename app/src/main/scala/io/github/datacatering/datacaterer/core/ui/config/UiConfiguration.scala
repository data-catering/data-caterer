package io.github.datacatering.datacaterer.core.ui.config

import org.apache.log4j.Logger

object UiConfiguration {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getInstallDirectory: String = {
    val osName = System.getProperty("os.name").toLowerCase
    if (osName.contains("win")) {
      "/Program Files/data-caterer"
    } else if (osName.contains("nix") || osName.contains("nux") || osName.contains("aix")) {
      "/opt/data-caterer"
    } else if (osName.contains("mac")) {
      "/Applications/data-caterer"
    } else {
      LOGGER.warn(s"Unknown operating system name, defaulting install directory to '/tmp/data-caterer', os.name=$osName")
      "/tmp/data-caterer"
    }
  }

  lazy val INSTALL_DIRECTORY = getInstallDirectory

}
