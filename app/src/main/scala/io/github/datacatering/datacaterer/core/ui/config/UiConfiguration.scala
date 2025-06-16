package io.github.datacatering.datacaterer.core.ui.config

import org.apache.log4j.Logger

object UiConfiguration {

  private val LOGGER = Logger.getLogger(getClass.getName)

  val INSTALL_DIRECTORY: String = getInstallDirectory

  def getInstallDirectory: String = {
    val osName = System.getProperty("os.name").toLowerCase
    val overrideDirectory = System.getProperty("data-caterer-install-dir")
    if (overrideDirectory != null && overrideDirectory.nonEmpty) {
      LOGGER.info(s"Using override install directory, override-directory=$overrideDirectory")
      overrideDirectory
    } else if (osName.contains("win")) {
      val appDataDir = System.getenv("APPDATA")
      s"$appDataDir/DataCaterer"
    } else if (osName.contains("nix") || osName.contains("nux") || osName.contains("aix")) {
      "/opt/DataCaterer"
    } else if (osName.contains("mac")) {
      val userHome = System.getProperty("user.home")
      s"$userHome/Library/DataCaterer"
    } else {
      LOGGER.warn(s"Unknown operating system name, defaulting install directory to '/tmp/DataCaterer', os-name=$osName")
      "/tmp/DataCaterer"
    }
  }

}
