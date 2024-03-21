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
      setHadoopHome(appDataDir)
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

  private def setHadoopHome(appDataDir: String): Unit = {
    val hadoopPath = Path.of(s"$appDataDir/DataCaterer/hadoop").toFile
    if (!hadoopPath.exists()) hadoopPath.mkdirs()
    System.setProperty("hadoop.home.dir", s"$appDataDir/DataCaterer/hadoop")
  }

}
