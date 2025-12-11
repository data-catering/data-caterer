package io.github.datacatering.datacaterer.core.ui.config

import org.apache.log4j.Logger

object UiConfiguration {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def INSTALL_DIRECTORY: String = getInstallDirectory

  /**
   * CORS Configuration
   * 
   * Environment variables:
   * - DATA_CATERER_CORS_ALLOWED_ORIGINS: Comma-separated list of allowed origins (default: "*" for all)
   *   Examples: "*", "http://localhost:3000", "http://localhost:3000,https://example.com"
   * - DATA_CATERER_CORS_ALLOWED_METHODS: Comma-separated list of allowed HTTP methods (default: "GET,POST,PUT,DELETE,OPTIONS")
   * - DATA_CATERER_CORS_ALLOWED_HEADERS: Comma-separated list of allowed headers (default: "Content-Type,Authorization,X-Requested-With")
   * - DATA_CATERER_CORS_MAX_AGE: Max age for preflight cache in seconds (default: "86400")
   */
  object Cors {
    val allowedOrigins: String = Option(System.getenv("DATA_CATERER_CORS_ALLOWED_ORIGINS"))
      .filter(_.nonEmpty)
      .getOrElse("*")
    
    val allowedMethods: Seq[String] = Option(System.getenv("DATA_CATERER_CORS_ALLOWED_METHODS"))
      .filter(_.nonEmpty)
      .map(_.split(",").map(_.trim).toSeq)
      .getOrElse(Seq("GET", "POST", "PUT", "DELETE", "OPTIONS"))
    
    val allowedHeaders: Seq[String] = Option(System.getenv("DATA_CATERER_CORS_ALLOWED_HEADERS"))
      .filter(_.nonEmpty)
      .map(_.split(",").map(_.trim).toSeq)
      .getOrElse(Seq("Content-Type", "Authorization", "X-Requested-With"))
    
    val maxAge: Long = Option(System.getenv("DATA_CATERER_CORS_MAX_AGE"))
      .filter(_.nonEmpty)
      .flatMap(s => scala.util.Try(s.toLong).toOption)
      .getOrElse(86400L) // 24 hours
    
    def logConfiguration(): Unit = {
      LOGGER.info(s"CORS configuration: allowed-origins=$allowedOrigins, " +
        s"allowed-methods=${allowedMethods.mkString(",")}, " +
        s"allowed-headers=${allowedHeaders.mkString(",")}, " +
        s"max-age=$maxAge")
    }
  }

  def getInstallDirectory: String = {
    val overrideDirectory = System.getProperty("data-caterer-install-dir")
    if (overrideDirectory != null && overrideDirectory.nonEmpty) {
      LOGGER.info(s"Using override install directory, override-directory=$overrideDirectory")
      overrideDirectory
    } else {
      "/opt/DataCaterer"
    }
  }

}
