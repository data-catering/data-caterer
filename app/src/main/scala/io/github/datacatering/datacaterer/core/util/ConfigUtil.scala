package io.github.datacatering.datacaterer.core.util

object ConfigUtil {

  def cleanseOptions(config: Map[String, String]): Map[String, String] = {
    config.filter(o =>
      !(
        o._1.toLowerCase.contains("password") || o._2.toLowerCase.contains("password") ||
          o._1.toLowerCase.contains("token") ||
          o._1.toLowerCase.contains("secret") ||
          o._1.toLowerCase.contains("private")
        )
    )
  }

}
