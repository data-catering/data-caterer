package io.github.datacatering.datacaterer.core.util

import org.apache.log4j.Logger

import scala.util.matching.Regex

/**
 * Utility for interpolating environment variables in strings using Docker Compose / Bash syntax.
 * Supports:
 * - ${VAR} - Required variable (throws if not found)
 * - ${VAR:-default} - Variable with default value
 * - $${VAR} - Escaped literal (becomes ${VAR})
 */
object EnvVarInterpolator {

  private val LOGGER = Logger.getLogger(getClass.getName)

  // Regex to match environment variable patterns
  // Matches: ${VAR_NAME} or ${VAR_NAME:-default_value} or $${ESCAPED}
  private val ENV_VAR_PATTERN: Regex = """(\$\$\{[^}]+\}|\$\{([^:}]+)(?::-([^}]*))?\})""".r

  /**
   * Interpolate environment variables in a string.
   *
   * @param input The input string potentially containing env var placeholders
   * @return The string with environment variables replaced
   * @throws IllegalArgumentException if a required variable (without default) is missing
   */
  def interpolate(input: String): String = {
    if (input == null) return null

    ENV_VAR_PATTERN.replaceAllIn(input, m => {
      val fullMatch = m.group(0)

      // Handle escaped variables: $${VAR} -> ${VAR}
      if (fullMatch.startsWith("$$")) {
        Regex.quoteReplacement(fullMatch.substring(1)) // Remove one $
      } else {
        val varName = m.group(2)
        val defaultValue = Option(m.group(3))

        val value = sys.env.get(varName) match {
          case Some(v) =>
            LOGGER.debug(s"Resolved environment variable: $varName")
            v
          case None =>
            defaultValue match {
              case Some(default) =>
                LOGGER.debug(s"Environment variable '$varName' not found, using default: '$default'")
                default
              case None =>
                val errorMsg = s"Required environment variable '$varName' is not set and no default value provided"
                LOGGER.error(errorMsg)
                throw new IllegalArgumentException(errorMsg)
            }
        }

        Regex.quoteReplacement(value)
      }
    })
  }

  /**
   * Interpolate environment variables in all string values within a Map.
   *
   * @param map The input map
   * @return A new map with interpolated values
   */
  def interpolateMap(map: Map[String, String]): Map[String, String] = {
    map.map { case (key, value) =>
      key -> interpolate(value)
    }
  }

  /**
   * Interpolate environment variables in all string values within a Map[String, Any].
   * Only interpolates String values, leaves other types unchanged.
   *
   * @param map The input map
   * @return A new map with interpolated string values
   */
  def interpolateAnyMap(map: Map[String, Any]): Map[String, Any] = {
    map.map { case (key, value) =>
      key -> (value match {
        case s: String => interpolate(s)
        case other => other
      })
    }
  }

  /**
   * Check if a string contains any environment variable placeholders.
   *
   * @param input The input string
   * @return true if the string contains ${...} patterns
   */
  def containsEnvVars(input: String): Boolean = {
    if (input == null) false
    else ENV_VAR_PATTERN.findFirstIn(input).isDefined
  }

  /**
   * Safely interpolate, returning the original string if interpolation fails.
   *
   * @param input         The input string
   * @param logErrors     Whether to log errors (default: true)
   * @return The interpolated string, or original if interpolation fails
   */
  def interpolateSafe(input: String, logErrors: Boolean = true): String = {
    try {
      interpolate(input)
    } catch {
      case e: IllegalArgumentException =>
        if (logErrors) {
          LOGGER.warn(s"Failed to interpolate environment variables in: $input", e)
        }
        input
      case e: Exception =>
        if (logErrors) {
          LOGGER.error(s"Unexpected error during interpolation: $input", e)
        }
        input
    }
  }
}
