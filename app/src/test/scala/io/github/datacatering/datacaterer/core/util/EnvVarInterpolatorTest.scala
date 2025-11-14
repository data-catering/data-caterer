package io.github.datacatering.datacaterer.core.util

import org.scalatest.funsuite.AnyFunSuite

class EnvVarInterpolatorTest extends AnyFunSuite {

  test("interpolate with environment variable that exists") {
    // Setup - use a known env var (PATH always exists)
    val input = "Value is: ${PATH}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result.startsWith("Value is: "))
    assert(result != input) // Should be interpolated
  }

  test("interpolate with default value when env var does not exist") {
    val input = "jdbc:postgresql://${DB_HOST:-localhost}:5432/db"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "jdbc:postgresql://localhost:5432/db")
  }

  test("interpolate with multiple env vars") {
    val input = "${DB_USER:-postgres}@${DB_HOST:-localhost}:${DB_PORT:-5432}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "postgres@localhost:5432")
  }

  test("interpolate empty default value") {
    val input = "key=${API_KEY:-}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "key=")
  }

  test("interpolate escaped variable") {
    val input = "Literal: $${NOT_A_VAR}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "Literal: ${NOT_A_VAR}")
  }

  test("interpolate required variable that exists") {
    // Use PATH which always exists
    val input = "Path is ${PATH}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result.startsWith("Path is "))
    assert(!result.contains("${"))
  }

  test("interpolate required variable that does not exist throws exception") {
    val input = "Value: ${NONEXISTENT_VAR_12345}"
    val exception = intercept[IllegalArgumentException] {
      EnvVarInterpolator.interpolate(input)
    }
    assert(exception.getMessage.contains("NONEXISTENT_VAR_12345"))
    assert(exception.getMessage.contains("not set"))
  }

  test("interpolate null returns null") {
    val result = EnvVarInterpolator.interpolate(null)
    assert(result == null)
  }

  test("interpolate string without variables returns unchanged") {
    val input = "Just a plain string"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == input)
  }

  test("interpolate with nested braces") {
    val input = "URL: ${API_URL:-http://localhost:8080}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "URL: http://localhost:8080")
  }

  test("interpolateMap with all string values") {
    val input = Map(
      "url" -> "jdbc:postgresql://${DB_HOST:-localhost}:5432/db",
      "user" -> "${DB_USER:-postgres}",
      "password" -> "${DB_PASSWORD:-password123}"
    )
    val result = EnvVarInterpolator.interpolateMap(input)
    assert(result("url") == "jdbc:postgresql://localhost:5432/db")
    assert(result("user") == "postgres")
    assert(result("password") == "password123")
  }

  test("interpolateAnyMap with mixed value types") {
    val input = Map[String, Any](
      "url" -> "jdbc:postgresql://${DB_HOST:-localhost}:5432/db",
      "port" -> 5432,
      "enabled" -> true,
      "timeout" -> 30.5
    )
    val result = EnvVarInterpolator.interpolateAnyMap(input)
    assert(result("url") == "jdbc:postgresql://localhost:5432/db")
    assert(result("port") == 5432)
    assert(result("enabled") == true)
    assert(result("timeout") == 30.5)
  }

  test("containsEnvVars detects env var placeholders") {
    assert(EnvVarInterpolator.containsEnvVars("${VAR}"))
    assert(EnvVarInterpolator.containsEnvVars("${VAR:-default}"))
    assert(EnvVarInterpolator.containsEnvVars("prefix ${VAR} suffix"))
    assert(!EnvVarInterpolator.containsEnvVars("plain string"))
    assert(!EnvVarInterpolator.containsEnvVars(null))
  }

  test("interpolateSafe returns original on error") {
    val input = "Value: ${NONEXISTENT_VAR_SAFE}"
    val result = EnvVarInterpolator.interpolateSafe(input, logErrors = false)
    assert(result == input) // Should return original
  }

  test("interpolateSafe succeeds with valid input") {
    val input = "Value: ${DB_HOST:-localhost}"
    val result = EnvVarInterpolator.interpolateSafe(input)
    assert(result == "Value: localhost")
  }

  test("interpolate with special characters in default") {
    val input = "${SPECIAL:-value-with-dashes_and_underscores.123}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "value-with-dashes_and_underscores.123")
  }

  test("interpolate with URL as default") {
    val input = "${DATABASE_URL:-jdbc:postgresql://localhost:5432/mydb?ssl=true&timeout=30}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "jdbc:postgresql://localhost:5432/mydb?ssl=true&timeout=30")
  }

  test("interpolate complex real-world example") {
    val input = "jdbc:postgresql://${DB_HOST:-db.example.com}:${DB_PORT:-5432}/${DB_NAME:-production}?user=${DB_USER:-admin}&password=${DB_PASSWORD:-secret123}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "jdbc:postgresql://db.example.com:5432/production?user=admin&password=secret123")
  }

  test("interpolate with empty string") {
    val input = ""
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "")
  }

  test("interpolate with only variable") {
    val input = "${VAR:-value}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "value")
  }

  test("interpolate multiple escaped variables") {
    val input = "Config: $${VAR1} and $${VAR2}"
    val result = EnvVarInterpolator.interpolate(input)
    assert(result == "Config: ${VAR1} and ${VAR2}")
  }
}
