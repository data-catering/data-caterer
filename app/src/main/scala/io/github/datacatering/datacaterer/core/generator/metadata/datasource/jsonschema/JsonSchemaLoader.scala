package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import com.networknt.schema.{JsonSchemaFactory, SpecVersion}
import org.apache.log4j.Logger

import java.io.{File, InputStream}
import java.net.{URL, URLConnection}
import scala.util.{Failure, Success, Try, Using}

/**
 * Utility class for loading JSON schemas from files or URLs
 */
object JsonSchemaLoader {
  
  private val LOGGER = Logger.getLogger(getClass.getName)
  
  /**
   * Load a JSON schema from a file path or URL
   *
   * @param source the file path or URL to the JSON schema
   * @return the loaded JsonSchema or throws JsonSchemaLoadException
   */
  def loadSchema(source: String): com.networknt.schema.JsonSchema = {
    LOGGER.info(s"Loading JSON schema from source: $source")
    
    if (isUrl(source)) {
      loadFromUrl(source)
    } else {
      loadFromFile(source)
    }
  }
  
  /**
   * Load a JSON schema from a URL
   */
  private def loadFromUrl(urlString: String): com.networknt.schema.JsonSchema = {
    Try {
      val url = new URL(urlString)
      val connection: URLConnection = url.openConnection()
      connection.setConnectTimeout(30000) // 30 seconds
      connection.setReadTimeout(60000)    // 60 seconds
      
      Using(connection.getInputStream) { inputStream =>
        createSchemaFromInputStream(inputStream, urlString)
      }.get
    } match {
      case Success(schema) =>
        LOGGER.info(s"Successfully loaded JSON schema from URL: $urlString")
        schema
      case Failure(exception) =>
        val errorMsg = s"Failed to load JSON schema from URL: $urlString"
        LOGGER.error(errorMsg, exception)
        throw JsonSchemaLoadException(errorMsg, exception)
    }
  }
  
  /**
   * Load a JSON schema from a file path
   */
  private def loadFromFile(filePath: String): com.networknt.schema.JsonSchema = {
    Try {
      val file = new File(filePath)
      if (!file.exists()) {
        throw new IllegalArgumentException(s"Schema file does not exist: $filePath")
      }
      if (!file.canRead) {
        throw new IllegalArgumentException(s"Schema file is not readable: $filePath")
      }
      
      Using(new java.io.FileInputStream(file)) { inputStream =>
        createSchemaFromInputStream(inputStream, filePath)
      }.get
    } match {
      case Success(schema) =>
        LOGGER.info(s"Successfully loaded JSON schema from file: $filePath")
        schema
      case Failure(exception) =>
        val errorMsg = s"Failed to load JSON schema from file: $filePath"
        LOGGER.error(errorMsg, exception)
        throw JsonSchemaLoadException(errorMsg, exception)
    }
  }
  
  /**
   * Create a JsonSchema object from an InputStream
   */
  private def createSchemaFromInputStream(inputStream: InputStream, source: String): com.networknt.schema.JsonSchema = {
    Try {
      // Auto-detect schema version, defaulting to latest (2020-12)
      val factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
      factory.getSchema(inputStream)
    } match {
      case Success(schema) => schema
      case Failure(exception) =>
        throw JsonSchemaParseException(s"Failed to parse JSON schema from source: $source", exception)
    }
  }
  
  /**
   * Check if the source string is a URL
   */
  private def isUrl(source: String): Boolean = {
    source.startsWith("http://") || source.startsWith("https://")
  }
}

/**
 * Exception thrown when JSON schema loading fails
 */
case class JsonSchemaLoadException(message: String, cause: Throwable = null) extends Exception(message, cause)

/**
 * Exception thrown when JSON schema parsing fails
 */
case class JsonSchemaParseException(message: String, cause: Throwable = null) extends Exception(message, cause) 