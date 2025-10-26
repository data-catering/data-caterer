package io.github.datacatering.datacaterer.core.ui.service

import io.github.datacatering.datacaterer.api.model.Constants.{DRIVER, FORMAT, JDBC, MYSQL, MYSQL_DRIVER, POSTGRES, POSTGRES_DRIVER}
import io.github.datacatering.datacaterer.core.ui.model.Connection
import io.github.datacatering.datacaterer.core.ui.plan.ConnectionRepository
import org.apache.log4j.Logger

/**
 * Centralized service for connection management
 *
 * This service provides a simplified, non-actor-based interface to ConnectionRepository
 * for use in synchronous contexts like PlanRepository and FastSampleGenerator.
 *
 * Features:
 * - Simplified connection retrieval (masks passwords by default)
 * - Connection details with format-specific configuration
 * - Batch connection loading for multiple data sources
 * - Format-specific driver and JDBC configuration
 */
object ConnectionService {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Get a single connection by name
   *
   * @param connectionName Name of the connection
   * @param masking Whether to mask sensitive fields like passwords (default: true)
   * @return Connection details
   * @throws IllegalArgumentException if connection not found
   */
  def getConnection(connectionName: String, masking: Boolean = true): Connection = {
    LOGGER.debug(s"Getting connection, connection-name=$connectionName, masking=$masking")
    ConnectionRepository.getConnection(connectionName, masking)
  }

  /**
   * Get multiple connections by names
   *
   * @param connectionNames List of connection names
   * @param masking Whether to mask sensitive fields
   * @return List of connections
   */
  def getConnections(connectionNames: List[String], masking: Boolean = true): List[Connection] = {
    LOGGER.debug(s"Getting connections, count=${connectionNames.size}, masking=$masking")
    connectionNames.map(name => getConnection(name, masking))
  }

  /**
   * Get connection details with format-specific configuration added
   *
   * This method adds driver and format information based on connection type
   * (e.g., for Postgres, adds JDBC driver and format)
   *
   * @param connectionName Name of the connection
   * @return Map of configuration options with format-specific additions
   */
  def getConnectionDetailsWithFormat(connectionName: String): Map[String, String] = {
    val connection = getConnection(connectionName, masking = false)

    val additionalConfig = connection.`type` match {
      case POSTGRES => Map(FORMAT -> JDBC, DRIVER -> POSTGRES_DRIVER)
      case MYSQL => Map(FORMAT -> JDBC, DRIVER -> MYSQL_DRIVER)
      case format => Map(FORMAT -> format)
    }

    connection.options ++ additionalConfig
  }

  /**
   * Get connection details for multiple connections as a map
   *
   * @param connectionNames List of connection names
   * @return Map of connection name to connection options with format
   */
  def getConnectionDetailsMap(connectionNames: List[String]): Map[String, Map[String, String]] = {
    LOGGER.debug(s"Getting connection details map, count=${connectionNames.size}")
    connectionNames.map(name => {
      name -> getConnectionDetailsWithFormat(name)
    }).toMap
  }

  /**
   * Get connection details for a task-to-datasource mapping
   *
   * This is a common pattern where tasks reference data sources by name
   *
   * @param taskToDataSourceMap Map of task names to data source names
   * @return Map of data source name to connection options
   */
  def getConnectionDetailsForTasks(taskToDataSourceMap: Map[String, String]): Map[String, Map[String, String]] = {
    val uniqueDataSourceNames = taskToDataSourceMap.values.toList.distinct
    LOGGER.debug(s"Getting connection details for tasks, unique-data-sources=${uniqueDataSourceNames.size}")
    getConnectionDetailsMap(uniqueDataSourceNames)
  }

  /**
   * Check if a connection exists
   *
   * @param connectionName Name of the connection to check
   * @return true if connection exists, false otherwise
   */
  def connectionExists(connectionName: String): Boolean = {
    try {
      getConnection(connectionName, masking = true)
      true
    } catch {
      case _: IllegalArgumentException => false
      case _: Exception => false
    }
  }

  /**
   * Get metadata source connection information
   *
   * This is used when steps reference metadata sources for schema discovery
   *
   * @param metadataSourceName Name of the metadata source
   * @param connectionConfigsByName Existing connection configs (to check before querying repository)
   * @return Map of metadata connection options
   */
  def getMetadataSourceInfo(
    metadataSourceName: String,
    connectionConfigsByName: Map[String, Map[String, String]]
  ): Map[String, String] = {
    if (connectionConfigsByName.contains(metadataSourceName)) {
      LOGGER.debug(s"Using metadata source from existing configs, metadata-source=$metadataSourceName")
      connectionConfigsByName(metadataSourceName)
    } else {
      LOGGER.debug(s"Loading metadata source from connection repository, metadata-source=$metadataSourceName")
      val metadataConnection = getConnection(metadataSourceName, masking = false)
      metadataConnection.options ++ Map(io.github.datacatering.datacaterer.api.model.Constants.METADATA_SOURCE_TYPE -> metadataConnection.`type`)
    }
  }

  /**
   * Get connection with all details (no masking)
   * Use with caution - only for internal processing
   *
   * @param connectionName Name of the connection
   * @return Connection with unmasked credentials
   */
  def getConnectionUnmasked(connectionName: String): Connection = {
    getConnection(connectionName, masking = false)
  }
}
