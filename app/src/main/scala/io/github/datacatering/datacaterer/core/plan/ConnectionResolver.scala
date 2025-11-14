package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.{Connection, Plan, TaskSummary}
import io.github.datacatering.datacaterer.core.util.EnvVarInterpolator
import org.apache.log4j.Logger

/**
 * Resolves connections for tasks using the following priority:
 * 1. Inline connection in task
 * 2. Connection from plan's connections section
 * 3. Connection from application.conf (fallback with warning)
 *
 * Handles environment variable interpolation for all connection properties.
 */
object ConnectionResolver {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Build a connection map from a Plan's connections section.
   * Applies environment variable interpolation to all connection properties.
   *
   * @param plan The plan containing connection definitions
   * @return Map of connection name to Connection object
   */
  def buildConnectionMap(plan: Plan): Map[String, Connection] = {
    plan.connections match {
      case Some(connections) =>
        connections.flatMap { conn =>
          val interpolatedConnection = interpolateConnection(conn)
          interpolatedConnection.name.map { name =>
            LOGGER.debug(s"Registered connection from plan: $name (type: ${interpolatedConnection.`type`})")
            name -> interpolatedConnection
          }.orElse {
            LOGGER.warn(s"Connection defined without name in plan: ${interpolatedConnection.`type`}")
            None
          }
        }.toMap
      case None =>
        LOGGER.debug("No connections defined in plan")
        Map.empty
    }
  }

  /**
   * Resolve a connection for a task summary.
   * Returns the resolved connection and whether it came from application.conf fallback.
   *
   * @param taskSummary               The task summary with connection info
   * @param planConnectionMap        Map of connections from the plan
   * @param applicationConfConnections Map of connections from application.conf
   * @return Option of (resolved connection as Map[String, String], isFromAppConf: Boolean)
   */
  def resolveTaskConnection(
                             taskSummary: TaskSummary,
                             planConnectionMap: Map[String, Connection],
                             applicationConfConnections: Map[String, Map[String, String]]
                           ): Option[(Map[String, String], Boolean)] = {

    // Priority 1: Inline connection in task summary
    taskSummary.connection match {
      case Some(Left(connectionName)) =>
        // Connection reference by name
        resolveConnectionByName(connectionName, planConnectionMap, applicationConfConnections)

      case Some(Right(inlineConnection)) =>
        // Inline connection object
        LOGGER.debug(s"Using inline connection for task '${taskSummary.name}' (type: ${inlineConnection.`type`})")
        val interpolated = interpolateConnection(inlineConnection)
        Some((connectionToMap(interpolated), false))

      case None =>
        // Priority 2 & 3: Look up by dataSourceName
        resolveConnectionByName(taskSummary.dataSourceName, planConnectionMap, applicationConfConnections)
    }
  }

  /**
   * Resolve a connection by name, checking plan connections first, then application.conf.
   *
   * @param connectionName            The connection name to resolve
   * @param planConnectionMap        Map of connections from the plan
   * @param applicationConfConnections Map of connections from application.conf
   * @return Option of (resolved connection as Map[String, String], isFromAppConf: Boolean)
   */
  private def resolveConnectionByName(
                                       connectionName: String,
                                       planConnectionMap: Map[String, Connection],
                                       applicationConfConnections: Map[String, Map[String, String]]
                                     ): Option[(Map[String, String], Boolean)] = {

    // Check plan connections first
    planConnectionMap.get(connectionName) match {
      case Some(connection) =>
        LOGGER.debug(s"Resolved connection '$connectionName' from plan (type: ${connection.`type`})")
        Some((connectionToMap(connection), false))

      case None =>
        // Fallback to application.conf
        applicationConfConnections.get(connectionName) match {
          case Some(appConfConnection) =>
            LOGGER.warn(
              s"Connection '$connectionName' not found in plan YAML, falling back to application.conf. " +
                "Consider migrating to unified YAML format for better portability and clarity."
            )
            // Apply environment variable interpolation to application.conf values
            val interpolated = EnvVarInterpolator.interpolateMap(appConfConnection)
            Some((interpolated, true))

          case None =>
            LOGGER.error(s"Connection '$connectionName' not found in plan or application.conf")
            None
        }
    }
  }

  /**
   * Interpolate environment variables in a Connection object.
   *
   * @param connection The connection to interpolate
   * @return Connection with environment variables resolved
   */
  def interpolateConnection(connection: Connection): Connection = {
    connection.copy(
      options = EnvVarInterpolator.interpolateMap(connection.options)
    )
  }

  /**
   * Convert a Connection object to a Map[String, String] for use with existing code.
   *
   * @param connection The connection to convert
   * @return Map representation of the connection
   */
  def connectionToMap(connection: Connection): Map[String, String] = {
    Map(
      "format" -> connection.`type`  // Use type as format for compatibility
    ) ++ connection.options
  }

  /**
   * Validate that all task summary connections can be resolved.
   *
   * @param plan                       The plan to validate
   * @param applicationConfConnections Connections from application.conf
   * @return List of validation errors (empty if all valid)
   */
  def validateConnections(
                           plan: Plan,
                           applicationConfConnections: Map[String, Map[String, String]]
                         ): List[String] = {
    val planConnectionMap = buildConnectionMap(plan)
    val errors = scala.collection.mutable.ListBuffer[String]()

    plan.tasks.foreach { taskSummary =>
      taskSummary.connection match {
        case Some(Left(connectionName)) =>
          if (!planConnectionMap.contains(connectionName) && !applicationConfConnections.contains(connectionName)) {
            errors += s"Task '${taskSummary.name}' references unknown connection '$connectionName'"
          }
        case Some(Right(_)) =>
          // Inline connection is always valid
        case None =>
          // Check dataSourceName
          if (taskSummary.dataSourceName.nonEmpty &&
              !planConnectionMap.contains(taskSummary.dataSourceName) &&
              !applicationConfConnections.contains(taskSummary.dataSourceName)) {
            errors += s"Task '${taskSummary.name}' references unknown connection '${taskSummary.dataSourceName}'"
          }
      }
    }

    errors.toList
  }

  /**
   * Get a summary of all connections available (plan + application.conf).
   *
   * @param plan                       The plan
   * @param applicationConfConnections Connections from application.conf
   * @return Map of connection names to their source ("plan" or "application.conf")
   */
  def getConnectionSources(
                            plan: Plan,
                            applicationConfConnections: Map[String, Map[String, String]]
                          ): Map[String, String] = {
    val planConnections = buildConnectionMap(plan).keys.map(_ -> "plan").toMap
    val appConfConnections = applicationConfConnections.keys.map(_ -> "application.conf").toMap
    appConfConnections ++ planConnections  // Plan connections override if duplicates (plan on right = higher priority)
  }
}
