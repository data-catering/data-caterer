package io.github.datacatering.datacaterer.core.ui.resource

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_MASTER, DEFAULT_RUNTIME_CONFIG}
import io.github.datacatering.datacaterer.core.util.SparkProvider
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Singleton manager for SparkSession lifecycle
 * Ensures only one SparkSession exists per application to avoid resource overhead
 *
 * Thread-safe using double-checked locking pattern
 */
object SparkSessionManager {

  private val LOGGER = Logger.getLogger(getClass.getName)

  @volatile private var sparkSession: Option[SparkSession] = None
  private val lock = new Object()
  private var shutdownHookRegistered = false

  /**
   * Get or create the singleton SparkSession
   * Thread-safe using double-checked locking
   *
   * @return The singleton SparkSession instance
   */
  def getOrCreate(): SparkSession = {
    // First check without locking (fast path)
    if (sparkSession.isEmpty || !isActive(sparkSession)) {
      lock.synchronized {
        // Second check with lock (ensures thread safety)
        if (sparkSession.isEmpty || !isActive(sparkSession)) {
          LOGGER.info("Creating new SparkSession instance")
          val startTime = System.currentTimeMillis()

          val session = new SparkProvider(DEFAULT_MASTER, DEFAULT_RUNTIME_CONFIG).getSparkSession
          sparkSession = Some(session)

          val creationTime = System.currentTimeMillis() - startTime
          LOGGER.info(s"SparkSession created successfully, " +
            s"session-id=${session.sparkContext.applicationId}, " +
            s"creation-time=${creationTime}ms, " +
            s"master=${session.sparkContext.master}")

          registerShutdownHook()
        }
      }
    }
    sparkSession.get
  }

  /**
   * Explicitly stop the SparkSession
   * Should only be called during application shutdown
   */
  def stop(): Unit = {
    lock.synchronized {
      sparkSession.foreach { session =>
        LOGGER.info(s"Stopping SparkSession, session-id=${session.sparkContext.applicationId}")
        try {
          session.stop()
          LOGGER.info("SparkSession stopped successfully")
        } catch {
          case ex: Exception =>
            LOGGER.error("Error stopping SparkSession", ex)
        }
        sparkSession = None
      }
    }
  }

  /**
   * Check if the current session is active and usable
   *
   * @param session Optional SparkSession to check
   * @return true if session exists and is not stopped
   */
  private def isActive(session: Option[SparkSession]): Boolean = {
    session.exists { s =>
      !s.sparkContext.isStopped
    }
  }

  /**
   * Register shutdown hook to clean up SparkSession on JVM shutdown
   * Only registers once to avoid duplicate hooks
   */
  private def registerShutdownHook(): Unit = {
    if (!shutdownHookRegistered) {
      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        LOGGER.info("Shutdown hook triggered, stopping SparkSession")
        stop()
      }))
      shutdownHookRegistered = true
      LOGGER.debug("Shutdown hook registered for SparkSession cleanup")
    }
  }

  /**
   * Get current session statistics for monitoring and debugging
   *
   * @return Optional statistics if session exists
   */
  def getStats: Option[SparkSessionStats] = {
    sparkSession.map { session =>
      val uptime = System.currentTimeMillis() - session.sparkContext.startTime
      SparkSessionStats(
        applicationId = session.sparkContext.applicationId,
        applicationName = session.sparkContext.appName,
        master = session.sparkContext.master,
        isActive = !session.sparkContext.isStopped,
        startTime = session.sparkContext.startTime,
        uptimeMillis = uptime
      )
    }
  }

  /**
   * Check if a SparkSession instance currently exists
   *
   * @return true if session exists and is active
   */
  def exists(): Boolean = {
    isActive(sparkSession)
  }
}

/**
 * Statistics about the current SparkSession
 *
 * @param applicationId Spark application ID
 * @param applicationName Spark application name
 * @param master Spark master URL
 * @param isActive Whether the session is active
 * @param startTime When the session was started (epoch millis)
 * @param uptimeMillis How long the session has been running
 */
case class SparkSessionStats(
  applicationId: String,
  applicationName: String,
  master: String,
  isActive: Boolean,
  startTime: Long,
  uptimeMillis: Long
) {
  override def toString: String = {
    s"""SparkSession Statistics:
       |  Application ID: $applicationId
       |  Application Name: $applicationName
       |  Master: $master
       |  Active: $isActive
       |  Start Time: ${new java.util.Date(startTime)}
       |  Uptime: ${uptimeMillis / 1000} seconds
       |""".stripMargin
  }
}
