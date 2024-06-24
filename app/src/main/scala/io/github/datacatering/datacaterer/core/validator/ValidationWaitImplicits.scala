package io.github.datacatering.datacaterer.core.validator

import io.github.datacatering.datacaterer.api.model.Constants.FORMAT
import io.github.datacatering.datacaterer.api.model.{DataExistsWaitCondition, FileExistsWaitCondition, PauseWaitCondition, WaitCondition, WebhookWaitCondition}
import io.github.datacatering.datacaterer.core.exception.InvalidWaitConditionException
import io.github.datacatering.datacaterer.core.util.ConfigUtil
import io.github.datacatering.datacaterer.core.util.HttpUtil.getAuthHeader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.asynchttpclient.Dsl.asyncHttpClient

import scala.util.{Failure, Success, Try}


object ValidationWaitImplicits {
  implicit class WaitConditionOps(waitCondition: WaitCondition = PauseWaitCondition()) {
    private val LOGGER = Logger.getLogger(getClass.getName)

    def checkCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Boolean = true

    def waitForCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Unit = {
      if (waitCondition.isRetryable) {
        var retries = 0
        while (retries < waitCondition.maxRetries) {
          if (!checkCondition(connectionConfigByName)) {
            LOGGER.debug(s"Wait condition failed, pausing before retrying, pause-before-retry-seconds=${waitCondition.waitBeforeRetrySeconds}, " +
              s"num-retries=$retries, max-retries=${waitCondition.maxRetries}")
            Thread.sleep(waitCondition.waitBeforeRetrySeconds * 1000)
            retries += 1
          } else {
            return
          }
        }
        LOGGER.warn(s"Max retries has been reached for validation wait condition, continuing to try validation, " +
          s"max-retries=${waitCondition.maxRetries}")
      } else {
        checkCondition(connectionConfigByName)
      }
    }
  }

  implicit class PauseWaitConditionOps(pauseWaitCondition: PauseWaitCondition) extends WaitConditionOps(pauseWaitCondition) {
    private val LOGGER = Logger.getLogger(getClass.getName)

    override def checkCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Boolean = {
      LOGGER.debug(s"Pausing execution before starting validation, pause-in-seconds=${pauseWaitCondition.pauseInSeconds}")
      Thread.sleep(pauseWaitCondition.pauseInSeconds * 1000)
      true
    }
  }

  implicit class FileExistsWaitConditionOps(fileExistsWaitCondition: FileExistsWaitCondition) extends WaitConditionOps(fileExistsWaitCondition) {
    private val LOGGER = Logger.getLogger(getClass.getName)

    override def checkCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Boolean = {
      LOGGER.debug(s"Checking if file exists before running validations, file-path=${fileExistsWaitCondition.path}")
      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      fs.exists(new Path(fileExistsWaitCondition.path))
    }
  }

  implicit class DataExistsWaitConditionOps(dataExistsWaitCondition: DataExistsWaitCondition) extends WaitConditionOps(dataExistsWaitCondition) {
    private val LOGGER = Logger.getLogger(getClass.getName)

    override def checkCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Boolean = {
      LOGGER.debug(s"Checking if data exists before running validations, data-source-name=${dataExistsWaitCondition.dataSourceName}," +
        s"data-source-options=${ConfigUtil.cleanseOptions(dataExistsWaitCondition.options)}, expression=${dataExistsWaitCondition.expr}")
      val connectionOptions = connectionConfigByName(dataExistsWaitCondition.dataSourceName)
      val loadData = sparkSession.read
        .format(connectionOptions(FORMAT))
        .options(connectionOptions ++ dataExistsWaitCondition.options)
        .load()
        .where(dataExistsWaitCondition.expr)
      !loadData.isEmpty
    }
  }

  implicit class WebhookWaitConditionOps(webhookWaitCondition: WebhookWaitCondition) extends WaitConditionOps(webhookWaitCondition) {
    private val LOGGER = Logger.getLogger(getClass.getName)

    override def checkCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Boolean = {
      val webhookOptions = connectionConfigByName.getOrElse(webhookWaitCondition.dataSourceName, Map())
      val request = asyncHttpClient().prepare(webhookWaitCondition.method, webhookWaitCondition.url)
      val authHeader = getAuthHeader(webhookOptions)
      val requestWithAuth = if (authHeader.nonEmpty) request.setHeader(authHeader.head._1, authHeader.head._2) else request

      LOGGER.debug(s"Attempting HTTP request, url=${webhookWaitCondition.url}")
      val tryResponse = Try(requestWithAuth.execute().get())

      tryResponse match {
        case Failure(exception) =>
          LOGGER.error(s"Failed to execute HTTP wait condition request, url=${webhookWaitCondition.url}", exception)
          false
        case Success(value) =>
          if (webhookWaitCondition.statusCodes.contains(value.getStatusCode)) {
            true
          } else {
            LOGGER.debug(s"HTTP wait condition status code did not match expected status code, url=${webhookWaitCondition.url}, " +
              s"expected-status-code=${webhookWaitCondition.statusCodes}, actual-status-code=${value.getStatusCode}, " +
              s"response-body=${value.getResponseBody}")
            false
          }
      }
    }
  }
}
