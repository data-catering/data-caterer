package io.github.datacatering.datacaterer.core.validator

import io.github.datacatering.datacaterer.api.model.Constants.FORMAT
import io.github.datacatering.datacaterer.api.model.{DataExistsWaitCondition, FileExistsWaitCondition, PauseWaitCondition, WaitCondition, WebhookWaitCondition}
import io.github.datacatering.datacaterer.core.exception.InvalidWaitConditionException
import io.github.datacatering.datacaterer.core.util.HttpUtil.getAuthHeader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.asynchttpclient.Dsl.asyncHttpClient

import scala.util.{Failure, Success, Try}


object ValidationWaitImplicits {
  implicit class WaitConditionOps(waitCondition: WaitCondition = PauseWaitCondition()) {
    def checkCondition(implicit sparkSession: SparkSession): Boolean = true

    def checkCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Boolean = true

    def waitForCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Unit = {
      if (waitCondition.isRetryable) {
        var retries = 0
        while (retries < waitCondition.maxRetries) {
          val isDataAvailable = waitCondition match {
            case DataExistsWaitCondition(_, _, _) | WebhookWaitCondition(_, _, _, _) => this.checkCondition(connectionConfigByName)
            case FileExistsWaitCondition(_) => this.checkCondition
            case x => throw new InvalidWaitConditionException(x.getClass.getName)
          }
          if (!isDataAvailable) {
            Thread.sleep(waitCondition.waitBeforeRetrySeconds * 1000)
            retries += 1
          } else {
            return
          }
        }
      } else {
        this.checkCondition
      }
    }
  }

  implicit class PauseWaitConditionOps(pauseWaitCondition: PauseWaitCondition) extends WaitConditionOps(pauseWaitCondition) {
    override def checkCondition(implicit sparkSession: SparkSession): Boolean = {
      Thread.sleep(pauseWaitCondition.pauseInSeconds * 1000)
      true
    }
  }

  implicit class FileExistsWaitConditionOps(fileExistsWaitCondition: FileExistsWaitCondition) extends WaitConditionOps(fileExistsWaitCondition) {
    override def checkCondition(implicit sparkSession: SparkSession): Boolean = {
      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      fs.exists(new Path(fileExistsWaitCondition.path))
    }
  }

  implicit class DataExistsWaitConditionOps(dataExistsWaitCondition: DataExistsWaitCondition) extends WaitConditionOps(dataExistsWaitCondition) {
    override def checkCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Boolean = {
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
