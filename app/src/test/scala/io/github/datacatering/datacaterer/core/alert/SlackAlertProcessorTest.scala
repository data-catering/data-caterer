package io.github.datacatering.datacaterer.core.alert

import com.slack.api.Slack
import com.slack.api.methods.MethodsClient
import com.slack.api.methods.request.chat.ChatPostMessageRequest
import com.slack.api.methods.response.chat.ChatPostMessageResponse
import io.github.datacatering.datacaterer.api.model.{DataSourceResult, SinkResult, SlackAlertConfig, Step, Task, ValidationConfigResult}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class SlackAlertProcessorTest extends AnyFunSuiteLike with Matchers with MockFactory {

  val mockMethodsClient: MethodsClient = mock[MethodsClient]
  val slackAlertConfig = SlackAlertConfig("token", List("channel1", "channel2"))

  test("sendAlerts should send messages to all channels") {
    val mockSlack = mock[Slack]
    val processor = new SlackAlertProcessor(slackAlertConfig) {
      override protected def getSlackInstance: Slack = mockSlack
    }
    val generationResult = List(DataSourceResult("source1", Task(), Step(), SinkResult(), 10))
    val validationResults = List(ValidationConfigResult("validation1", "desc1", List(), LocalDateTime.now(), LocalDateTime.now()))
    val mockResponse = mock[ChatPostMessageResponse]
    (mockSlack.methods(_: String)).expects(*).returning(mockMethodsClient).once()
    (() => mockResponse.isOk).expects().returning(true).twice()
    (mockMethodsClient.chatPostMessage(_: ChatPostMessageRequest)).expects(*).returning(mockResponse).twice()

    processor.sendAlerts(generationResult, validationResults, Some("/path/to/report"))
  }

  test("sendAlerts should log error if message sending fails") {
    val mockSlack = mock[Slack]
    val processor = new SlackAlertProcessor(slackAlertConfig) {
      override protected def getSlackInstance: Slack = mockSlack
    }
    val generationResult = List(DataSourceResult("source1", Task(), Step(), SinkResult(), 10))
    val validationResults = List(ValidationConfigResult("validation1", "desc1", List(), LocalDateTime.now(), LocalDateTime.now()))
    val mockResponse = mock[ChatPostMessageResponse]
    (mockSlack.methods(_: String)).expects(*).returning(mockMethodsClient).once()
    (() => mockResponse.isOk).expects().returning(false).twice()
    (mockMethodsClient.chatPostMessage(_: ChatPostMessageRequest)).expects(*).returning(mockResponse).twice()
    (() => mockResponse.getError).expects().returning("error").twice()

    processor.sendAlerts(generationResult, validationResults, Some("/path/to/report"))
  }

  test("sendAlerts should log debug if token or channels are empty") {
    val emptyConfigProcessor = new SlackAlertProcessor(SlackAlertConfig("", List.empty))

    emptyConfigProcessor.sendAlerts(List.empty, List.empty, None)
  }
}
