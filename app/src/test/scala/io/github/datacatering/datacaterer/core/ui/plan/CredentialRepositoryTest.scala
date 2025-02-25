package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.core.exception.{InvalidCredentialsException, UserNotFoundException}
import io.github.datacatering.datacaterer.core.ui.model.CredentialsRequest
import io.github.datacatering.datacaterer.core.ui.security.{CredentialsManager, DefaultCredentialsManager}
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class CredentialRepositoryTest extends AnyFunSuiteLike with Matchers with MockFactory {

  private val testKit = ActorTestKit()

  private val credentialsRequest = CredentialsRequest("username", "password")
  private val replyTo = testKit.createTestProbe[CredentialRepository.CredentialsResponse]()

  test("SaveCredentials should reply with VerifiedCredentialsResponse for valid credentials") {
    val credentialsManager = mock[CredentialsManager]
    (credentialsManager.validateCredentials _).expects(credentialsRequest).once().returns("valid")
    val credentialRepositoryWithManager = CredentialRepository
    credentialRepositoryWithManager.credentialsManager = credentialsManager

    val credentialRepository = testKit.spawn(credentialRepositoryWithManager())
    credentialRepository ! CredentialRepository.SaveCredentials(credentialsRequest, replyTo.ref)

    replyTo.expectMessage(CredentialRepository.VerifiedCredentialsResponse())
  }

  test("SaveCredentials should reply with InvalidCredentialsResponse for invalid credentials") {
    val credentialsManager = mock[CredentialsManager]
    (credentialsManager.validateCredentials _).expects(credentialsRequest).once().throws(InvalidCredentialsException("Invalid credentials"))
    val credentialRepositoryWithManager = CredentialRepository
    credentialRepositoryWithManager.credentialsManager = credentialsManager

    val credentialRepository = testKit.spawn(credentialRepositoryWithManager())
    credentialRepository ! CredentialRepository.SaveCredentials(credentialsRequest, replyTo.ref)

    replyTo.expectMessage(CredentialRepository.InvalidCredentialsResponse())
  }

  test("SaveCredentials should reply with UserNotFoundCredentialsResponse for non-existent user") {
    val credentialsManager = mock[CredentialsManager]
    (credentialsManager.validateCredentials _).expects(credentialsRequest).once().throws(UserNotFoundException("User not found"))
    val credentialRepositoryWithManager = CredentialRepository
    credentialRepositoryWithManager.credentialsManager = credentialsManager

    val credentialRepository = testKit.spawn(credentialRepositoryWithManager())
    credentialRepository ! CredentialRepository.SaveCredentials(credentialsRequest, replyTo.ref)

    replyTo.expectMessage(CredentialRepository.UserNotFoundCredentialsResponse())
  }

  test("SaveCredentials should reply with InternalErrorCredentialsResponse for other exceptions") {
    val credentialsManager = mock[CredentialsManager]
    (credentialsManager.validateCredentials _).expects(credentialsRequest).once().throws(new RuntimeException("Internal error"))
    val credentialRepositoryWithManager = CredentialRepository
    credentialRepositoryWithManager.credentialsManager = credentialsManager

    val credentialRepository = testKit.spawn(credentialRepositoryWithManager())
    credentialRepository ! CredentialRepository.SaveCredentials(credentialsRequest, replyTo.ref)

    replyTo.expectMessage(CredentialRepository.InternalErrorCredentialsResponse())
  }
}
