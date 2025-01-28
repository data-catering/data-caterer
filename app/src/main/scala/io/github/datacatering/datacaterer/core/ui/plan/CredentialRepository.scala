package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.core.exception.{InvalidCredentialsException, UserNotFoundException}
import io.github.datacatering.datacaterer.core.ui.model.CredentialsRequest
import io.github.datacatering.datacaterer.core.ui.security.CredentialsManager
import org.apache.log4j.Logger
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}

import scala.util.{Failure, Success, Try}


object CredentialRepository {

  private val LOGGER = Logger.getLogger(getClass.getName)

  sealed trait CredentialsCommand

  final case class SaveCredentials(credentialsRequest: CredentialsRequest, replyTo: ActorRef[CredentialsResponse]) extends CredentialsCommand

  sealed trait CredentialsResponse

  final case class VerifiedCredentialsResponse() extends CredentialsResponse
  final case class InvalidCredentialsResponse() extends CredentialsResponse
  final case class UserNotFoundCredentialsResponse() extends CredentialsResponse
  final case class InternalErrorCredentialsResponse() extends CredentialsResponse


  def apply(): Behavior[CredentialsCommand] = {
    Behaviors.supervise[CredentialsCommand] {
      Behaviors.receiveMessage {
        case SaveCredentials(creds, replyTo) =>
          Try(saveCredentials(creds)) match {
            case Success(_) => replyTo ! VerifiedCredentialsResponse()
            case Failure(exception) =>
              exception match {
                case _: InvalidCredentialsException => replyTo ! InvalidCredentialsResponse()
                case _: UserNotFoundException => replyTo ! UserNotFoundCredentialsResponse()
                case _ => replyTo ! InternalErrorCredentialsResponse()
              }
          }
          Behaviors.same
      }
    }.onFailure(SupervisorStrategy.restart)
  }

  private def saveCredentials(credentialsRequest: CredentialsRequest): String = {
    LOGGER.debug("Checking if credentials are valid")
    CredentialsManager.validateCredentials(credentialsRequest)
  }
}
