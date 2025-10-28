package io.github.datacatering.datacaterer.core.ui.http

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}

import java.time.Instant

/**
 * Structured error response model
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class ErrorResponse(
  status: Int,
  error: String,
  message: String,
  details: Option[String] = None,
  timestamp: String = Instant.now().toString,
  path: Option[String] = None
)

/**
 * Pekko HTTP compatible error response helpers
 */
object PekkoErrorResponse {

  import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil

  /**
   * Create a standardized error response
   */
  def errorResponse(
    statusCode: org.apache.pekko.http.scaladsl.model.StatusCode,
    error: String,
    message: String,
    details: Option[String] = None,
    path: Option[String] = None
  ): HttpResponse = {
    val errorObj = ErrorResponse(
      status = statusCode.intValue(),
      error = error,
      message = message,
      details = details,
      path = path
    )

    val json = ObjectMapperUtil.jsonObjectMapper.writeValueAsString(errorObj)
    HttpResponse(
      status = statusCode,
      entity = HttpEntity(ContentTypes.`application/json`, json)
    )
  }

  /**
   * 400 Bad Request
   */
  def badRequest(message: String, details: Option[String] = None, path: Option[String] = None): HttpResponse = {
    errorResponse(StatusCodes.BadRequest, "Bad Request", message, details, path)
  }

  /**
   * 401 Unauthorized
   */
  def unauthorized(message: String = "Authentication required", details: Option[String] = None, path: Option[String] = None): HttpResponse = {
    errorResponse(StatusCodes.Unauthorized, "Unauthorized", message, details, path)
  }

  /**
   * 403 Forbidden
   */
  def forbidden(message: String = "Access denied", details: Option[String] = None, path: Option[String] = None): HttpResponse = {
    errorResponse(StatusCodes.Forbidden, "Forbidden", message, details, path)
  }

  /**
   * 404 Not Found
   */
  def notFound(resource: String, details: Option[String] = None, path: Option[String] = None): HttpResponse = {
    errorResponse(StatusCodes.NotFound, "Not Found", s"$resource not found", details, path)
  }

  /**
   * 409 Conflict
   */
  def conflict(message: String, details: Option[String] = None, path: Option[String] = None): HttpResponse = {
    errorResponse(StatusCodes.Conflict, "Conflict", message, details, path)
  }

  /**
   * 422 Unprocessable Entity (validation error)
   */
  def validationError(message: String, details: Option[String] = None, path: Option[String] = None): HttpResponse = {
    errorResponse(StatusCodes.UnprocessableEntity, "Validation Error", message, details, path)
  }

  /**
   * 429 Too Many Requests
   */
  def tooManyRequests(message: String = "Rate limit exceeded", details: Option[String] = None, path: Option[String] = None): HttpResponse = {
    errorResponse(StatusCodes.TooManyRequests, "Too Many Requests", message, details, path)
  }

  /**
   * 500 Internal Server Error
   */
  def internalError(message: String = "An internal error occurred", details: Option[String] = None, path: Option[String] = None): HttpResponse = {
    errorResponse(StatusCodes.InternalServerError, "Internal Server Error", message, details, path)
  }

  /**
   * 503 Service Unavailable
   */
  def serviceUnavailable(message: String = "Service temporarily unavailable", details: Option[String] = None, path: Option[String] = None): HttpResponse = {
    errorResponse(StatusCodes.ServiceUnavailable, "Service Unavailable", message, details, path)
  }

  /**
   * Create error from exception
   */
  def fromException(
    ex: Throwable,
    statusCode: org.apache.pekko.http.scaladsl.model.StatusCode = StatusCodes.InternalServerError,
    path: Option[String] = None,
    includeStackTrace: Boolean = false
  ): HttpResponse = {
    val details = if (includeStackTrace) {
      Some(ex.getStackTrace.take(5).mkString("\n"))
    } else {
      Option(ex.getMessage)
    }

    errorResponse(
      statusCode,
      ex.getClass.getSimpleName,
      Option(ex.getMessage).getOrElse("An error occurred"),
      details,
      path
    )
  }
}
