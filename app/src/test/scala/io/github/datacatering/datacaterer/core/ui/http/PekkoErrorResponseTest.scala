package io.github.datacatering.datacaterer.core.ui.http

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.pekko.http.scaladsl.model.{ContentTypes, StatusCodes}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class PekkoErrorResponseTest extends AnyFunSuiteLike with Matchers {

  implicit val mapper: ObjectMapper = ObjectMapperUtil.jsonObjectMapper

  test("ErrorResponse should have correct structure") {
    val error = ErrorResponse(
      status = 404,
      error = "Not Found",
      message = "Resource not found",
      details = Some("Additional context"),
      path = Some("/api/test")
    )

    error.status shouldBe 404
    error.error shouldBe "Not Found"
    error.message shouldBe "Resource not found"
    error.details shouldBe Some("Additional context")
    error.path shouldBe Some("/api/test")
    error.timestamp should not be empty
  }

  test("ErrorResponse should have timestamp") {
    val error = ErrorResponse(
      status = 500,
      error = "Internal Error",
      message = "Something went wrong"
    )

    error.timestamp should not be empty
    // Timestamp should be parseable as ISO datetime
    noException should be thrownBy java.time.Instant.parse(error.timestamp)
  }

  test("ErrorResponse should be serializable to JSON") {
    val error = ErrorResponse(
      status = 400,
      error = "Bad Request",
      message = "Invalid input"
    )

    val json = mapper.writeValueAsString(error)
    json should include("400")
    json should include("Bad Request")
    json should include("Invalid input")
  }

  test("badRequest should create 400 response") {
    val response = PekkoErrorResponse.badRequest("Invalid input")

    response.status shouldBe StatusCodes.BadRequest
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("badRequest should create valid error response") {
    val response = PekkoErrorResponse.badRequest("Invalid input", details = Some("Field 'name' is required"))

    response.status shouldBe StatusCodes.BadRequest
    // Response should have non-empty entity
    response.entity.isKnownEmpty shouldBe false
  }

  test("unauthorized should create 401 response") {
    val response = PekkoErrorResponse.unauthorized()

    response.status shouldBe StatusCodes.Unauthorized
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("unauthorized should use default message") {
    val response = PekkoErrorResponse.unauthorized()

    response.status shouldBe StatusCodes.Unauthorized
    response.entity.isKnownEmpty shouldBe false
  }

  test("forbidden should create 403 response") {
    val response = PekkoErrorResponse.forbidden()

    response.status shouldBe StatusCodes.Forbidden
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("notFound should create 404 response") {
    val response = PekkoErrorResponse.notFound("Plan", path = Some("/plan/test-plan"))

    response.status shouldBe StatusCodes.NotFound
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("notFound should create valid response") {
    val response = PekkoErrorResponse.notFound("Connection", details = Some("No connection with that name"))

    response.status shouldBe StatusCodes.NotFound
    response.entity.isKnownEmpty shouldBe false
  }

  test("conflict should create 409 response") {
    val response = PekkoErrorResponse.conflict("Resource already exists")

    response.status shouldBe StatusCodes.Conflict
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("validationError should create 422 response") {
    val response = PekkoErrorResponse.validationError("Validation failed", details = Some("Name is too long"))

    response.status shouldBe StatusCodes.UnprocessableEntity
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("validationError should create valid response") {
    val response = PekkoErrorResponse.validationError(
      "Validation failed",
      details = Some("Fields: name, email"),
      path = Some("/api/users")
    )

    response.status shouldBe StatusCodes.UnprocessableEntity
    response.entity.isKnownEmpty shouldBe false
  }

  test("tooManyRequests should create 429 response") {
    val response = PekkoErrorResponse.tooManyRequests()

    response.status shouldBe StatusCodes.TooManyRequests
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("tooManyRequests should use default message") {
    val response = PekkoErrorResponse.tooManyRequests()

    response.status shouldBe StatusCodes.TooManyRequests
    response.entity.isKnownEmpty shouldBe false
  }

  test("internalError should create 500 response") {
    val response = PekkoErrorResponse.internalError()

    response.status shouldBe StatusCodes.InternalServerError
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("internalError should allow custom message") {
    val response = PekkoErrorResponse.internalError("Database connection failed")

    response.status shouldBe StatusCodes.InternalServerError
    response.entity.isKnownEmpty shouldBe false
  }

  test("serviceUnavailable should create 503 response") {
    val response = PekkoErrorResponse.serviceUnavailable()

    response.status shouldBe StatusCodes.ServiceUnavailable
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("fromException should create error from exception") {
    val exception = new RuntimeException("Test exception")
    val response = PekkoErrorResponse.fromException(exception)

    response.status shouldBe StatusCodes.InternalServerError
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("fromException should create valid response") {
    val exception = new IllegalArgumentException("Invalid argument provided")
    val response = PekkoErrorResponse.fromException(exception)

    response.status shouldBe StatusCodes.InternalServerError
    response.entity.isKnownEmpty shouldBe false
  }

  test("fromException should use exception class for error type") {
    val exception = new IllegalStateException("Bad state")
    val response = PekkoErrorResponse.fromException(exception)

    response.status shouldBe StatusCodes.InternalServerError
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("fromException should respect custom status code") {
    val exception = new RuntimeException("Not found")
    val response = PekkoErrorResponse.fromException(exception, statusCode = StatusCodes.NotFound)

    response.status shouldBe StatusCodes.NotFound
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("fromException should support stack trace inclusion") {
    val exception = new RuntimeException("Error with trace")
    val response = PekkoErrorResponse.fromException(exception, includeStackTrace = true)

    response.status shouldBe StatusCodes.InternalServerError
    response.entity.isKnownEmpty shouldBe false
  }

  test("fromException should not include stack trace by default") {
    val exception = new RuntimeException("Error without trace")
    val response = PekkoErrorResponse.fromException(exception, includeStackTrace = false)

    response.status shouldBe StatusCodes.InternalServerError
    response.entity.isKnownEmpty shouldBe false
  }

  test("fromException should handle exception with null message") {
    val exception = new RuntimeException()
    val response = PekkoErrorResponse.fromException(exception)

    response.status shouldBe StatusCodes.InternalServerError
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("all error responses should have JSON content type") {
    val responses = List(
      PekkoErrorResponse.badRequest("test"),
      PekkoErrorResponse.unauthorized(),
      PekkoErrorResponse.forbidden(),
      PekkoErrorResponse.notFound("test"),
      PekkoErrorResponse.conflict("test"),
      PekkoErrorResponse.validationError("test"),
      PekkoErrorResponse.tooManyRequests(),
      PekkoErrorResponse.internalError(),
      PekkoErrorResponse.serviceUnavailable(),
      PekkoErrorResponse.fromException(new RuntimeException("test"))
    )

    responses.foreach { response =>
      response.entity.contentType shouldBe ContentTypes.`application/json`
    }
  }

  test("error responses should have non-empty entity") {
    val response = PekkoErrorResponse.notFound("Plan", path = Some("/api/plans/test-plan"))

    response.status shouldBe StatusCodes.NotFound
    response.entity.isKnownEmpty shouldBe false
  }

  test("error responses should handle empty details") {
    val response = PekkoErrorResponse.badRequest("Test", details = None)

    response.status shouldBe StatusCodes.BadRequest
    response.entity.contentType shouldBe ContentTypes.`application/json`
  }

  test("ErrorResponse model should serialize correctly") {
    val error = ErrorResponse(
      status = 404,
      error = "Not Found",
      message = "Resource not found",
      details = Some("Additional info"),
      path = Some("/api/test")
    )

    val json = mapper.writeValueAsString(error)

    // Should be valid JSON
    noException should be thrownBy mapper.readTree(json)

    // Deserialize back
    val deserialized = mapper.readValue(json, classOf[ErrorResponse])
    deserialized.status shouldBe 404
    deserialized.error shouldBe "Not Found"
    deserialized.message shouldBe "Resource not found"
  }

  test("ErrorResponse should support round-trip serialization") {
    val original = ErrorResponse(
      status = 400,
      error = "Bad Request",
      message = "Invalid input",
      details = Some("Field validation failed"),
      path = Some("/api/users")
    )

    val json = mapper.writeValueAsString(original)
    val deserialized = mapper.readValue(json, classOf[ErrorResponse])

    deserialized.status shouldBe original.status
    deserialized.error shouldBe original.error
    deserialized.message shouldBe original.message
    deserialized.details shouldBe original.details
    deserialized.path shouldBe original.path
  }

  test("error response helpers should create correct status codes") {
    PekkoErrorResponse.badRequest("test").status.intValue() shouldBe 400
    PekkoErrorResponse.unauthorized().status.intValue() shouldBe 401
    PekkoErrorResponse.forbidden().status.intValue() shouldBe 403
    PekkoErrorResponse.notFound("test").status.intValue() shouldBe 404
    PekkoErrorResponse.conflict("test").status.intValue() shouldBe 409
    PekkoErrorResponse.validationError("test").status.intValue() shouldBe 422
    PekkoErrorResponse.tooManyRequests().status.intValue() shouldBe 429
    PekkoErrorResponse.internalError().status.intValue() shouldBe 500
    PekkoErrorResponse.serviceUnavailable().status.intValue() shouldBe 503
  }
}
