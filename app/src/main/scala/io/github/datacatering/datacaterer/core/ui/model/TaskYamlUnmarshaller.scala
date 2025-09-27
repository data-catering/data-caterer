package io.github.datacatering.datacaterer.core.ui.model

import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, MediaTypes}
import org.apache.pekko.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object TaskYamlUnmarshaller {

  /**
   * Custom unmarshaller that can handle both JSON and raw YAML content for TaskYamlSampleRequest.
   * 
   * - For application/json: Deserializes the JSON directly to TaskYamlSampleRequest
   * - For text/plain or application/yaml: Treats content as raw YAML and creates TaskYamlSampleRequest 
   *   with the raw content, relying on query parameters for other fields
   */
  implicit val taskYamlSampleRequestUnmarshaller: FromEntityUnmarshaller[TaskYamlSampleRequest] = {
    Unmarshaller.withMaterializer { implicit ec: ExecutionContext => implicit mat =>
      entity: HttpEntity =>
        entity.toStrict(3.seconds) flatMap { strictEntity => // Use 3 second timeout
          val contentType = entity.contentType
          val data = strictEntity.data.utf8String
          
          Future {
            val mediaType = contentType.mediaType
            if (mediaType == MediaTypes.`application/json`) {
              // Use Jackson to deserialize JSON to TaskYamlSampleRequest
              Try {
                ObjectMapperUtil.jsonObjectMapper.readValue(data, classOf[TaskYamlSampleRequest])
              }
            } else if (mediaType == MediaTypes.`text/plain` || mediaType.toString.contains("yaml")) {
              // Treat as raw YAML content - create TaskYamlSampleRequest with raw content
              // Query parameters will be handled separately in the route
              Try {
                TaskYamlSampleRequest(
                  taskYamlContent = data,
                  stepName = None, // Will be overridden by query parameters
                  sampleSize = 10, // Will be overridden by query parameters  
                  fastMode = true // Will be overridden by query parameters
                )
              }
            } else {
              Failure(new IllegalArgumentException(
                s"Unsupported content type: $mediaType. Supported: application/json, text/plain, application/yaml"))
            }
          } flatMap {
            case Success(result) => Future.successful(result)
            case Failure(ex) => Future.failed(ex)
          }
        }
    }
  }
}