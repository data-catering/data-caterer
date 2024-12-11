package io.github.datacatering.datacaterer.core.parser

import com.google.inject.Guice
import io.github.datacatering.datacaterer.api.model.{BooleanType, ByteType, DataType, DecimalType, DoubleType, Field, FloatType, Generator, IntegerType, LongType, MapType, Schema, StringType, StructType}
import io.protostuff.compiler.ParserModule
import io.protostuff.compiler.model.{Message, ScalarFieldType, UserType}
import io.protostuff.compiler.parser.{Importer, LocalFileReader}

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

object ProtobufParser {

  def getSchemaFromProtoFile(filePath: Path, messageName: String): Schema = {
    val message = getMessageFromProtoFile(filePath, messageName)
    protoMessageToSchema(message)
  }

  def getMessageFromProtoString(protoString: String, messageName: String): Schema = {
    val tmpFile = Files.createTempFile("proto", s"$messageName.proto")
    Files.writeString(tmpFile, protoString)
    val message = getMessageFromProtoFile(tmpFile, messageName)
    protoMessageToSchema(message)
  }

  private def getMessageFromProtoFile(filePath: Path, messageName: String): Message = {
    val injector = Guice.createInjector(new ParserModule)
    val importer = injector.getInstance(classOf[Importer])
    val protoContext = importer.importFile(new LocalFileReader(filePath.getParent), filePath.getFileName.toString)

    protoContext.getProto.getMessage(messageName)
  }

  def protoMessageToSchema(message: Message): Schema = {
    if (message.getFields != null && !message.getFields.isEmpty) {
      val mappedFields = message.getFields.asScala.map(field => {
        val (dataType, opts) = field.getType match {
          case fieldType: ScalarFieldType =>
            val baseType = fieldType match {
              case ScalarFieldType.INT32 | ScalarFieldType.SINT32 |
                   ScalarFieldType.FIXED32 | ScalarFieldType.SFIXED32 => IntegerType
              case ScalarFieldType.UINT32 | ScalarFieldType.INT64 | ScalarFieldType.SINT64 |
                   ScalarFieldType.FIXED64 | ScalarFieldType.SFIXED64 => LongType
              case ScalarFieldType.UINT64 => DecimalType
              case ScalarFieldType.FLOAT => FloatType
              case ScalarFieldType.DOUBLE => DoubleType
              case ScalarFieldType.BOOL => BooleanType
              case ScalarFieldType.STRING => StringType
              case ScalarFieldType.BYTES => ByteType
            }
            (baseType, Map[String, String]())
          case userType: UserType =>
            if (userType.isEnum) {
              val baseEnum = if (userType.isNested) {
                userType.getParent.getEnum(userType.getName)
              } else {
                userType.getProto.getEnum(userType.getName)
              }
              val oneOf = baseEnum.getConstantNames.asScala.mkString(",")
              (StringType, Map("oneOf" -> oneOf))
            } else if (userType.isMessage && !field.isMap) {
              val baseMessage = if (userType.isNested) {
                userType.getParent.getMessage(userType.getName)
              } else {
                userType.getProto.getMessage(userType.getName)
              }
              val innerSchema = protoMessageToSchema(baseMessage)
              val innerFields = innerSchema.fields.getOrElse(List())
                .map(f => f.name -> DataType.fromString(f.`type`.getOrElse(StringType.toString)))
              (new StructType(innerFields), Map[String, String]())
            } else if (field.isMap) {
              val innerMessage = userType.asInstanceOf[Message]
              val innerSchema = protoMessageToSchema(innerMessage).fields.getOrElse(List())
              val keyType = innerSchema.find(f => f.name == "key")
                .map(_.`type`.getOrElse("string"))
                .getOrElse("string")
              val valueType = innerSchema.find(f => f.name == "value")
                .map(_.`type`.getOrElse("string"))
                .getOrElse("string")
              (new MapType(DataType.fromString(keyType), DataType.fromString(valueType)), Map[String, String]())
            } else {
              (StringType, Map[String, String]())
            }
          case _ =>
            //check if it is a map or array type
            if (field.isMap) {
              new MapType()
            } else if (field.isRepeated) {

            } else if (field.isOneofPart) {
              field.getOneof
            } else {
              StringType
            }
            (StringType, Map[String, String]())
        }
        Field(
          field.getName,
          Some(dataType.toString),
          Some(Generator(options = opts)),
          nullable = field.getModifier.name() == "OPTIONAL"
        )
      }).toList
      Schema(Some(mappedFields))
    } else {
      Schema(None)
    }
  }

}
