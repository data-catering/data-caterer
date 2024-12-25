package io.github.datacatering.datacaterer.core.parser

import com.google.inject.Guice
import io.github.datacatering.datacaterer.api.model.{ArrayType, BooleanType, ByteType, DataType, DecimalType, DoubleType, Field, FloatType, IntegerType, LongType, MapType, StringType, StructType}
import io.github.datacatering.datacaterer.core.exception.InvalidNumberOfProtobufMessages
import io.protostuff.compiler.ParserModule
import io.protostuff.compiler.model.{Message, ScalarFieldType, UserType}
import io.protostuff.compiler.parser.{Importer, LocalFileReader}

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

object ProtobufParser {

  def getSchemaFromProtoFile(filePath: Path): List[Field] = {
    val message = getMessageFromProtoFile(filePath)
    protoMessageToSchema(message)
  }

  def getSchemaFromProtoString(protoString: String, schemaName: String): List[Field] = {
    val tmpFile = Files.createTempFile("proto", s"$schemaName.proto")
    Files.writeString(tmpFile, protoString)
    val message = getMessageFromProtoFile(tmpFile)
    protoMessageToSchema(message)
  }

  private def getMessageFromProtoFile(filePath: Path): Message = {
    val injector = Guice.createInjector(new ParserModule)
    val importer = injector.getInstance(classOf[Importer])
    val protoContext = importer.importFile(new LocalFileReader(filePath.getParent), filePath.getFileName.toString)
    val protoMessages = protoContext.getProto.getMessages
    if (protoMessages.isEmpty || protoMessages.size() > 1) {
      throw InvalidNumberOfProtobufMessages(filePath.getFileName.toString)
    } else {
      protoMessages.get(0)
    }
  }

  private def protoMessageToSchema(message: Message): List[Field] = {
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
              val innerFields = innerSchema.map(f => f.name -> DataType.fromString(f.`type`.getOrElse(StringType.toString)))
              (new StructType(innerFields), Map[String, String]())
            } else if (field.isMap) {
              val innerMessage = userType.asInstanceOf[Message]
              val innerSchema = protoMessageToSchema(innerMessage)
              val keyType = getInnerDataType(innerSchema, "key")
              val valueType = getInnerDataType(innerSchema, "value")
              (new MapType(DataType.fromString(keyType), DataType.fromString(valueType)), Map[String, String]())
            } else {
              (StringType, Map[String, String]())
            }
          case _ => (StringType, Map[String, String]())
        }

        val finalDataType = if (field.hasModifier && !field.isMap && field.getModifier.name().equalsIgnoreCase("repeated")) {
          new ArrayType(dataType)
        } else {
          dataType
        }
        Field(
          field.getName,
          Some(finalDataType.toString),
          opts,
          nullable = field.getModifier.name() == "OPTIONAL"
        )
      }).toList
      mappedFields
    } else {
      List()
    }
  }

  private def getInnerDataType(innerSchema: List[Field], fieldName: String): String = {
    innerSchema.find(f => f.name == fieldName)
      .map(_.`type`.getOrElse("string"))
      .getOrElse("string")
  }
}
