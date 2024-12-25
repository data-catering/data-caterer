package io.github.datacatering.datacaterer.core.util

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.{BoolValue, BytesValue, DescriptorProtos, DoubleValue, FloatValue, Int32Value, Int64Value, StringValue, UInt32Value, UInt64Value, WireFormat}
import io.github.datacatering.datacaterer.api.model.{ArrayType, BinaryType, BooleanType, DataType, DecimalType, DoubleType, Field, FloatType, IntegerType, LongType, MapType, StringType, StructType, TimestampType}
import io.github.datacatering.datacaterer.core.exception.UnsupportedProtobufType
import org.apache.log4j.Logger
import org.apache.spark.sql.protobuf.utils.SchemaConverters
import org.apache.spark.sql.types.{DataTypes, StructField}

import java.io.{BufferedInputStream, FileInputStream}
import scala.collection.JavaConverters.asScalaBufferConverter

object ProtobufUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def toStructType(descriptorFile: String): Map[String, StructType] = {
    val file = new BufferedInputStream(new FileInputStream(descriptorFile))
    val descriptorProto = DescriptorProtos.DescriptorProto.parseFrom(file)
    val fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(file)
//    fileDescriptorSet.getFileList.asScala
//      .flatMap(fd => {
//        fd.getMessageTypeList.asScala.toList.map(message => {
//          (message.getName, StructType(getSchemaFromFieldsProto(message.getFieldList.asScala.toList)))
//        })
//        //      (fd.getName, StructType(getSchemaFromFields(fd.getMessageTypeList.asScala.toList)))
//      }).toMap
    Map()
  }

//  private def getSchemaFromFields(fields: List[FieldDescriptor]): Array[StructField] = {
//    fields.map(field => {
//      val dataType = getDataTypeForField(field)
//      StructField(field.getName, dataType, !field.isRequired)
//    }).toArray
//  }
//
//  private def getSchemaFromFieldsProto(fields: List[FieldDescriptorProto]): Array[StructField] = {
//    fields.map(field => {
//      val dataType = getDataTypeForField(field)
//      StructField(field.getName, dataType)
//    }).toArray
//  }
//
//  private def getDataTypeForField(fieldDescriptor: FieldDescriptor): DataType = {
//    fieldDescriptor.getJavaType match {
//      case JavaType.BOOLEAN => DataTypes.BooleanType
//      case JavaType.INT => DataTypes.IntegerType
//      case JavaType.LONG => DataTypes.LongType
//      case JavaType.DOUBLE => DataTypes.DoubleType
//      case JavaType.FLOAT => DataTypes.FloatType
//      case JavaType.STRING => DataTypes.StringType
//      case JavaType.ENUM => DataTypes.StringType
//      case JavaType.BYTE_STRING => DataTypes.BinaryType
//      case JavaType.MESSAGE =>
//        new StructType(getSchemaFromFields(fieldDescriptor.getMessageType.getFields.asScala.toList))
//      case _ => throw new RuntimeException(s"Unable to parse proto type, type=${fieldDescriptor.getType}")
//    }
//  }
//
//  private def getDataTypeForField(fieldDescriptor: FieldDescriptorProto): DataType = {
//    //    val nonProtoField = FieldDescriptor.Type.valueOf(fieldDescriptor.getType)
//    FieldDescriptor.Type.valueOf(fieldDescriptor.getType).getJavaType match {
//      case JavaType.BOOLEAN => DataTypes.BooleanType
//      case JavaType.INT => DataTypes.IntegerType
//      case JavaType.LONG => DataTypes.LongType
//      case JavaType.DOUBLE => DataTypes.DoubleType
//      case JavaType.FLOAT => DataTypes.FloatType
//      case JavaType.STRING => DataTypes.StringType
//      case JavaType.ENUM => DataTypes.StringType
//      case JavaType.BYTE_STRING => DataTypes.BinaryType
//      case JavaType.MESSAGE =>
//        new StructType(getSchemaFromFields(fieldDescriptor.getDescriptorForType.getFields.asScala.toList))
//      case _ => throw new RuntimeException(s"Unable to parse proto type, type=${fieldDescriptor}")
//    }
//  }

  //comes from https://github.com/apache/spark/blob/master/connector/protobuf/src/main/scala/org/apache/spark/sql/protobuf/utils/SchemaConverters.scala#L68
  def fieldFromDescriptor(
                           fd: FieldDescriptor,
                           existingRecordNames: Map[String, Int]
                         ): Option[Field] = {
    import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._

    val dataType = fd.getJavaType match {
      // When the protobuf type is unsigned and upcastUnsignedIntegers has been set,
      // use a larger type (LongType and Decimal(20,0) for uint32 and uint64).
      case INT =>
        if (fd.getLiteType == WireFormat.FieldType.UINT32) {
          Some(LongType)
        } else {
          Some(IntegerType)
        }
      case LONG => if (fd.getLiteType == WireFormat.FieldType.UINT64) {
        Some(DecimalType)
      } else {
        Some(LongType)
      }
      case FLOAT => Some(FloatType)
      case DOUBLE => Some(DoubleType)
      case BOOLEAN => Some(BooleanType)
      case STRING => Some(StringType)
      case BYTE_STRING => Some(BinaryType)
      case ENUM => Some(StringType)
      case MESSAGE
        if fd.getMessageType.getName == "Duration" &&
          fd.getMessageType.getFields.size() == 2 &&
          fd.getMessageType.getFields.get(0).getName.equals("seconds") &&
          fd.getMessageType.getFields.get(1).getName.equals("nanos") =>
        LOGGER.warn(s"DateTimeInterval is not a supported data type, field-name=${fd.getFullName}")
        None
      case MESSAGE
        if fd.getMessageType.getName == "Timestamp" &&
          fd.getMessageType.getFields.size() == 2 &&
          fd.getMessageType.getFields.get(0).getName.equals("seconds") &&
          fd.getMessageType.getFields.get(1).getName.equals("nanos") =>
        Some(TimestampType)
      case MESSAGE if fd.getMessageType.getFullName == "google.protobuf.Any" =>
        Some(StringType)
      // Unwrap well known primitive wrapper types if the option has been set.
      case MESSAGE if fd.getMessageType.getFullName == BoolValue.getDescriptor.getFullName =>
        Some(BooleanType)
      case MESSAGE if fd.getMessageType.getFullName == Int32Value.getDescriptor.getFullName =>
        Some(IntegerType)
      case MESSAGE if fd.getMessageType.getFullName == UInt32Value.getDescriptor.getFullName =>
        Some(LongType)
      case MESSAGE if fd.getMessageType.getFullName == Int64Value.getDescriptor.getFullName =>
        Some(LongType)
      case MESSAGE if fd.getMessageType.getFullName == UInt64Value.getDescriptor.getFullName =>
        Some(DecimalType)
      case MESSAGE if fd.getMessageType.getFullName == StringValue.getDescriptor.getFullName =>
        Some(StringType)
      case MESSAGE if fd.getMessageType.getFullName == BytesValue.getDescriptor.getFullName =>
        Some(BinaryType)
      case MESSAGE if fd.getMessageType.getFullName == FloatValue.getDescriptor.getFullName =>
        Some(FloatType)
      case MESSAGE if fd.getMessageType.getFullName == DoubleValue.getDescriptor.getFullName =>
        Some(DoubleType)

      case MESSAGE if fd.isRepeated && fd.getMessageType.getOptions.hasMapEntry =>
        var keyType: Option[DataType] = None
        var valueType: Option[DataType] = None
        fd.getMessageType.getFields.forEach { field =>
          field.getName match {
            case "key" =>
              keyType =
                fieldFromDescriptor(
                  field,
                  existingRecordNames
                ).map(f => DataType.fromString(f.`type`.getOrElse("string")))
            case "value" =>
              valueType =
                fieldFromDescriptor(
                  field,
                  existingRecordNames
                ).map(f => DataType.fromString(f.`type`.getOrElse("string")))
          }
        }
        (keyType, valueType) match {
          case (None, _) =>
            // This is probably never expected. Protobuf does not allow complex types for keys.
            LOGGER.info(s"Dropping map field ${fd.getFullName}. Key reached max recursive depth.")
            None
          case (_, None) =>
            LOGGER.info(s"Dropping map field ${fd.getFullName}. Value reached max recursive depth.")
            None
          case (Some(kt), Some(vt)) => Some(new MapType(kt, vt))
        }
      case MESSAGE =>
        // If the `recursive.fields.max.depth` value is not specified, it will default to -1,
        // and recursive fields are not permitted. Setting it to 1 drops all recursive fields,
        // 2 allows it to be recursed once, and 3 allows it to be recursed twice and so on.
        // A value less than or equal to 0 or greater than 10 is not allowed, and if a protobuf
        // record has more depth for recursive fields than the allowed value, it will be truncated
        // and some fields may be discarded.
        // SQL Schema for protob2uf `message Person { string name = 1; Person bff = 2;}`
        // will vary based on the value of "recursive.fields.max.depth".
        // 1: struct<name: string>
        // 2: struct<name: string, bff: struct<name: string>>
        // 3: struct<name: string, bff: struct<name: string, bff: struct<name: string>>>
        // and so on.
        // TODO(rangadi): A better way to terminate would be replace the remaining recursive struct
        //      with the byte array of corresponding protobuf. This way no information is lost.
        //      i.e. with max depth 2, the above looks like this:
        //      struct<name: string, bff: struct<name: string, _serialized_bff: bytes>>
        val recordName = fd.getMessageType.getFullName
        val recursiveDepth = existingRecordNames.getOrElse(recordName, 0)
        val recursiveFieldMaxDepth = 2
        if (existingRecordNames.contains(recordName) &&
          recursiveDepth >= recursiveFieldMaxDepth) {
          // Recursive depth limit is reached. This field is dropped.
          // If it is inside a container like map or array, the containing field is dropped.
          LOGGER.info(
            s"The field ${fd.getFullName} of type $recordName is dropped " +
              s"at recursive depth $recursiveDepth"
          )
          None
        } else {
          val newRecordNames = existingRecordNames + (recordName -> (recursiveDepth + 1))
          val fields = fd.getMessageType.getFields.asScala.flatMap(
            fieldFromDescriptor(_, newRecordNames)
          ).toList
          fields match {
            case Nil =>
              LOGGER.info(
                s"Dropping ${fd.getFullName} as it does not have any fields left " +
                  "likely due to recursive depth limit."
              )
              None
            case fds => Some(new StructType(fds.map(f => f.name -> DataType.fromString(f.`type`.getOrElse("string")))))
          }
        }
      case other => throw UnsupportedProtobufType(other.toString)
    }
    dataType.map {
      case dt: MapType => Field(fd.getName, Some(dt.toString))
      case dt if fd.isRepeated => Field(fd.getName, Some(new ArrayType(dt).toString))
      case dt => Field(fd.getName, Some(dt.toString), nullable = !fd.isRequired)
    }
  }
}
