//package io.github.datacatering.datacaterer.core.util
//
//import com.google.protobuf.DescriptorProtos
//import com.google.protobuf.DescriptorProtos.FieldDescriptorProto
//import com.google.protobuf.Descriptors.FieldDescriptor
//import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
//import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
//
//import java.io.{BufferedInputStream, FileInputStream}
//import scala.collection.JavaConverters.asScalaBufferConverter
//
//object ProtobufUtil {
//
//  def toStructType(descriptorFile: String): Map[String, StructType] = {
//    val file = new BufferedInputStream(new FileInputStream(descriptorFile))
//    val fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(file)
//    fileDescriptorSet.getFileList.asScala
//      .flatMap(fd => {
//        fd.getMessageTypeList.asScala.toList.map(message => {
//          (message.getName, StructType(getSchemaFromFieldsProto(message.getFieldList.asScala.toList)))
//        })
//        //      (fd.getName, StructType(getSchemaFromFields(fd.getMessageTypeList.asScala.toList)))
//      }).toMap
//  }
//
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
//      case JavaType.MESSAGE => {
//        new StructType(getSchemaFromFields(fieldDescriptor.getMessageType.getFields.asScala.toList))
//      }
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
//      case JavaType.MESSAGE => {
//        new StructType(getSchemaFromFields(fieldDescriptor.getDescriptorForType.getFields.asScala.toList))
//      }
//      case _ => throw new RuntimeException(s"Unable to parse proto type, type=${fieldDescriptor}")
//    }
//  }
//
//}
