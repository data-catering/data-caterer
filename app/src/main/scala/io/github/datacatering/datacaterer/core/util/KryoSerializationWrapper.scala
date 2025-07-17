package io.github.datacatering.datacaterer.core.util

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkEnv}

import java.nio.ByteBuffer
import scala.reflect.ClassTag

class KryoSerializationWrapper[T: ClassTag] extends Serializable {

  @transient var value: T = _

  private var valueSerialized: Array[Byte] = _

  // The getter and setter for valueSerialized is used for XML serialization.
  def getValueSerialized(): Array[Byte] = {
    valueSerialized = BaseKryoSerializer.serialize(value)
    valueSerialized
  }

  def setValueSerialized(bytes: Array[Byte]) = {
    valueSerialized = bytes
    value = BaseKryoSerializer.deserialize[T](valueSerialized)
  }

  // Used for Java serialization.
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    getValueSerialized()
    out.defaultWriteObject()
  }

  private def readObject(in: java.io.ObjectInputStream): Unit = {
    in.defaultReadObject()
    setValueSerialized(valueSerialized)
  }
}


object KryoSerializationWrapper {
  def apply[T: ClassTag](value: T): KryoSerializationWrapper[T] = {
    val wrapper = new KryoSerializationWrapper[T]
    wrapper.value = value
    wrapper
  }
}

object BaseKryoSerializer {

  @transient lazy val ser: KryoSerializer = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new KryoSerializer(sparkConf)
  }

  def serialize[T: ClassTag](o: T): Array[Byte] = {
    ser.newInstance().serialize(o).array()
  }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T  = {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
  }
}
