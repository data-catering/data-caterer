package io.github.datacatering.datacaterer.api.converter

import java.util.Optional
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter, seqAsJavaListConverter}

object Converters {

  def toScalaMap[T, K](m: java.util.Map[T, K]): Map[T, K] = m.asScala.toMap

  def toScalaList[T](list: java.util.List[T]): List[T] = list.asScala.toList

  def toScalaSeq[T](list: java.util.List[T]): Seq[T] = list.asScala.toSeq

  def toScalaTuple[T, K](key: T, value: K): (T, K) = (key, value)

  def toScalaOption[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get()) else None

  def toJavaList[T](seq: Seq[T]): java.util.List[T] = seq.asJava
}
