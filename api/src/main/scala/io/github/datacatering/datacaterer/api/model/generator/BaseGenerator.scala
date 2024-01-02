package io.github.datacatering.datacaterer.api.model.generator

trait BaseGenerator[T] {
  val options: Map[String, Any] = Map()

  val edgeCases: List[T] = List()

  def generateSqlExpression: String = ""

  //TODO how to set default for generic trait method
  def generate: T
}
