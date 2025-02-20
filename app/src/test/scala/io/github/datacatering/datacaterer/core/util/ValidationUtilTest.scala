package io.github.datacatering.datacaterer.core.util

import org.scalatest.funsuite.AnyFunSuiteLike

class ValidationUtilTest extends AnyFunSuiteLike {

  test("Can remove brackets from identifier for folder pathway") {
    val result = ValidationUtil.cleanValidationIdentifier("/my/path/{id}")
    assertResult("/my/path/id")(result)
  }
}
