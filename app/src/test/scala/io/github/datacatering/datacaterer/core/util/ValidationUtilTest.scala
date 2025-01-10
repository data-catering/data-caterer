package io.github.datacatering.datacaterer.core.util

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ValidationUtilTest extends AnyFunSuiteLike {

  test("Can remove brackets from identifier for folder pathway") {
    val result = ValidationUtil.cleanValidationIdentifier("/my/path/{id}")
    assertResult("/my/path/id")(result)
  }
}
