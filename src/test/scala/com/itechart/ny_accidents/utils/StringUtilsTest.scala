package com.itechart.ny_accidents.utils

import org.scalatest.FunSpec

class StringUtilsTest extends FunSpec {
  describe("A StringUtils") {
    it("should return approximate value for strings equality") {
      val firstString = "qwerty"
      val secondString = "qwert y"

      assert(StringUtils.getLineMatchPercentage(firstString, secondString) > 70.0)
    }

    it("should return 100% if strings is equals") {
      val firstString = "qwerty"
      val secondString = "qwerty"

      assert(StringUtils.getLineMatchPercentage(firstString, secondString) == 100.0)
    }

    it("should return 0 if strings completely different") {
      val firstString = "qwerty"
      val secondString = "xzczxcxzc"

      assert(StringUtils.getLineMatchPercentage(firstString, secondString) == 0)
    }
  }
}
