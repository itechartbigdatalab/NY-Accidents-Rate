package com.itechart.ny_accidents.utils

import org.scalatest.FunSpec

class StringUtilsTest extends FunSpec {
  describe("A StringUtils") {
    it("should return approximate value for strings equality") {
      val firstString = "qwerty"
      val secondString = "qwert y"

      assert(StringUtils.getLineMatchPercentage(firstString, secondString) > .7)
    }

    it("should return 100% if strings is equals") {
      val firstString = "qwerty"
      val secondString = "qwerty"

      assert(StringUtils.getLineMatchPercentage(firstString, secondString) == 1.0)
    }

    it("should return 0 if strings completely different") {
      val firstString = "qwerty"
      val secondString = "xzczxcxzc"

      assert(StringUtils.getLineMatchPercentage(firstString, secondString) == 0.0)
    }

    it("should return wind string to double without dot") {
      val testString = "1234.56"
      val expectedValue = 123456

      assert(StringUtils.windStringToDoubleParse(testString).contains(expectedValue))
    }

    it("should return None if string is empty") {
      val testString = ""

      assert(StringUtils.windStringToDoubleParse(testString).isEmpty)
    }
  }
}