package com.itechart.ny_accidents.utils

import org.scalatest.FunSpec

class NumberUtilsTest extends FunSpec {
  describe("A NumberUtils") {
    it("should return None if double like positive or negative infinity") {
      val testData = Double.NegativeInfinity

      assert(NumberUtils.validateDouble(testData).isEmpty)
    }

    it("should return Some(number) if double isn't like infinity") {
      val testData = 123.1

      assert(NumberUtils.validateDouble(testData).contains(123.1))
    }
  }
}
