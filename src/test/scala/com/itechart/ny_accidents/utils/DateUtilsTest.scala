package com.itechart.ny_accidents.utils

import java.time.format.DateTimeFormatter

import org.scalatest.FunSpec

class DateUtilsTest extends FunSpec {

  describe("DateUtils") {
    it("should add one hour to date as long") {
      val testValue = 1558095780000L
      val expectedValue = 1558099380000L

      assert(DateUtils.addHour(testValue) == expectedValue)
    }

    it("should subtract one hour from date as long") {
      val testValue = 1558095780000L
      val expectedValue = 1558092180000L

      assert(DateUtils.subtractHour(testValue) == expectedValue)
    }
  }
}