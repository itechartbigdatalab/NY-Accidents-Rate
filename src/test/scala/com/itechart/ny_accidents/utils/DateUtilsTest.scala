package com.itechart.ny_accidents.utils

import java.time.format.DateTimeFormatter

import org.scalatest.FunSpec

class DateUtilsTest extends FunSpec {
  // Will be rewrite. Test failed on different timezone machines
  // TODO need fix
  describe("DateUtils") {
    //    it("should return valid value") {
    //      val format: DateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm")
    //      val expectedResult = 1558095780000L
    //      val testValue = "17.05.2019 15:23"
    //
    //      assert(DateUtils.parseDateToMillis(testValue, format).contains(expectedResult))
    //    }

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