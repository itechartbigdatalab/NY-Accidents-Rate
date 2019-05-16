package com.itechart.ny_accidents.districts.parser

import com.itechart.ny_accidents.GeneralConstants
import com.itechart.ny_accidents.utils.DateUtils
import org.scalatest.FunSpec

class DateUtilsTest extends FunSpec {
  describe("DateUtils") {
    it("should return valid value") {
      val expectedResult = 1463187060000L
      val testValue = "14.05.2016 00:51"
      println(DateUtils.parseDate(testValue, GeneralConstants.DATE_TIME_FORMATTER_WEATHER).isDefined)

      assert(DateUtils.parseDate(testValue, GeneralConstants.DATE_TIME_FORMATTER_WEATHER).contains(expectedResult))
    }
  }
}
