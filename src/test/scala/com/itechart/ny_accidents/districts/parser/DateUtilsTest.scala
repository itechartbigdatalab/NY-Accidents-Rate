package com.itechart.ny_accidents.districts.parser

import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.utils.DateUtils
import org.scalatest.FunSpec

class DateUtilsTest extends FunSpec {
  describe("DateUtils") {
    it("should return valid value") {
      val expectedResult = 1558095780000L
      val testValue = "17.05.2019 15:23"
      println(DateUtils.parseDateToMillis(testValue, GeneralConstants.DATE_TIME_WEATHER_PATTERN).isDefined)

      assert(DateUtils.parseDateToMillis(testValue, GeneralConstants.DATE_TIME_WEATHER_PATTERN).contains(expectedResult))
    }
  }
}
