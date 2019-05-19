package com.itechart.ny_accidents.utils

import com.itechart.ny_accidents.constants.GeneralConstants
import org.scalatest.FunSpec

class DateUtilsTest extends FunSpec {
  describe("DateUtils") {
    it("should return valid value") {
      val expectedResult = 1558095780000L
      val testValue = "17.05.2019 15:23"
<<<<<<< HEAD:src/test/scala/com/itechart/ny_accidents/utils/DateUtilsTest.scala
=======
      println(DateUtils.parseDateToMillis(testValue, GeneralConstants.DATE_TIME_WEATHER_PATTERN).isDefined)
>>>>>>> master:src/test/scala/com/itechart/ny_accidents/districts/parser/DateUtilsTest.scala

      assert(DateUtils.parseDateToMillis(testValue, GeneralConstants.DATE_TIME_WEATHER_PATTERN).contains(expectedResult))
    }
  }
}
