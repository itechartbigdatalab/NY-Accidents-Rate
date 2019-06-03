package com.itechart.ny_accidents.utils

import java.sql.Date
import java.time.format.DateTimeFormatter
import java.util.Calendar

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

    it("should correctly calculate hash of date") {
      val testValue = 1558095790000L
      val expectedValue = 1558094400000L

      assert(DateUtils.hashByDate(testValue) == expectedValue)
    }

    it("should parse date string to sql Date") {
      val testValue = "05/02/2019"
      val formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")
      val resultValue = DateUtils.parseSqlDate(testValue, formatter)

      assert(resultValue.isDefined)
      val resultCalendar = Calendar.getInstance()

      resultCalendar.setTime(resultValue.get)
      assert(resultCalendar.get(Calendar.YEAR) == 2019 &&
        resultCalendar.get(Calendar.MONTH) == 4 &&
        resultCalendar.get(Calendar.DAY_OF_MONTH) == 2)
    }
  }
}