package com.itechart.accidents.service

import com.itechart.accidents.constants.GeneralConstants
import com.itechart.accidents.integration.ny.service.DayPeriodService
import com.itechart.accidents.utils.DateUtils
import org.scalatest.FunSpec

class DayPeriodServiceTest extends FunSpec {

  private val testSubject = DayPeriodService

  describe("Reports") {

    it("should return Day for day date") {
      val testData = DateUtils.parseDate("05/15/2016 14:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val expectedValue = GeneralConstants.DAY
      assert(testSubject.defineLighting(testData.get) == expectedValue)
    }

    it("should return Night for date before midnight") {
      val testData = DateUtils.parseDate("05/15/2016 0:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val expectedValue = GeneralConstants.NIGHT
      assert(testSubject.defineLighting(testData.get) == expectedValue)
    }

    it("should return Night for date after midnight") {
      val testData = DateUtils.parseDate("05/15/2016 23:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val expectedValue = GeneralConstants.NIGHT
      assert(testSubject.defineLighting(testData.get) == expectedValue)
    }

    it("should return Morning Twilight for morning date") {
      val testData = DateUtils.parseDate("05/15/2016 5:15", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val expectedValue = GeneralConstants.MORNING_TWILIGHT
      assert(testSubject.defineLighting(testData.get) == expectedValue)
    }

    it("should return Evening Twilight for evening date") {
      val testData = DateUtils.parseDate("05/15/2016 20:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val expectedValue = GeneralConstants.EVENING_TWILIGHT
      assert(testSubject.defineLighting(testData.get) == expectedValue)

    }
  }

}