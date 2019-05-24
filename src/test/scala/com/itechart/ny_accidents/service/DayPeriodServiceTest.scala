package com.itechart.ny_accidents.service

import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.utils.DateUtils
import org.scalatest.FunSpec

class DayPeriodServiceTest extends FunSpec {

  private val testSubject = DayPeriodService

  describe("Reports") {
    it("should return correct day phase") {
      val dateDay = DateUtils.parseDate("05/15/2016 14:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
      val dateNight1 = DateUtils.parseDate("05/15/2016 0:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
      val dateNight2 = DateUtils.parseDate("05/15/2016 23:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
      val dateMorningTwilight = DateUtils.parseDate("05/15/2016 5:15", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
      val dataEveningTwilight = DateUtils.parseDate("05/15/2016 20:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)

      val expectedResultDay = GeneralConstants.DAY
      val expectedResultEveningTwilight = GeneralConstants.EVENING_TWILIGHT
      val expectedResultMorningTwilight = GeneralConstants.MORNING_TWILIGHT
      val expectedResultNight = GeneralConstants.NIGHT

      assert(testSubject.defineLighting(dateDay.get) == expectedResultDay)
      assert(testSubject.defineLighting(dateNight1.get) == expectedResultNight)
      assert(testSubject.defineLighting(dateNight2.get) == expectedResultNight)
      assert(testSubject.defineLighting(dateMorningTwilight.get) == expectedResultMorningTwilight)
      assert(testSubject.defineLighting(dataEveningTwilight.get) == expectedResultEveningTwilight)
    }
  }

}