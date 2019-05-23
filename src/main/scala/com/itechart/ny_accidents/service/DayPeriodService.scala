package com.itechart.ny_accidents.service

import java.util.{Calendar, Date}

import com.itechart.ny_accidents.constants.GeneralConstants._
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.scraper.ContentExtractors.{attr, elementList, texts}

object DayPeriodService extends {

  val sunrisesSunsets: Map[Date, Seq[Date]] = parseSunrisesSunsets

  private def parseSunrisesSunsets: Map[Date, Seq[Date]] = {
    val browser = JsoupBrowser()
    (FIRST_MONTH_URL to LAST_MONTH_URL)
      .map(SUNRISES_SITE_URL_WITHOUT_MONTH_NUMBER + _)
      .map(browser.get)
      .map(_ >> elementList(HTML_ELEMENT_DAY_SELECTOR))
      .map(_.map(day => (day >> attr(HTML_ELEMENT_DATE_ATTRIBUTE)) +: (day >> texts(HTML_ELEMENT_TD)).toSeq.take(4)))
      .reduce(_ ++ _)
      .map(dates => {
        val dayStr = dates.head
        (DATE_SUNRISES_FORMAT.parse(dayStr),
          dates.drop(1)
            .map(dayStr + " " + _)
            .map(_.toUpperCase)
            .map(DATE_TIME_SUNRISES_FORMAT.parse))
      }).toMap
  }

  val defineLighting: Date => String = dateTime => {
    val hashDateCalendar = Calendar.getInstance()
    hashDateCalendar.setTime(dateTime)
    hashDateCalendar.set(Calendar.HOUR_OF_DAY, 0)
    hashDateCalendar.set(Calendar.MINUTE, 0)
    hashDateCalendar.set(Calendar.YEAR, SUNRISE_YEAR)
    val lightParameters = sunrisesSunsets(hashDateCalendar.getTime)

    val currentDateToCorrectYearCalendar = Calendar.getInstance()
    currentDateToCorrectYearCalendar.setTime(dateTime)
    currentDateToCorrectYearCalendar.set(Calendar.YEAR, SUNRISE_YEAR)
    val formattedDateTime = currentDateToCorrectYearCalendar.getTime
    if (formattedDateTime.before(lightParameters(TWILIGHT_START_C))) {
      NIGHT
    } else if (formattedDateTime.before(lightParameters(SUNRISE_C))) {
      MORNING_TWILIGHT
    } else if (formattedDateTime.before(lightParameters(SUNSET_C))) {
      DAY
    } else if (formattedDateTime.before(lightParameters(TWILIGHT_END_C))) {
      EVENING_TWILIGHT
    } else {
      NIGHT
    }
  }

}
