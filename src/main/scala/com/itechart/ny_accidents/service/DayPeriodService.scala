package com.itechart.ny_accidents.service

import java.time.{LocalDate, LocalDateTime}
import java.time.temporal.ChronoUnit

import com.itechart.ny_accidents.constants.GeneralConstants._
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.scraper.ContentExtractors.{attr, elementList, texts}

object DayPeriodService extends {

  private val sunrisesSunsets: Map[LocalDate, Seq[LocalDateTime]] = parseSunrisesSunsets


  private def parseSunrisesSunsets: Map[LocalDate, Seq[LocalDateTime]] = {
    val browser = JsoupBrowser()
    (FIRST_MONTH_URL to LAST_MONTH_URL)
      .map(SUNRISES_SITE_URL_WITHOUT_MONTH_NUMBER + _)
      .map(browser.get)
      .map(_ >> elementList(HTML_ELEMENT_DAY_SELECTOR))
      .map(_.map(day => (day >> attr(HTML_ELEMENT_DATE_ATTRIBUTE)) +: (day >> texts(HTML_ELEMENT_TD)).toSeq.take(4)))
      .reduce(_ ++ _)
      .map(dates => {
        val dayStr = dates.head
        (LocalDate.parse(dayStr, DATE_SUNRISES_FORMATTER),
          dates.drop(1)
            .map(dayStr + " " + _)
            .map(_.toUpperCase)
            .map(LocalDateTime.parse(_, DATE_TIME_SUNRISES_FORMAT)))
      }).toMap
  }

  val defineLighting: LocalDateTime => String = dateTime => {
    val hashDateTime = dateTime.truncatedTo(ChronoUnit.HOURS).withYear(SUNRISE_YEAR)
    val lightParameters = sunrisesSunsets(hashDateTime.toLocalDate)
    val formattedDateTime: LocalDateTime = dateTime.withYear(SUNRISE_YEAR)

    if (formattedDateTime.isBefore(lightParameters(TWILIGHT_START_C))) {
      NIGHT
    } else if (formattedDateTime.isBefore(lightParameters(SUNRISE_C))) {
      MORNING_TWILIGHT
    } else if (formattedDateTime.isBefore(lightParameters(SUNSET_C))) {
      DAY
    } else if (formattedDateTime.isBefore(lightParameters(TWILIGHT_END_C))) {
      EVENING_TWILIGHT
    } else {
      NIGHT
    }
  }

}
