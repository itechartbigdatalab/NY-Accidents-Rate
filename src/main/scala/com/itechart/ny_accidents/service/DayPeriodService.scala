package com.itechart.ny_accidents.service

import java.util.{Calendar, Date}

import com.itechart.ny_accidents.constants.GeneralConstants._
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.scraper.ContentExtractors.{attr, elementList, texts}

class DayPeriodService {

  private val sunrisesSunsets: Map[Date, Seq[Date]] = parseSunrisesSunsets

  private def parseSunrisesSunsets: Map[Date, Seq[Date]] = {
    val browser = JsoupBrowser()
    (1 to 12)
      .map("https://sunrise-sunset.org/us/new-york-ny/2019/" + _)
      .map(browser.get)
      .map(_ >> elementList(".day"))
      .map(_.map(day => (day >> attr("rel")) +: (day >> texts("td")).toSeq.take(4)))
      .reduce(_ ++ _)
      .map(dates => {
        val dayStr = dates.head
        (DATE_SUNRISES_FORMAT.parse(dayStr),
          dates.drop(1)
            .map(dayStr + " " + _)
            .map(DATE_TIME_SUNRISES_FORMAT.parse))
      }).toMap
  }

  def defineLighting(dateTime: Date): String = {
    val calendar = Calendar.getInstance()
    calendar.setTime(dateTime)
    calendar.set(Calendar.YEAR, SUNRISE_YEAR)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    val lightParameters = sunrisesSunsets(calendar.getTime)
    if (dateTime.before(lightParameters(TWILIGHT_START_C))) {
      NIGHT
    } else if (dateTime.before(lightParameters(SUNRISE_C))) {
      MORNING_TWILIGHT
    } else if (dateTime.before(lightParameters(SUNSET_C))) {
      DAY
    } else if (dateTime.before(lightParameters(TWILIGHT_END_C))) {
      EVENING_TWILIGHT
    } else {
      NIGHT
    }
  }

}
