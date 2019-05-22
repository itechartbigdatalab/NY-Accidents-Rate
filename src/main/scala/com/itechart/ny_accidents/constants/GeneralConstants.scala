package com.itechart.ny_accidents.constants

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.Locale

object GeneralConstants {
  val KIBANA_REPORT_TIME_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  val DATE_TIME_FORMATTER_WEATHER: DateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm")
  val DATE_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")
  val TIME_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("H:mm")
  val DATE_TIME_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy H:mm")

  val DATE_TIME_WEATHER_PATTERN:  SimpleDateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm")
  val DATE_TIME_ACCIDENTS_PATTERN: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy H:mm")
  val DATE_SUNRISES_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val DATE_TIME_SUNRISES_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd h:mm:ss a", Locale.ENGLISH)

  val SUNRISE_YEAR = 2019

  val TWILIGHT_START_C = 0
  val SUNRISE_C = 1
  val SUNSET_C = 2
  val TWILIGHT_END_C = 3

  val NIGHT = "night"
  val MORNING_TWILIGHT = "morning_twilight"
  val EVENING_TWILIGHT = "evening_twilight"
  val DAY = "day"

  val MILLIS_IN_HOUR: Long = 3600000

  val FIRST_MONTH_URL = 1
  val LAST_MONTH_URL = 12
  val SUNRISES_SITE_URL_WITHOUT_MONTH_NUMBER = "https://sunrise-sunset.org/us/new-york-ny/2019/"

  val HTML_ELEMENT_DAY_SELECTOR = ".day"
  val HTML_ELEMENT_DATE_ATTRIBUTE = "rel"
  val HTML_ELEMENT_TD = "td"
}
