package com.itechart.ny_accidents.constants

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter

object GeneralConstants {
  val DATE_TIME_FORMATTER_WEATHER: DateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm")
  val DATE_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")
  val TIME_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("H:mm")
  val DATE_TIME_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy H:mm")

  val DATE_TIME_WEATHER_PATTERN: String = "dd.MM.yyyy HH:mm"
  val DATE_TIME_ACCIDENTS_PATTERN: String = "MM/dd/yyyy H:mm"
  val DATE_SUNRISES_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val DATE_TIME_SUNRISES_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd H:mm:ss a")

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
}
