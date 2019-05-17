package com.itechart.ny_accidents

import java.time.format.DateTimeFormatter

object GeneralConstants {
  val DATE_TIME_FORMATTER_WEATHER: DateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm")
  val DATE_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")
  val TIME_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("H:mm")
  val DATE_TIME_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy H:mm")

  val DATE_TIME_WEATHER_PATTERN: String = "dd.MM.yyyy HH:mm"
  val DATE_TIME_ACCIDENTS_PATTERN: String = "MM/dd/yyyy H:mm"

  val MILLIS_IN_HOUR: Long = 3600000
}
