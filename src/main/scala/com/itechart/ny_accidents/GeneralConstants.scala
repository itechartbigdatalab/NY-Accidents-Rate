package com.itechart.ny_accidents

import java.time.format.DateTimeFormatter

object GeneralConstants {

  val DATE_TIME_FORMATTER_WEATHER: DateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm")
  val DATE_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/d/yyyy")
  val TIME_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("H:mm")


}
