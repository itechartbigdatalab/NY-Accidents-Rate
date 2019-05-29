package com.itechart.ny_accidents.utils

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.itechart.ny_accidents.constants.GeneralConstants.MILLIS_IN_HOUR

import scala.util.Try

object DateUtils {

  def subtractHour(dateTimeMillis: Long): Long = {
    dateTimeMillis - MILLIS_IN_HOUR
  }

  def addHour(dateTimeMillis: Long): Long = {
    dateTimeMillis + MILLIS_IN_HOUR
  }

  def parseDateToMillis(dateStr: String, dateFormatter: DateTimeFormatter): Option[Long] = {
    Try(LocalDateTime.parse(dateStr, dateFormatter).toInstant(ZoneOffset.UTC).toEpochMilli).toOption
  }

  def parseDate(dateStr: String, dateFormatter: DateTimeFormatter): Option[LocalDateTime] = {
    Try(LocalDateTime.parse(dateStr, dateFormatter)).toOption
  }

  def hashByDate(dateTimeMillis: Long): Long = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTimeMillis), ZoneOffset.UTC)
      .truncatedTo(ChronoUnit.HOURS)
      .toInstant(ZoneOffset.UTC)
      .toEpochMilli
  }

  def getStringFromDate(date: LocalDateTime, format: DateTimeFormatter): String = {
    date.format(format)
  }
}
