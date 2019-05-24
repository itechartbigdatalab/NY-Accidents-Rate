package com.itechart.ny_accidents.utils

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}
import java.util.{Calendar, Date}

import com.itechart.ny_accidents.constants.GeneralConstants.MILLIS_IN_HOUR

import scala.util.Try

object DateUtils {

  def subtractHour(dateTimeMillis: Long): Long = {
    dateTimeMillis - MILLIS_IN_HOUR
  }

  def addHour(dateTimeMillis: Long): Long = {
    dateTimeMillis + MILLIS_IN_HOUR
  }

  def parseDateToMillis(dateStr: String, dateFormat: SimpleDateFormat): Option[Long] = {
    Try(dateFormat.parse(dateStr).getTime).toOption
  }

  def parseDate(dateStr: String, dateFormat: SimpleDateFormat): Option[Date] = {
    Try(dateFormat.parse(dateStr)).toOption
  }

  def fromLongToLocalDateTime(localDateTimeMillis: Long): LocalDateTime = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(localDateTimeMillis), ZoneId.systemDefault())
  }

  def hashByDate(dateTimeMillis: Long): Long = {
    fromLongToLocalDateTime(dateTimeMillis)
      .withMinute(0).toInstant(ZoneOffset.UTC).toEpochMilli
  }

  def getStringFromDate(date: Date, format: DateTimeFormatter): String = {
    date.toInstant
      .atZone(ZoneId.systemDefault())
      .toLocalDate.format(format)
  }
}
