package com.itechart.ny_accidents.utils

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}
import java.util.Date

import org.apache.commons.lang.time.DateFormatUtils
import org.joda.time.DateTime

import scala.util.Try

object DateUtils {
  final val MILLIS_IN_HOUR = 3600000

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

  def hashByDate(localDateTimeMillis: Long): Long = {
    fromLongToLocalDateTime(localDateTimeMillis)
      .withMinute(0).toInstant(ZoneOffset.UTC).toEpochMilli
  }

  def getStringFromDate(date: Date, format: String): String = {
    DateFormatUtils.format(date, format)
  }
}
