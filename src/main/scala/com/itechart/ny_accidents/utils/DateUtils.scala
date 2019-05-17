package com.itechart.ny_accidents.utils

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}

import scala.util.{Failure, Success, Try}

object DateUtils {


  def subtractHour(dateTimeMillis: Long): Long = {
    dateTimeMillis - 3600000
  }

  def addHour(dateTimeMillis: Long): Long = {
    dateTimeMillis + 3600000
  }

  def parseDate(dateStr: String, datePattern: String): Option[Long] = {
    val dateFormat = new SimpleDateFormat(datePattern)
    Try(dateFormat.parse(dateStr).getTime).toOption
  }

  def fromLongToLocalDateTime(localDateTimeMillis: Long): LocalDateTime = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(localDateTimeMillis), ZoneId.systemDefault())
  }

  def hashByDate(localDateTimeMillis: Long): Long = {
    fromLongToLocalDateTime(localDateTimeMillis)
      .withMinute(0).toInstant(ZoneOffset.UTC).toEpochMilli
  }

}
