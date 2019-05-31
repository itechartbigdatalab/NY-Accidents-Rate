package com.itechart.ny_accidents.utils

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.itechart.ny_accidents.constants.GeneralConstants.{MILLIS_IN_HOUR, MILLIS_IN_MINUTE, HASH_DIFFERENCE}
import com.itechart.ny_accidents.entity.WeatherForAccident
import org.apache.spark.sql.Row

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
    val dateTimeWithoutHours = LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTimeMillis), ZoneOffset.UTC)
      .truncatedTo(ChronoUnit.HOURS)
      .toInstant(ZoneOffset.UTC)
      .toEpochMilli
    val diff = dateTimeMillis - dateTimeWithoutHours
    if (diff < MILLIS_IN_MINUTE * 7.5) dateTimeWithoutHours
    else if (diff < MILLIS_IN_MINUTE * 22.5) dateTimeWithoutHours + HASH_DIFFERENCE
    else if (diff < MILLIS_IN_MINUTE * 37.5) dateTimeWithoutHours + HASH_DIFFERENCE * 2
    else if (diff < MILLIS_IN_MINUTE * 52.5) dateTimeWithoutHours + HASH_DIFFERENCE * 3
    else dateTimeWithoutHours + HASH_DIFFERENCE * 4
  }

  def getStringFromDate(date: LocalDateTime, format: DateTimeFormatter): String = {
    date.format(format)
  }

  def weatherForAccidentMapper(row: Row): WeatherForAccident = {
    WeatherForAccident(
      row.getDouble(3),
      row.getDouble(4),
      row.getDouble(5),
      row.getString(6),
      row.getDouble(7),
      row.getDouble(8)
    )
  }
}
