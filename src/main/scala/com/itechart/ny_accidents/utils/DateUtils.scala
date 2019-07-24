package com.itechart.ny_accidents.utils

import java.sql.{Date => SqlDate}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

import com.itechart.ny_accidents.constants.GeneralConstants.{HASH_DIFFERENCE, MILLIS_IN_HOUR, MILLIS_IN_MINUTE}
import com.itechart.ny_accidents.entity.WeatherForAccident
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object DateUtils {

  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val hashMultiplicity = 15

  def getLocalDateFromMillis(dateTimeMillis: Long): LocalDateTime = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTimeMillis), ZoneOffset.UTC)
  }

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

  def parseSqlDate(dateString: String, dateFormatter: DateTimeFormatter): Option[SqlDate] = {
    Try(new SqlDate(LocalDate.parse(dateString, dateFormatter)
      .atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli)) match {
      case Success(value) => Some(value)
      case Failure(exception) =>
        logger.error(exception.toString)
        None
    }
  }

  def hashByDate(dateTimeMillis: Long): Long = {
    val dateTimeWithoutHours = LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTimeMillis), ZoneOffset.UTC)
      .truncatedTo(ChronoUnit.HOURS)
      .toInstant(ZoneOffset.UTC)
      .toEpochMilli
    val diff = dateTimeMillis - dateTimeWithoutHours
    if (diff < MILLIS_IN_MINUTE * hashMultiplicity / 2) dateTimeWithoutHours
    else if (diff < MILLIS_IN_MINUTE * hashMultiplicity / 3) dateTimeWithoutHours + HASH_DIFFERENCE
    else if (diff < MILLIS_IN_MINUTE * hashMultiplicity / 5) dateTimeWithoutHours + HASH_DIFFERENCE * 2
    else if (diff < MILLIS_IN_MINUTE * hashMultiplicity / 7) dateTimeWithoutHours + HASH_DIFFERENCE * 3
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
