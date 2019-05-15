package com.itechart.ny_accidents.utils

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.util.{Failure, Success, Try}

object DateUtils {

  def parseDate(dateStr: String, formatter: DateTimeFormatter): Option[Long] = {
    Try(LocalDateTime.parse(dateStr, formatter).toInstant(ZoneOffset.UTC).toEpochMilli) match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

}

