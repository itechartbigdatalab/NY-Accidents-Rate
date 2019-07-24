package com.itechart.ny_accidents.utils

import java.time.LocalDateTime

import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.constants.GeneralConstants.ZERO_DATE
import org.apache.spark.sql.Row
import java.sql.{Date => SqlDate}

import scala.util.Try

object ParserUtils {

  def toLongFromInt(accident: Row, column: Int): Option[Long] = {
    Try(accident.getInt(column).toLong).filter(value => value != 0).toOption
  }

  def toLocalDate(dateStr: String, timeStr: String): LocalDateTime = {
    val dateTimeStr = dateStr + " " + timeStr
    DateUtils.parseDate(dateTimeStr, GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER).getOrElse(ZERO_DATE)
  }

  def toMillis(row: Row, dateColumn: Int, timeColumn: Int): Option[Long] = {
    val dateTime = row.getString(dateColumn) + " " + row.getString(timeColumn)
    val value = DateUtils.parseDateToMillis(dateTime, GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER).getOrElse(0L)
    if (value == 0) None
    else Some(value)
  }

  def toDouble(accident: Row, column: Int): Option[Double] = {
    val value: Double = Try(accident.getDouble(column)).toOption.getOrElse(0)
    if (value == 0) None
    else Some(value)
  }

  def toString(accident: Row, column: Int): Option[String] = {
    Option(accident.getString(column))
  }

  def toInt(accident: Row, column: Int): Int = {
    val value = Try(accident.getInt(column)).getOrElse(0)
    value
  }

  def toStringList(row: Row, columns: Array[Int]): List[Option[String]] = {
    columns
      .map(toString(row, _))
      .toList
  }

  def toDate(row: Row, column: Int): Option[SqlDate] = {
    toString(row, column) match {
      case Some(value) => DateUtils.parseSqlDate(value, GeneralConstants.DATE_FORMATTER_ACCIDENTS)
      case None => None
    }
  }
}
