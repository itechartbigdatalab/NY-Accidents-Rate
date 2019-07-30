package com.itechart.accidents.utils

import java.time.LocalDateTime

import com.itechart.accidents.constants.GeneralConstants
import com.itechart.accidents.constants.GeneralConstants.ZERO_DATE
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
    DateUtils.parseDateToMillis(dateTime, GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER).filter(value => value != 0)
  }

  def toDouble(accident: Row, column: Int): Option[Double] = {
    Try(accident.getDouble(column)).filter(value => value != 0).toOption

  }

  def toString(accident: Row, column: Int): Option[String] = {
    Option(accident.getString(column))
  }

  def toInt(accident: Row, column: Int): Option[Int]= {
    Try(accident.getInt(column)).toOption
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
