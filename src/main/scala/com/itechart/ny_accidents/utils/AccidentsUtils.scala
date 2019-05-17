package com.itechart.ny_accidents.utils

import java.time.{LocalDate, LocalTime}

import com.itechart.ny_accidents.GeneralConstants
import com.itechart.ny_accidents.GeneralConstants.{DATE_FORMATTER_ACCIDENTS, TIME_FORMATTER_ACCIDENTS}
import com.itechart.ny_accidents.entity.AccidentsHeader._
import com.itechart.ny_accidents.entity.AccidentsNY.RawAccidentsNY
import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}

object AccidentsUtils {

  //  // TODO replace try {} by Try-match
  //  def toDouble(s: String): Option[Double] = {
  //    try {
  //      Some(s.toDouble)
  //    } catch {
  //      case e: NumberFormatException => None
  //    }
  //  }
  //
  //  // TODO replace try {} by Try-match
  //  def toInt(s: String): Option[Int] = {
  //    try {
  //      Some(s.toInt)
  //    } catch {
  //      case e: NumberFormatException => None
  //    }
  //  }

  //  def accidentsMapper(value: Map[String, String]): RawAccidentsNY={
  //
  //
  //    val contributingFactorsFromFile =contributingFactors.toList.map(v=>value(v))
  //    val vehicleTypeFromFile =vehicleType.toList.map(v=>value(v).toString)
  //
  //    RawAccidentsNY(
  //      LocalDate.parse(value(date), DateTimeFormatter.ofPattern("MM/d/yyyy")), LocalTime.parse( value(time), DateTimeFormatter.ofPattern("H:mm")),
  //      toDouble(value(latitude)) ,toDouble(value(longitude)),
  //      value(onStreet).toString, value(crossStreet).toString, value(offStreet).toString, toInt(value(personsInjured)), toInt(value(personsKilled)),
  //    toInt(value(pedastriansInjured)), toInt(value(pedastriansKilled)),  toInt(value(cyclistInjured)), toInt(value(cyclistKilled)), toInt(value(motoristInjured)),
  //      toInt(value(motoristKilled)),
  //      contributingFactorsFromFile, vehicleTypeFromFile)
  //
  //  }

  def accidentsMapper(accident: Row): RawAccidentsNY = {

    RawAccidentsNY(
      LocalDate.parse(accident.getString(DATE_C), DATE_FORMATTER_ACCIDENTS),
      LocalTime.parse(accident.getString(TIME_C), TIME_FORMATTER_ACCIDENTS),
      toMillis(accident, DATE_C, TIME_C),
      toDouble(accident, LATITUDE_C),
      toDouble(accident, LONGITUDE_C),
      accident.getString(ON_STREET_NAME_C),
      accident.getString(CROSS_STREET_NAME_C),
      accident.getString(OFF_STREET_NAME_C),
      toInt(accident, PERSONS_INJURED_C),
      toInt(accident, PERSONS_KILLED_C),
      toInt(accident, PEDESTRIANS_INJURED),
      toInt(accident, PERSONS_KILLED_C),
      toInt(accident, CYCLIST_INJURED),
      toInt(accident, CYCLIST_KILLED),
      toInt(accident, MOTORIST_INJURED),
      toInt(accident, MOTORIST_KILLED),
      toStringList(accident, CONTRIBUTING_FACTOR_VEHICLE_COLUMNS),
      toStringList(accident, VEHICLE_TYPE_CODE_COLUMNS))

  }

  private def toMillis(row: Row, dateColumn: Int, timeColumn: Int): Option[Long] = {
    val dateTime = row.getString(dateColumn) + " " + row.getString(timeColumn)
    DateUtils.parseDate(dateTime, GeneralConstants.DATE_TIME_FORMATTER_ACCIDENTS)
  }

  private def toDouble(accident: Row, column: Int): Option[Double] = {
    Try(accident.getString(column).toDouble).toOption
  }

  private def toString(accident: Row, column: Int): Option[String] = {
    Try(accident.getString(column)).toOption
  }

  private def toInt(accident: Row, column: Int): Option[Int] = {
    Try(accident.getString(column).toInt).toOption
  }

  private def toStringList(row: Row, columns: Array[Int]): List[String] = {
    columns
      .map(row.getString)
      .toList
  }

}
