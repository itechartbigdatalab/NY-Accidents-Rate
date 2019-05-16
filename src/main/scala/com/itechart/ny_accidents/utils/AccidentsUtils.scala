package com.itechart.ny_accidents.utils

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime}

import com.itechart.ny_accidents.entity.AccidentsHeader._
import com.itechart.ny_accidents.entity.AccidentsNY.RawAccidentsNY

object AccidentsUtils {

  // TODO replace try {} by Try-match
  def toDouble(s: String):Option[Double] = {
    try {
      Some(s.toDouble)
    } catch {
      case e: NumberFormatException => None
    }
  }

  // TODO replace try {} by Try-match
  def toInt(s: String):Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def accidentsMapper(value: Map[String, String]): RawAccidentsNY={


    val contributingFactorsFromFile =contributingFactors.toList.map(v=>value(v))
    val vehicleTypeFromFile =vehicleType.toList.map(v=>value(v).toString)

    RawAccidentsNY(
      LocalDate.parse(value(date), DateTimeFormatter.ofPattern("MM/d/yyyy")), LocalTime.parse( value(time), DateTimeFormatter.ofPattern("H:mm")),
      toDouble(value(latitude)) ,toDouble(value(longitude)),
      value(onStreet).toString, value(crossStreet).toString, value(offStreet).toString, toInt(value(personsInjured)), toInt(value(personsKilled)),
    toInt(value(pedastriansInjured)), toInt(value(pedastriansKilled)),  toInt(value(cyclistInjured)), toInt(value(cyclistKilled)), toInt(value(motoristInjured)),
      toInt(value(motoristKilled)),
      contributingFactorsFromFile, vehicleTypeFromFile)

  }
}
