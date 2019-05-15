package com.itechart.ny_accidents.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.itechart.ny_accidents.entity.AccidentsNY.RawAccidentsNY
import com.itechart.ny_accidents.entity.AccidentsHeader._


import scala.collection.immutable.HashMap
import scala.util.{Failure, Success, Try}

object AccidentsUtils {
  def accidentsMapper(value: Map[String, String])={

    val formatter = DateTimeFormatter.ofPattern("d/MM/yyyy")

    val date_from_file = LocalDate.parse(value(date).toString, formatter)
//      value(date).toString match {
//      case Some(tmp) => Some(LocalDate.parse(tmp, formatter))
//      case "" =='' => None
//    }

    print(date_from_file)
//    RawAccidentsNY(
//      value(date), latitude,longitude,
//        onStreet:, crossStreet, offStreet, personsInjured, personsKilled,
//        pedastriansInjured, pedastriansKilled,  cyclistInjured, cyclistKilled, motoristInjured, motoristLikked,
//        contributingFactors, vehicleType )
//    )

  }
}
