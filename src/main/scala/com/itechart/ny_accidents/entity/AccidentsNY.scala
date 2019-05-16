package com.itechart.ny_accidents.entity
import java.time.{LocalDate, LocalTime}

object AccidentsNY {
case class RawAccidentsNY(date: LocalDate, time: LocalTime, latitude: Option[Double], longitude: Option[Double],
                          onStreet: String, crossStreet:String, offStreet:String, personsInjured: Option[Int], personsKilled: Option[Int],
                          pedastriansInjured: Option[Int], pedastriansKilled: Option[Int],  cyclistInjured: Option[Int], cyclistKilled: Option[Int], motoristInjured: Option[Int],
                          motoristKilled: Option[Int], contributingFactors: List[String], vehicleType:List[String] )
}
