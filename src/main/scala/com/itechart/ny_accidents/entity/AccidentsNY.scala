package com.itechart.ny_accidents.entity
import java.time.{LocalDate, LocalTime}
import java.util.Date

object AccidentsNY {
case class RawAccidentsNY(date: LocalDate, time: LocalTime, latitude: Option[Double], longitude: Option[Double],
                          onStreet: String, crossStreet:String, offStreet:String, personsInjured: Int, personsKilled: Int,
                          pedastriansInjured: Int, pedastriansKilled: Int,  cyclistInjured: Int, cyclistKilled: Int, motoristInjured: Int, motoristLikked: Int,
                          contributingFactors: List[String], vehicleType:List[String] )
}
