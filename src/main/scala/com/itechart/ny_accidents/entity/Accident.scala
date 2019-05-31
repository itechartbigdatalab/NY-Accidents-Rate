package com.itechart.ny_accidents.entity

import java.time.LocalDateTime

case class Accident(uniqueKey: Option[Long],
                    localDateTime: Option[LocalDateTime],
                    dateTimeMillis: Option[Long],
                    borough: Option[String],
                    latitude: Option[Double],
                    longitude: Option[Double],
                    onStreet: Option[String],
                    crossStreet: Option[String],
                    offStreet: Option[String],
                    personsInjured: Option[Int],
                    personsKilled: Option[Int],
                    pedestriansInjured: Option[Int],
                    pedestriansKilled: Option[Int],
                    cyclistInjured: Option[Int],
                    cyclistKilled: Option[Int],
                    motoristInjured: Option[Int],
                    motoristKilled: Option[Int],
                    contributingFactors: List[Option[String]],
                    vehicleType: List[Option[String]])

case class AccidentWithoutOption(uniqueKey: Long,
                    localDateTime: LocalDateTime,
                    dateTimeMillis: Long,
                    borough: String,
                    latitude: Double,
                    longitude: Double,
                    onStreet: String,
                    crossStreet: String,
                    offStreet: String,
                    personsInjured: Int,
                    personsKilled: Int,
                    pedestriansInjured: Int,
                    pedestriansKilled: Int,
                    cyclistInjured: Int,
                    cyclistKilled: Int,
                    motoristInjured: Int,
                    motoristKilled: Int,
                    contributingFactors: List[String],
                    vehicleType: List[String])

case class ReportAccident(dateTime: Option[LocalDateTime],
                          dateTimeMillis: Option[Long],
                          latitude: Option[Double],
                          longitude: Option[Double])