package com.itechart.accidents.entity

import java.time.LocalDateTime
import java.sql.{Date => SqlDate}

case class Accident(uniqueKey: Option[Long],
                    localDateTime: Option[LocalDateTime],
                    dateTimeMillis: Option[Long],
                    borough: Option[String],
                    latitude: Option[Double],
                    longitude: Option[Double],
                    onStreet: Option[String],
                    crossStreet: Option[String],
                    offStreet: Option[String],
                    personsInjured:  Option[Int],
                    personsKilled:  Option[Int],
                    pedestriansInjured:  Option[Int],
                    pedestriansKilled:  Option[Int],
                    cyclistInjured:  Option[Int],
                    cyclistKilled:  Option[Int],
                    motoristInjured:  Option[Int],
                    motoristKilled:  Option[Int],
                    contributingFactors: List[Option[String]],
                    vehicleType: List[Option[String]])

case class AccidentSparkFormat(uniqueKey: Option[Long],
                               date: Option[SqlDate],
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


case class ReportAccident(dateTime: Option[LocalDateTime],
                          dateTimeMillis: Option[Long],
                          latitude: Option[Double],
                          longitude: Option[Double])