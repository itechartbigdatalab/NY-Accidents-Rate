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
                    personsInjured: Int,
                    personsKilled: Int,
                    pedestriansInjured: Int,
                    pedestriansKilled: Int,
                    cyclistInjured: Int,
                    cyclistKilled: Int,
                    motoristInjured: Int,
                    motoristKilled: Int,
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
                               personsInjured: Int,
                               personsKilled: Int,
                               pedestriansInjured: Int,
                               pedestriansKilled: Int,
                               cyclistInjured: Int,
                               cyclistKilled: Int,
                               motoristInjured: Int,
                               motoristKilled: Int,
                               contributingFactors: List[Option[String]],
                               vehicleType: List[Option[String]])


case class ReportAccident(dateTime: Option[LocalDateTime],
                          dateTimeMillis: Option[Long],
                          latitude: Option[Double],
                          longitude: Option[Double])