package com.itechart.ny_accidents.entity

import java.util.Date

case class Accident(dateTime: Option[Date],
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
