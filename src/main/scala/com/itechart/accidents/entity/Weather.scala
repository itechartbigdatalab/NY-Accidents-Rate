package com.itechart.accidents.entity

import com.vividsolutions.jts.geom.Point

case class WeatherEntity(id: Long,
                         stationId: Int,
                         dateTime: Long,
                         temperature: Double,
                         pressure: Double,
                         humidity: Double,
                         windSpeed: Double,
                         phenomenon: String,
                         visibility: Double)

case class WeatherStation(id: Int, name: String, geom: Point)

case class WeatherForAccident(temperature: Double,
                              pressure: Double,
                              humidity: Double,
                              phenomenon: String,
                              windSpeed: Double,
                              visibility: Double)
