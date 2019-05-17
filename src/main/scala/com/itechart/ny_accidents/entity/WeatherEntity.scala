package com.itechart.ny_accidents.entity

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
