package com.itechart.ny_accidents.entity

import com.vividsolutions.jts.geom.Point


case class WeatherEntity(id: Long,
                         stationId: Int,
                         localDateTime: Long,
                         temperature: Int,
                         pressure: Double,
                         humidity: Int,
                         windSpeed: Double,
                         phenomenon: String,
                         visibility: Double)

case class WeatherStation(id: Int, name: String, geom: Point)
