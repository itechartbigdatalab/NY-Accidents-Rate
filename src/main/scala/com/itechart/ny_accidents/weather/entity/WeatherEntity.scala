package com.itechart.ny_accidents.weather.entity


case class WeatherEntity(
                          id: Long,
                          localDateTime: Long,
                          temperature: Int,
                          pressure: Double,
                          humidity: Int,
                          windSpeed: Double,
                          phenomenon: String,
                          visibility: Double
                        )
