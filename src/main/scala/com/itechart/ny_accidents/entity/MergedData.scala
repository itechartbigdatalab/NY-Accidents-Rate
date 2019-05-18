package com.itechart.ny_accidents.entity

case class MergedData(accident: Accident,
                      district: Option[District],
                      weather: Option[WeatherForAccident])
