package com.itechart.ny_accidents.entity

import com.itechart.ny_accidents.entity.AccidentsNY.RawAccidentsNY

case class MergedData (accident: RawAccidentsNY, district: Option[District], weather: Option[WeatherForAccident])
