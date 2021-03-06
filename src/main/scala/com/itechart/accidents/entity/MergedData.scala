package com.itechart.accidents.entity

case class MergedDataDataSets(accident: AccidentSparkFormat,
                              district: District,
                              weather: WeatherForAccident)

case class MergedData(accident: Accident,
                      district: Option[District],
                      weather: Option[WeatherForAccident])

case class ReportMergedData(accident: ReportAccident, district: Option[District], weather: Option[WeatherForAccident])