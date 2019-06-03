package com.itechart.ny_accidents.entity

case class MergedDataDataSets(accident: AccidentWithoutOptionAndLocalDate,
                              district: Option[DistrictWithoutGeometry],
                              weather: Option[WeatherForAccident])

case class MergedData(accident: Accident,
                      district: Option[District],
                      weather: Option[WeatherForAccident])

case class ReportMergedData(accident: ReportAccident, district: Option[District], weather: Option[WeatherForAccident])