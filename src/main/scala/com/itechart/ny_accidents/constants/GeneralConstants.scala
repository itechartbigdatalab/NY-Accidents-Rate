package com.itechart.ny_accidents.constants

import java.time.format.DateTimeFormatter
import java.util.Locale

object GeneralConstants {
  val KIBANA_REPORT_TIME_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  val DATE_TIME_FORMATTER_WEATHER: DateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm")
  val DATE_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")
  val TIME_FORMATTER_ACCIDENTS: DateTimeFormatter = DateTimeFormatter.ofPattern("H:mm")
  val DATE_TIME_ACCIDENTS_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy H:mm")
  val DATE_SUNRISES_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val DATE_TIME_SUNRISES_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd h:mm:ss a", Locale.ENGLISH)

//  val DATE_TIME_WEATHER_PATTERN:  SimpleDateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm")
//  val DATE_TIME_ACCIDENTS_PATTERN: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy H:mm")
//  val DATE_SUNRISES_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//  val DATE_TIME_SUNRISES_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd h:mm:ss a", Locale.ENGLISH)

  val SUNRISE_YEAR = 2019

  val TWILIGHT_START_C = 0
  val SUNRISE_C = 1
  val SUNSET_C = 2
  val TWILIGHT_END_C = 3

  val NIGHT = "night"
  val MORNING_TWILIGHT = "morning_twilight"
  val EVENING_TWILIGHT = "evening_twilight"
  val DAY = "day"

  val MILLIS_IN_HOUR: Long = 3600000

  val FIRST_MONTH_URL = 1
  val LAST_MONTH_URL = 12
  val SUNRISES_SITE_URL_WITHOUT_MONTH_NUMBER = "https://sunrise-sunset.org/us/new-york-ny/2019/"

  val HTML_ELEMENT_DAY_SELECTOR = ".day"
  val HTML_ELEMENT_DATE_ATTRIBUTE = "rel"
  val HTML_ELEMENT_TD = "td"

  val HASH_DIFFERENCE: Long = MILLIS_IN_HOUR
  val FIRST_STATION_ID = 1 // In NY we always have only 3 weather stations with hand write id's 1, 2, 3
  val LAST_STATION_ID = 3

  val DAY_OF_WEEK_REPORT_HEADER: Seq[String] = Seq("Day_of_week","Accident_count","Percentage")
  val HOUR_OF_DAY_REPORT_HEADER: Seq[String] = Seq("Hour_of_day","Accident_count","Percentage")
  val DAY_PERIOD_REPORT_HEADER: Seq[String] = Seq("Day_period","Accident_count","Percentage")
  val PHENOMENON_REPORT_HEADER: Seq[String] = Seq("Phenomenon","Accident_count","Percentage")
  val BOROUGH_REPORT_HEADER: Seq[String] = Seq("Borough","Accident_count","Percentage")
  val DISTRICT_REPORT_HEADER: Seq[String] = Seq("District","Accident_count","Percentage")
  val POPULATION_TO_ACCIDENTS_REPORT_HEADER: Seq[String] = Seq("District","Ratio","Density", "Accident_Count")
  val ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_HEADER: Seq[String] = Seq("Phenomenon","Accident_count","Total_phenomenon_hours", "Accident_per_hour")

  val DAY_OF_WEEK_REPORT_DATABASE_HEADER: Seq[String] = Seq("day_of_week", "accidents_count", "percentage")
  val HOUR_OF_DAY_REPORT_DATABASE_HEADER: Seq[String] = Seq("hour_of_day", "accidents_count", "percentage")
  val DAY_PERIOD_REPORT_DATABASE_HEADER: Seq[String] = Seq("day_period", "accidents_count", "percentage")
  val PHENOMENON_REPORT_DATABASE_HEADER: Seq[String] = Seq("phenomenon", "accidents_count", "percentage")
  val BOROUGH_REPORT_DATABASE_HEADER: Seq[String] = Seq("borough", "accidents_count", "percentage")
  val DISTRICT_REPORT_DATABASE_HEADER: Seq[String] = Seq("district", "accidents_count", "percentage")
  val POPULATION_TO_ACCIDENTS_REPORT_DATABASE_HEADER: Seq[String] = Seq("district", "ratio", "density", "accidents_count")
  val ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_DATABASE_HEADER: Seq[String] = Seq("phenomenon", "accidents_count", "total_phenomenon_hours", "accidents_per_hour")
}
