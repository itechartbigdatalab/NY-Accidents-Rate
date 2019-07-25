package com.itechart.ny_accidents.constants


import org.apache.spark.sql.types._


object ReportsDatabase {
  lazy val DAY_OF_WEEK_REPORT_SCHEMA: StructType = new StructType()
    .add("day_of_week", StringType)
    .add("accidents_count", IntegerType)
    .add("percentage", DoubleType)

  lazy val HOUR_OF_DAY_REPORT_SCHEMA: StructType = new StructType()
    .add("hour_of_day", IntegerType)
    .add("accidents_count", IntegerType)
    .add("percentage", DoubleType)

  lazy val DAY_PERIOD_REPORT_SCHEMA: StructType = new StructType()
    .add("day_period", StringType)
    .add("accidents_count", IntegerType)
    .add("percentage", DoubleType)

  lazy val PHENOMENON_REPORT_SCHEMA: StructType = new StructType()
    .add("phenomenon", StringType)
    .add("accidents_count", IntegerType)
    .add("percentage", DoubleType)

  lazy val BOROUGH_REPORT_SCHEMA: StructType = new StructType()
    .add("borough", StringType)
    .add("accidents_count", IntegerType)
    .add("percentage", DoubleType)

  lazy val DISTRICT_REPORT_SCHEMA: StructType = new StructType()
    .add("district", StringType)
    .add("accidents_count", IntegerType)
    .add("percentage", DoubleType)

  lazy val DETAILED_DISTRICT_REPORT_SCHEMA: StructType = new StructType()
    .add("district", StringType)
    .add("number_of_pedestrians_injured", IntegerType)
    .add("number_of_pedestrians_killed", IntegerType)
    .add("number_of_cyclist_injured", IntegerType)
    .add("number_of_cyclist_killed", IntegerType)
    .add("number_of_motorist_injured", IntegerType)
    .add("number_of_motorist_killed", IntegerType)

  lazy val POPULATION_TO_ACCIDENTS_REPORT_SCHEMA: StructType = new StructType()
    .add("district", StringType)
    .add("ratio", DoubleType)
    .add("density", DoubleType)
    .add("accidents_count", IntegerType)

  lazy val ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_SCHEMA: StructType = new StructType()
    .add("phenomenon", StringType)
    .add("accidents_count", IntegerType)
    .add("total_phenomenon_hours", DoubleType)
    .add("accidents_per_hour", DoubleType)
  lazy val FREQUENCY_REPORT_SCHEMA: StructType = new StructType()
    .add("day period", StringType)
    .add("frequency", LongType)


  lazy val DAY_OF_WEEK_REPORT_TABLE_NAME = "day_of_week_report"
  lazy val HOUR_OF_DAY_REPORT_TABLE_NAME = "hour_of_day_report"
  lazy val DAY_PERIOD_REPORT_TABLE_NAME = "day_period_report"
  lazy val PHENOMENON_REPORT_TABLE_NAME = "phenomenon_report"
  lazy val BOROUGH_REPORT_TABLE_NAME = "borough_report"
  lazy val DISTRICT_REPORT_TABLE_NAME = "district_report"
  lazy val POPULATION_TO_ACCIDENTS_REPORT_TABLE_NAME = "population_to_accident"
  lazy val ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_TABLE_NAME = "accidents_during_phenomenon_report"
  lazy val FREQUENCY_REPORT_TABLE_NAME = "frequency_report"
  lazy val DETAILED_DISTRICT_REPORT_TABLE_NAME = "detailed_district_report"

}
