package com.itechart.ny_accidents.constants

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object ReportsDatabaseSchemas {
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

  lazy val POPULATION_TO_ACCIDENTS_REPORT_SCHEMA: StructType = new StructType()
    .add("district", StringType)
    .add("ratio", DoubleType)
    .add("density", DoubleType)
    .add("accidents_count", IntegerType)

  lazy val ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT: StructType = new StructType()
    .add("phenomenon", StringType)
    .add("accidents_count", IntegerType)
    .add("total_phenomenon_hours", DoubleType)
    .add("accidents_per_hour", DoubleType)

}
