package com.itechart.accidents.service.metric

import com.google.inject.Singleton
import com.itechart.accidents.constants.Configuration
import com.itechart.accidents.entity.MergedData
import org.apache.spark.rdd.RDD

@Singleton
class YearDifferenceMetricService {

  def calculateDifferenceBetweenAccidentCount(firstYear: RDD[MergedData], secondYear: RDD[MergedData]): RDD[(String, Int)] = {
    val firstYearValue: Int = Configuration.FIRST_YEAR_VALUE
    val secondYearValue: Int = Configuration.SECOND_YEAR_VALUE

    val firstYearAccidentCount = firstYear
      .filter(_.district.isDefined)
      .map(_.district.get)
      .groupBy(_.districtName)
      .map(tuple => (tuple._1, (tuple._2.size, firstYearValue)))

    val secondYearAccidentCount = secondYear
      .filter(_.district.isDefined)
      .map(_.district.get)
      .groupBy(_.districtName)
      .map(tuple => (tuple._1, (tuple._2.size, secondYearValue)))

    val combinedData = firstYearAccidentCount.union(secondYearAccidentCount)
    combinedData.reduceByKey((first, second) => {
      if (first._2 < second._2) {
        (second._1 - first._1, 0)
      } else {
        (first._1 - second._1, 0)
      }
    }).map { case (district, difference) => (district, difference._1) }
  }

}
