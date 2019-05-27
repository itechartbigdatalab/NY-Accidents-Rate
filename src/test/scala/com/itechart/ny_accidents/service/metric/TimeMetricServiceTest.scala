package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.TestSparkApi
import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.entity.{Accident, District, MergedData}
import com.itechart.ny_accidents.utils.DateUtils
import org.apache.spark.rdd.RDD
import org.scalatest.FunSpec

class TimeMetricServiceTest extends FunSpec {
  lazy val emptyAccident = Accident(None, None, None, None, None,
    None, None, None, None, None, None,
    None, None, None, None, None, None, List(), List())

  describe("A TimeMetricService") {
    it("should return RDD of tuple with number of accidents for each hour") {
      val firstDate = DateUtils.parseDate("05/15/2016 14:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
      val secondDate = DateUtils.parseDate("05/15/2016 15:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
      val thirdDate = DateUtils.parseDate("05/15/2016 16:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
      val fourthDate = DateUtils.parseDate("05/15/2016 17:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)

      val testData: RDD[MergedData] = TestSparkApi.spark.parallelize(Seq(
        MergedData(emptyAccident.copy(dateTime = firstDate), Some(District("a", "A", null)), None),
        MergedData(emptyAccident.copy(dateTime = secondDate), Some(District("b", "B", null)), None),
        MergedData(emptyAccident.copy(dateTime = thirdDate), Some(District("c", "C", null)), None),
        MergedData(emptyAccident.copy(dateTime = fourthDate), Some(District("d", "D", null)), None)
      ))

      val expectedValue = Seq(
        (15, 1, 25.0),
        (16, 1, 25.0),
        (14, 1, 25.0),
        (17, 1, 25.0)
      )
      val result = TimeMetricService.countHours(testData).collect().toSeq

      assert(expectedValue == result)
    }

    it("should count number and percentage accidents per week day") {
      val firstDate = DateUtils.parseDate("05/15/2016 14:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
      val secondDate = DateUtils.parseDate("06/15/2016 15:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
      val thirdDate = DateUtils.parseDate("07/15/2016 16:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
      val fourthDate = DateUtils.parseDate("08/15/2016 17:30", GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)

      val testData: RDD[MergedData] = TestSparkApi.spark.parallelize(Seq(
        MergedData(emptyAccident.copy(dateTime = firstDate), Some(District("a", "A", null)), None),
        MergedData(emptyAccident.copy(dateTime = secondDate), Some(District("b", "B", null)), None),
        MergedData(emptyAccident.copy(dateTime = thirdDate), Some(District("c", "C", null)), None),
        MergedData(emptyAccident.copy(dateTime = fourthDate), Some(District("d", "D", null)), None)
      ))

      val expectedValue = Seq(
        ("Thu", 1, 25.0),
        ("Mon", 1, 25.0),
        ("Sat", 1, 25.0),
        ("Tue", 1, 25.0)
      )
      val result = TimeMetricService.countDayOfWeek(testData).collect().toSeq

      assert(expectedValue == result)
    }
  }
}