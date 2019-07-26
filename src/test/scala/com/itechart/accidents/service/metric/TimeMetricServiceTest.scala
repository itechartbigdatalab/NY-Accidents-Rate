package com.itechart.accidents.service.metric

import com.itechart.accidents.TestSparkApi
import com.itechart.accidents.constants.GeneralConstants
import com.itechart.accidents.entity.{Accident, District, MergedData}
import com.itechart.accidents.utils.DateUtils
import org.apache.spark.rdd.RDD
import org.scalatest.FunSpec

class TimeMetricServiceTest extends FunSpec {
  lazy val emptyAccident = Accident(None, None, None, None, None,
    None, None, None, None, 0, 0,
    0, 0, 0, 0, 0, 0, List(), List())

  describe("A TimeMetricService") {
    it("should return RDD of tuple with number of accidents for each hour") {
      val firstDate = DateUtils.parseDate("05/15/2016 14:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val secondDate = DateUtils.parseDate("05/15/2016 15:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val thirdDate = DateUtils.parseDate("05/15/2016 16:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val fourthDate = DateUtils.parseDate("05/15/2016 17:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)

      val testData: RDD[MergedData] = TestSparkApi.spark.parallelize(Seq(
        MergedData(emptyAccident.copy(localDateTime = firstDate), Some(District("a", "A")), None),
        MergedData(emptyAccident.copy(localDateTime = secondDate), Some(District("b", "B")), None),
        MergedData(emptyAccident.copy(localDateTime = thirdDate), Some(District("c", "C")), None),
        MergedData(emptyAccident.copy(localDateTime = fourthDate), Some(District("d", "D")), None)
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
      val firstDate = DateUtils.parseDate("05/15/2016 14:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val secondDate = DateUtils.parseDate("06/15/2016 15:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val thirdDate = DateUtils.parseDate("07/15/2016 16:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val fourthDate = DateUtils.parseDate("08/15/2016 17:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)

      val testData: RDD[MergedData] = TestSparkApi.spark.parallelize(Seq(
        MergedData(emptyAccident.copy(localDateTime = firstDate), Some(District("a", "A")), None),
        MergedData(emptyAccident.copy(localDateTime = secondDate), Some(District("b", "B")), None),
        MergedData(emptyAccident.copy(localDateTime = thirdDate), Some(District("c", "C")), None),
        MergedData(emptyAccident.copy(localDateTime = fourthDate), Some(District("d", "D")), None)
      ))

      val expectedValue = Seq(
        ("1) Mon", 1, 25.0),
        ("3) Wed", 1, 25.0),
        ("5) Fri", 1, 25.0),
        ("7) Sun", 1, 25.0)
      )
      val result = TimeMetricService.countDayOfWeek(testData).collect().toSeq

      assert(expectedValue == result)
    }
  }
}