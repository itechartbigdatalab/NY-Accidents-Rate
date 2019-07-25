package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.TestSparkApi
import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.entity.{Accident, DetailedDistrictData, District, MergedData}
import com.itechart.ny_accidents.utils.DateUtils
import org.apache.spark.rdd.RDD
import org.scalatest.FunSpec

class DistrictMetricServiceTest extends FunSpec {
  lazy val firstAccident = Accident(None, None, None, None, Some(40.869335),
    Some(-73.8255), None, None, None, 0, 0,
    0, 0, 1, 0, 1, 0, List(), List())
  lazy val secondAccident = Accident(None, None, None, None,Some(40.869335),
    Some(-73.8255), None, None, None, 0, 0,
    1, 0, 1, 2, 4, 0, List(), List())

  describe("A DistrictMetricService") {
    it("should return RDD of detailed district data") {
      val firstDate = DateUtils.parseDate("05/15/2016 14:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val secondDate = DateUtils.parseDate("05/15/2016 15:30", GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER)
      val testData: RDD[MergedData] = TestSparkApi.spark.parallelize(Seq(
        MergedData(firstAccident.copy(localDateTime = firstDate), Some(District("a", "A")), None),
        MergedData(secondAccident.copy(localDateTime = secondDate), Some(District("a", "A")), None)
      ))

      val expectedValue = DetailedDistrictData("a", 1, 0, 2,2,5,0,18,1,12,5 )
      val result = DistrictMetricService.getDetailedDistrictData(testData).take(1).head
      assert(expectedValue == result)
    }
  }
}
