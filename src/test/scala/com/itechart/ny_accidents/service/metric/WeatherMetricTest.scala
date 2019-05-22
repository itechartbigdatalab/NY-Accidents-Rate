package com.itechart.ny_accidents.service.metric

import java.util.{Date, GregorianCalendar}

import com.itechart.ny_accidents.TestSparkApi
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import org.scalatest.FunSpec

class WeatherMetricTest extends FunSpec {
  val emptyMergedDataObject = MergedData(
    Accident(None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      List(),
      List()),
    None,
    None)


  describe("A WeatherMetric") {
    it("should count accidents by each hour") {
      val oneHour = new GregorianCalendar(2013, 1, 28, 1, 0, 0).getTimeInMillis
      val twoHour = new GregorianCalendar(2013, 1, 28, 2, 0, 0).getTimeInMillis

      val data = TestSparkApi.spark.parallelize (Seq(
        emptyMergedDataObject.copy(accident = emptyMergedDataObject.accident.copy(dateTime = Some(new Date(oneHour)))),
        emptyMergedDataObject.copy(accident = emptyMergedDataObject.accident.copy(dateTime = Some(new Date(twoHour)))),
      ))

      val expectedResult = Map[Int, Double](
        1 -> 50.0,
        2 -> 50.0
      )
      val result = WeatherMetric.countHours(data).collect().toMap
      assert(result == expectedResult)
    }


  }
}
