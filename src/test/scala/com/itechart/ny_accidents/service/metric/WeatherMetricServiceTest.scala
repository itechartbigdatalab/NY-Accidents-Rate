package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.TestSparkApi
import com.itechart.ny_accidents.entity.{District, MergedData, WeatherForAccident}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSpec


class WeatherMetricServiceTest extends FunSpec {

  private val weatherMetricService = WeatherMetricService

  describe("Reports") {
    it("should return correct values grouping by phenomenon") {
      val data: RDD[MergedData] = TestSparkApi.spark.parallelize(Seq(
        MergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "A", .0, .0))),
        MergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "B", .0, .0))),
        MergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "C", .0, .0))),
        MergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "D", .0, .0)))
      ))

      val expectedResult: Seq[(String, Int, Double)] = Seq (
        ("B", 1, 25.0),
        ("A", 1, 25.0),
        ("C", 1, 25.0),
        ("D", 1, 25.0))

      val result: Seq[(String, Int, Double)] = weatherMetricService
        .getPhenomenonPercentage(data).collect().toSeq
      assert(result == expectedResult)
    }

    it("should return correct values grouping by districts") {
      val data: RDD[MergedData] = TestSparkApi.spark.parallelize(Seq(
        MergedData(null, Some(District("a", "A", null)), None),
        MergedData(null, Some(District("b", "A", null)), None),
        MergedData(null, Some(District("c", "A", null)), None),
        MergedData(null, Some(District("d", "A", null)), None)
      ))

      val expectedResult = Seq(
        ("d", 1, 25.0),
        ("a", 1, 25.0),
        ("b", 1, 25.0),
        ("c", 1, 25.0)
      )
      val result: Seq[(String, Int, Double)] = DistrictMetricService
        .getDistrictsPercentage(data).collect().toSeq

      assert(result == expectedResult)
    }

    it("should return correct values by Borough") {
      val data: RDD[MergedData] = TestSparkApi.spark.parallelize(Seq(
        MergedData(null, Some(District("a", "A", null)), None),
        MergedData(null, Some(District("b", "B", null)), None),
        MergedData(null, Some(District("c", "C", null)), None),
        MergedData(null, Some(District("d", "D", null)), None)
      ))
      val expectedResult: Seq[(String, Int, Double)] = Seq(
        ("B", 1, 25.0),
        ("A", 1, 25.0),
        ("C", 1, 25.0),
        ("D", 1, 25.0)
      )
      val result: Seq[(String, Int, Double)] = DistrictMetricService
        .getBoroughPercentage(data).collect().toSeq

      assert(result == expectedResult)
    }
  }
}