package com.itechart.ny_accidents.service.metric

import com.google.inject.Guice
import com.itechart.ny_accidents.{GuiceModule, TestSparkApi}
import com.itechart.ny_accidents.entity.{District, MergedData, ReportMergedData, WeatherForAccident}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.scalatest.FunSpec

import scala.collection.immutable

/*
class WeatherMetricServiceTest extends FunSpec {


  private val injector = Guice.createInjector(new GuiceModule)
  private val weatherMetricService = injector.getInstance(classOf[WeatherMetricService])

  describe("Reports") {
    it("should return correct map of values grouping by phenomenon") {
      val data: RDD[ReportMergedData] = TestSparkApi.spark.parallelize(Seq(
        ReportMergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "A", .0, .0))),
        ReportMergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "B", .0, .0))),
        ReportMergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "C", .0, .0))),
        ReportMergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "D", .0, .0)))
      ))

      val expectedResult: Map[String, Double] = Map[String, Double](
        "A" -> 25.0,
        "B" -> 25.0,
        "C" ->  25.0,
        "D" -> 25.0)
      val result: Map[String, Double] = weatherMetricService.getPhenomenonPercentage(data).collect().toMap
      assert(result == expectedResult)
    }

    it("should return correct map of values grouping by districts") {
      val data: RDD[ReportMergedData] = TestSparkApi.spark.parallelize(Seq(
        ReportMergedData(null, Some(District("a", "A", null)), None),
        ReportMergedData(null, Some(District("b", "A", null)), None),
        ReportMergedData(null, Some(District("c", "A", null)), None),
        ReportMergedData(null, Some(District("d", "A", null)), None)
      ))

      val expectedResult: Map[String, Double] = Map[String, Double](
        "a" -> 25.0,
        "b" -> 25.0,
        "c" -> 25.0,
        "d" -> 25.0)
      val result = DistrictMetricService.getDistrictsPercentage(data).collect().toMap

      assert(result == expectedResult)
    }

    it("should return correct map of values by Borough") {
      val data: RDD[ReportMergedData] = TestSparkApi.spark.parallelize(Seq(
        ReportMergedData(null, Some(District("a", "A", null)), None),
        ReportMergedData(null, Some(District("b", "B", null)), None),
        ReportMergedData(null, Some(District("c", "C", null)), None),
        ReportMergedData(null, Some(District("d", "D", null)), None)
      ))
      val expectedResult: Map[String, Double] = Map[String, Double](
        "A" -> 25.0,
        "B" -> 25.0,
        "C" -> 25.0,
        "D" -> 25.0
      )
      val result = DistrictMetricService.getBoroughPercentage(data).collect().toMap

      assert(result == expectedResult)
    }
  }
}
*/
