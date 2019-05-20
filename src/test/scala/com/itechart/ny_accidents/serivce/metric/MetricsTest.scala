package com.itechart.ny_accidents.serivce.metric

import com.itechart.ny_accidents.entity.{District, MergedData, WeatherForAccident}
import com.itechart.ny_accidents.service.metric.Metrics
import org.scalatest.FunSpec

class MetricsTest extends FunSpec {
//  val testSubject = new Metrics()
//
//  describe("Reports") {
//    it("should return correct map of values grouping by phenomenon") {
//      val data = Seq(
//        MergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "A", .0, .0))),
//        MergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "B", .0, .0))),
//        MergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "C", .0, .0))),
//        MergedData(null, None, Some(WeatherForAccident(.0, .0, .0, "D", .0, .0)))
//      )
//
//      val expectedResult = Map("A" -> 25.0, "B" -> 25.0, "C" -> 25.0, "D" -> 25.0)
//
//      assert(testSubject.getPhenomenonPercentage(data) == expectedResult)
//    }
//
//    it("should return correct map of values grouping by districts") {
//      val data = Seq(
//        MergedData(null, Some(District("a", "A", null)), None),
//        MergedData(null, Some(District("b", "A", null)), None),
//        MergedData(null, Some(District("c", "A", null)), None),
//        MergedData(null, Some(District("d", "A", null)), None)
//      )
//
//      val expectedResult = Map("a" -> 25.0, "b" -> 25.0, "c" -> 25.0, "d" -> 25.0)
//
//      assert(testSubject.getDistrictsPercentage(data) == expectedResult)
//    }
//
//    it("should return correct map of values districts by Borough") {
//      val data = Seq(
//        MergedData(null, Some(District("a", "A", null)), None),
//        MergedData(null, Some(District("b", "B", null)), None),
//        MergedData(null, Some(District("c", "C", null)), None),
//        MergedData(null, Some(District("d", "D", null)), None)
//      )
//      val expectedResult = Map(
//        "A" -> Map("a" -> 100.0),
//        "B" -> Map("b" -> 100.0),
//        "C" -> Map("c" -> 100.0),
//        "D" -> Map("d" -> 100.0)
//      )
//      assert(testSubject.getDistrictsPercentageByBorough(data) == expectedResult)
//    }
//
//    it("should return correct map of values by Borough") {
//      val data = Seq(
//        MergedData(null, Some(District("a", "A", null)), None),
//        MergedData(null, Some(District("b", "B", null)), None),
//        MergedData(null, Some(District("c", "C", null)), None),
//        MergedData(null, Some(District("d", "D", null)), None)
//      )
//      val expectedResult = Map(
//        "A" -> 25.0,
//        "B" -> 25.0,
//        "C" -> 25.0,
//        "D" -> 25.0
//      )
//
//      assert(testSubject.getBoroughPercentage(data) == expectedResult)
//    }
//  }
}
