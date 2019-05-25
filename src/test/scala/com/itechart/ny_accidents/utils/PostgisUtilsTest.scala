package com.itechart.ny_accidents.utils

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.FunSpec

class PostgisUtilsTest extends FunSpec {
  describe("A PostgisUtils") {
    it("should create postgis Point in correct format") {
      val (testDataLatitude, testDataLongitude) = (-70.0, 40.0)
      val geometryFactory = new GeometryFactory
      val expectedValue = geometryFactory.createPoint(new Coordinate(testDataLongitude, testDataLatitude))
      val result = PostgisUtils.createPoint(testDataLatitude, testDataLongitude)

      assert(result == expectedValue)
    }
  }
}