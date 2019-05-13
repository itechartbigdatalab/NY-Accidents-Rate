package com.itechart.ny_accidents.utils

import com.vividsolutions.jts.geom
import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import com.vividsolutions.jts.io.WKTReader

object PostgisUtils {
  def getMultiPolygonFromText(text: String): Geometry = {
    new WKTReader().read(text)
  }

  def createPoint(latitude: Double, longitude: Double): Point = {
    val geometryFactory = new geom.GeometryFactory
    geometryFactory.createPoint(new Coordinate(latitude, longitude))
  }
}
