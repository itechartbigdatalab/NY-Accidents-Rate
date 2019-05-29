package com.itechart.ny_accidents.utils

import com.vividsolutions.jts.geom
import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import com.vividsolutions.jts.io.WKTReader

object PostgisUtils {
  def getGeometryFromText(text: String): Geometry = {
    new WKTReader().read(text)
  }

  def createPoint(latitude: Double, longitude: Double): Point = {
    val geometryFactory = new geom.GeometryFactory
    // PostgreDocumentation -> https://postgis.net/docs/ST_Point.html
    // geometry ST_Point(float x_lon, float y_lat);
    geometryFactory.createPoint(new Coordinate(longitude, latitude))
  }
}
