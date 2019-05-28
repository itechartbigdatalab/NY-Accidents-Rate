package com.itechart.ny_accidents.utils

import com.vividsolutions.jts.geom
import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader

object PostgisUtils {
  private lazy val US_NATIONAL_ALTLAS_AREA_SRID = 2163

  def getGeometryFromText(text: String): Geometry = {
    new WKTReader().read(text)
  }

  def createPoint(latitude: Double, longitude: Double): Point = {
    val geometryFactory = new geom.GeometryFactory(new PrecisionModel(), US_NATIONAL_ALTLAS_AREA_SRID)
    // PostgreDocumentation -> https://postgis.net/docs/ST_Point.html
    // geometry ST_Point(float x_lon, float y_lat);
    geometryFactory.createPoint(new Coordinate(longitude, latitude))
  }
}
