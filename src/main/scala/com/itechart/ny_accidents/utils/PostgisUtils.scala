package com.itechart.ny_accidents.utils

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory, MultiPolygon, Point}
import com.vividsolutions.jts.io.WKTReader

object PostgisUtils {
  def getMultiPolygonFromText(text: String): Geometry = {
    new WKTReader().read(text)
  }
}
