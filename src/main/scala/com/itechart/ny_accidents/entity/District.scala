package com.itechart.ny_accidents.entity

import com.vividsolutions.jts.geom.{Geometry, MultiPolygon}

case class District (districtName: String, boroughName: String, geometry: Geometry)
