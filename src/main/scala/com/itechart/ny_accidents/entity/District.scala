package com.itechart.ny_accidents.entity

import com.vividsolutions.jts.geom.Geometry

case class District (districtName: String, boroughName: String, geometry: Geometry)
case class DistrictMongo (districtName: String, boroughName: String)