package com.itechart.accidents.entity

import com.vividsolutions.jts.geom.Geometry

case class DistrictWithGeometry(districtName: String, boroughName: String, geometry: Geometry)
case class District (districtName: String, boroughName: String)

// For backward compatibility
case class DistrictMongo (districtName: String, boroughName: String)