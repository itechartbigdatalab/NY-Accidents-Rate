package com.itechart.ny_accidents.entity

case class DetailedDistrictData (districtName: String, pedestriansInjured: Int, pedestriansKilled: Int,
                                 cyclistInjured: Int, cyclistKilled: Int,
                                 motoristInjured: Int, motoristKilled: Int, total: Int, pedestrians:Int, cyclist:Int, motorist:Int)
