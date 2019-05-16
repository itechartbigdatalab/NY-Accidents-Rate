package com.itechart.ny_accidents.districts.parser

import com.itechart.ny_accidents.entity.WeatherForAccident
import com.itechart.ny_accidents.weather.WeatherMappingService
import org.scalatest.{FunSpec, PrivateMethodTester}

class WeatherServiceTest extends FunSpec {

  describe("WeatherService") {
    it("should return correct pressure") {
      val resultValue = 757.4
      val service = new WeatherMappingService()

      assert(resultValue == service.findWeatherByTimeAndCoordinates(1557862500000L, 40.597415, -74.164599)
        .pressure)
    }
  }

}
