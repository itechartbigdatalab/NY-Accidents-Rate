package com.itechart.ny_accidents.weather

import com.itechart.ny_accidents.districts.controller.DistrictsDatabase
import com.itechart.ny_accidents.weather.parser.WeatherParser

object Application extends App {

  val DATA = "/home/aliaksandr/Downloads/CentralPark.csv"
  val weatherDAO = new WeatherDAO()
  val parser = new WeatherParser()

  val list = parser.parseCsv(DATA)
      .map(weatherDAO.insert)
      .map(DistrictsDatabase.database.run)

}
