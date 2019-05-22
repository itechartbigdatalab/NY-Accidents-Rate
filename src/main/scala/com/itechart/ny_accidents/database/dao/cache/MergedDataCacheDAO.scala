package com.itechart.ny_accidents.database.dao.cache

import com.google.inject.Singleton
import com.itechart.ny_accidents.entity.MergedData


trait MergedDataCacheDAO {
  def cacheMergedData(userData: MergedData)
  def readMergedDataFromCache(key: Long): Option[MergedData]
}