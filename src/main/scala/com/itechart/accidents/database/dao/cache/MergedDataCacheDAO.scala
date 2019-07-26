package com.itechart.accidents.database.dao.cache

import com.itechart.accidents.entity.MergedData


trait MergedDataCacheDAO {
  def cacheMergedData(userData: MergedData)
  def readMergedDataFromCache(key: Long): Option[MergedData]
  def close
}