package com.itechart.ny_accidents.database.dao.cache


import java.io.File

import com.google.inject.Singleton
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.entity.MergedData
import org.ehcache.config.builders.{CacheConfigurationBuilder, CacheManagerBuilder, ResourcePoolsBuilder}
import org.ehcache.config.units.MemoryUnit

import scala.util.Try

@Singleton
class EhCacheDAO extends MergedDataCacheDAO {
  private lazy val manager = CacheManagerBuilder.newCacheManagerBuilder()
    .`with`(CacheManagerBuilder.persistence(new File(Configuration.CACHE_PATH)))
    .withCache(Configuration.CACHE_NAME,
      CacheConfigurationBuilder.newCacheConfigurationBuilder(classOf[String], classOf[MergedData],
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .disk(Configuration.CACHE_DISK_SIZE, MemoryUnit.GB, true)))
    .build(true)
  private lazy val cache = manager.getCache(Configuration.CACHE_NAME, classOf[String], classOf[MergedData])

  override def cacheMergedData(userData: MergedData): Unit = {
    Try(cache.put(userData.accident.uniqueKey.get.toString, userData))
  }

  override def readMergedDataFromCache(key: Long): Option[MergedData] = {
    if (cache.containsKey(key.toString)) {
      Some(cache.get(key.toString))
    } else {
      None
    }
  }

  def close: Unit = manager.close()
}