package com.itechart.ny_accidents.database.dao.cache

import java.io.File

import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.entity.MergedData
import org.ehcache.config.ResourceUnit
import org.ehcache.config.builders.{CacheConfigurationBuilder, CacheManagerBuilder, ResourcePoolsBuilder}
import org.ehcache.config.units.{EntryUnit, MemoryUnit}

import scala.util.Try

object EhCacheDAO extends MergedDataCacheDAO {
  private lazy val manager = CacheManagerBuilder.newCacheManagerBuilder()
    .`with`(CacheManagerBuilder.persistence(new File(Configuration.CACHE_PATH)))
    .withCache(Configuration.CACHE_NAME,
      CacheConfigurationBuilder.newCacheConfigurationBuilder(classOf[String], classOf[MergedData],
        ResourcePoolsBuilder.newResourcePoolsBuilder()
//          .offheap(Configuration.CACHE_OFF_HEAP_SIZE, MemoryUnit.GB)
//          .heap(Configuration.CACHE_HEAP_SIZE, EntryUnit.ENTRIES)
          .disk(Configuration.CACHE_DISK_SIZE, MemoryUnit.GB, true)))
    .build(true)
  private lazy val cache = manager.getCache(Configuration.CACHE_NAME, classOf[String], classOf[MergedData])

  override def cacheMergedData(userData: MergedData): Unit = {
    Try(cache.put(userData.accident.uniqueKey.get.toString, userData))
  }

  override def readMergedDataFromCache(key: Long): Option[MergedData] = {
    cache.containsKey(key.toString) match {
      case true => Some(cache.get(key.toString))
      case false => None
    }
  }

  def close(): Unit = manager.close()
}
