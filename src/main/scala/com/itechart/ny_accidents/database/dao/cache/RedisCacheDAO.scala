package com.itechart.ny_accidents.database.dao.cache

import com.google.inject.Singleton
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.utils.SerializationUtil
import org.slf4j.LoggerFactory
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.util.Try

@Singleton
class RedisCacheDAO extends MergedDataCacheDAO {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  private lazy val poolConf =  new JedisPoolConfig()
  poolConf.setMaxTotal(Configuration.REDIS_POOL_SIZE)

  private lazy val redis = new JedisPool(poolConf,
    Configuration.REDIS_HOST,
    Configuration.REDIS_PORT)
  private final val QUERY_PREFIX = "merged_data_cache:id:"

  override def cacheMergedData(userData: MergedData) = {
    userData.accident.uniqueKey match {
      case Some(pk) =>
        val redisClient = redis.getResource
        redisClient.set(QUERY_PREFIX + pk, SerializationUtil.serialize(userData))
        redisClient.close()
    }
  }

  override def readMergedDataFromCache(key: Long): Option[MergedData] = {
    val redisClient = redis.getResource
    val objectString: String = redisClient.get(QUERY_PREFIX + key)
    redisClient.close()

    Try(SerializationUtil.deserialize[MergedData](objectString)).toOption
  }

  def close = logger.debug("Redis cache close")
}