package com.itechart.accidents.database.dao.cache

import com.google.inject.Singleton
import com.itechart.accidents.constants.Configuration
import com.itechart.accidents.entity.MergedData
import com.itechart.accidents.utils.SerializationUtils
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
        redisClient.set(QUERY_PREFIX + pk, SerializationUtils.serialize(userData))
        redisClient.close()
    }
  }

  override def readMergedDataFromCache(key: Long): Option[MergedData] = {
    val redisClient = redis.getResource
    val objectString: String = redisClient.get(QUERY_PREFIX + key)
    redisClient.close()

    Try(SerializationUtils.deserialize[MergedData](objectString)).toOption
  }

  def close = logger.debug("Redis cache close")
}