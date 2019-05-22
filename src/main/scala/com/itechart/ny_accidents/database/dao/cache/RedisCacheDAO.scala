package com.itechart.ny_accidents.database.dao.cache

import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.utils.SerializationUtil
import org.slf4j.LoggerFactory
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.util.Try

object RedisCacheDAO extends MergedDataCacheDAO {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  private val REDIS_HOST: String = "127.0.0.1"
  private val REDIS_PORT: Int = 6379
  private lazy val poolConf =  new JedisPoolConfig()
  poolConf.setMaxTotal(128)

  private lazy val redis = new JedisPool(poolConf, REDIS_HOST, REDIS_PORT)
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
}