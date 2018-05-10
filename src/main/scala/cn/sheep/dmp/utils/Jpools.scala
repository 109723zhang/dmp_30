package cn.sheep.dmp.utils

import redis.clients.jedis.{Jedis, JedisPool}

/**
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
object Jpools {


    private lazy val jedisPool = new JedisPool(ConfigHandler.redisHost, ConfigHandler.redisPort)

    def getJedis: Jedis = {
        val jedis = jedisPool.getResource
        jedis.select(ConfigHandler.redisdbIndex)
        jedis
    }

}
