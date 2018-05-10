package cn.sheep.dmp.etl

import cn.sheep.dmp.beans.Log
import cn.sheep.dmp.utils.{ConfigHandler, Jpools}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将离线的app字典数据写入到redis中
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
object AppDict2Redis {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf()
          .setAppName("将原始日志文件转换成parquet文件格式")
          .setMaster("local[*]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        // 创建sparkcontext
        val sc = new SparkContext(sparkConf)

        sc.textFile(ConfigHandler.appDictPath)
          .map(line => line.split("\t", -1)).filter(_.length >= 5)
          .map(fields => (fields(4), fields(1)))
          .filter(tp => StringUtils.isNotEmpty(tp._1) && StringUtils.isNotEmpty(tp._2))
          .foreachPartition(iter => {
              val jedis = Jpools.getJedis

              iter.foreach(tp => {
                  jedis.hset("appdict", tp._1, tp._2)
              })

              jedis.close()
          })

        sc.stop()
    }

}
