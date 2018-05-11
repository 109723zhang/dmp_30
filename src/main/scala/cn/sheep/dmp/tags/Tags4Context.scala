package cn.sheep.dmp.tags

import cn.sheep.dmp.utils.{ConfigHandler, TagsHandler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 将当天的数据进行标签化处理 - 上下文标签
  * sheep.Old @ 64341393
  * Created 2018/5/11
  */
object Tags4Context {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("数据进行标签化处理 - 上下文标签")
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)
        // app字典
        val appDict = sc.textFile(ConfigHandler.appDictPath).map(_.split("\t", -1)).filter(_.length>=5).map(arr => (arr(4), arr(1))).collect().toMap
        val appDictBT = sc.broadcast(appDict)

        // 停用词字典
        val stopwordsDict = sc.textFile(ConfigHandler.stopwordPath).map((_, 0)).collect().toMap
        val stopWordsDictBT = sc.broadcast(stopwordsDict)

        val sQLContext = new SQLContext(sc)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        // 过滤出来至少含有一个用户id的数据
        val filteredDataFrame = rawDataFrame.filter(TagsHandler.hasNeedOneUserId)

        filteredDataFrame.map(row => {

            // 代码逻辑实现
            val userId = TagsHandler.getAnyOneUserId(row)

            val adTag = Tags4Ad.makeTags(row)
            val areaTag = Tags4Area.makeTags(row)
            val deviceTag = Tags4Device.makeTags(row)
            val appTag = Tags4App.makeTags(row, appDictBT)
            val kwTag = Tags4KeyWords.makeTags(row, stopWordsDictBT)

            // 商圈标签的实现

            (userId, adTag ++ areaTag ++ deviceTag ++ appTag ++ kwTag)
        }).reduceByKey {
            (list1, list2) => (list1 ::: list2).groupBy(_._1).mapValues(_.foldLeft[Int](0)(_ + _._2)).toList
        }
          .map(tp => tp._1 + "\t" + tp._2.map(x => x._1+":"+x._2).mkString(","))
          .saveAsTextFile("F:\\dmp\\contextTags")

        sc.stop()

    }

}
