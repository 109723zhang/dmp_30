package cn.sheep.dmp.report

import cn.sheep.dmp.beans.ReportMediaAnalysis
import cn.sheep.dmp.utils.{ConfigHandler, MySQLHandler, RptKpiTools}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 媒体分析 - Broadcast
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
object RptMediaAnalysis {


    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("媒体分析")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)

        // 字典数据广播出去
        val appdictMap = sc.textFile(ConfigHandler.appDictPath)
          .map(line => line.split("\t", -1))
          .filter(_.length >= 5)
          .map(arr => (arr(4), arr(1))).collect().toMap

        val appdictBT: Broadcast[Map[String, String]] = sc.broadcast(appdictMap)


        val sQLContext = new SQLContext(sc)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        import sQLContext.implicits._
        val result = rawDataFrame
          .filter("appid!=null or appid!='' or appname!=null or appname!=''")
          .map(row => {

              val appId = row.getAs[String]("appid")
              var appName = row.getAs[String]("appname")

              if (StringUtils.isEmpty(appName)) {
                  appName = appdictBT.value.getOrElse(appId, appId)
              }
              (appName, RptKpiTools.offLineKpi(row))
          }).reduceByKey {
            (list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2)
        }.map(rs => ReportMediaAnalysis(rs._1, rs._2(0), rs._2(1), rs._2(2), rs._2(3), rs._2(4), rs._2(5), rs._2(6), rs._2(7), rs._2(8)))
          .toDF


        MySQLHandler.save2db(result, ConfigHandler.mediaAnalysisTableName)

        sc.stop()
    }

}
