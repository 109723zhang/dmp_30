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

        // 设置程序运行的相关参数
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("媒体分析")
        // 设置序列化方式
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        // 创建SparkContext实例对象
        val sc = new SparkContext(sparkConf)

        // 字典数据广播出去
        val appdictMap = sc.textFile(ConfigHandler.appDictPath)
          // 对每一行数据进行切分，并过滤掉长度小于5的数据
          .map(line => line.split("\t", -1)).filter(_.length >= 5)
          // 提取appId, appName并组装成元组
          .map(arr => (arr(4), arr(1)))
          // 将数据收集到Driver端，并将转换成Map
          .collect().toMap

        // 使用广播的方式将字典数据广播到Executor端
        val appdictBT: Broadcast[Map[String, String]] = sc.broadcast(appdictMap)

        // 创建SQLContext实例
        val sQLContext = new SQLContext(sc)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        import sQLContext.implicits._
        val result = rawDataFrame
          // 过滤掉appId和appName都为空的数据
          .filter("appid!=null or appid!='' or appname!=null or appname!=''")
          .map(row => {
              // 提取appId appName
              val appId = row.getAs[String]("appid")
              var appName = row.getAs[String]("appname")

              if (StringUtils.isEmpty(appName)) {
                  // 如果appName为空，则根据appId从广播变量中找，找不到直接返回appId
                  appName = appdictBT.value.getOrElse(appId, appId)
              }

              /*计算改行数据的KPI指标，并将指标装入List*/
              val kpi: List[Double] = RptKpiTools.offLineKpi(row)
              (appName, kpi)
          })
          // 根据appName进行聚合
          .reduceByKey {
              (list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2)// 对List进行拉链操作然后对zip后得到的元组求和
          }
          // 将结果数据封装成case class, 然后直接转成DataFrame
          .map(rs => ReportMediaAnalysis(rs._1, rs._2(0), rs._2(1), rs._2(2), rs._2(3), rs._2(4), rs._2(5), rs._2(6), rs._2(7), rs._2(8))).toDF

        // 将数据写入到MySQL中
        MySQLHandler.save2db(result, ConfigHandler.mediaAnalysisTableName)

        sc.stop()
    }

}
