package cn.sheep.dmp.report

import cn.sheep.dmp.utils.{ConfigHandler, MySQLHandler, RptKpiTools}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
object RptMediaAnalysisRedis {


    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("媒体分析")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sc)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)
          .filter("appid!=null or appid!='' or appname!=null or appname!=''")

        rawDataFrame.registerTempTable("log")


        sQLContext.udf.register("NotEmptyAppName",  RptKpiTools.notEmptyAppName)

        val result = sQLContext
          .sql(
            """
              |select NotEmptyAppName(appid, appname) appName,
              |sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) rawReq,
              |sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) effReq,
              |sum(case when requestmode=1 and processnode=3 then 1 else 0 end) adReq,
              |sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) rtbReq,
              |sum(case when iseffective=1 and isbilling=1 and iswin = 1 then 1 else 0 end) winReq,
              |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) adShow,
              |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) adClick,
              |sum(case when iseffective=1 and isbilling=1 and iswin = 1 then winprice/1000 else 0 end) adCost,
              |sum(case when iseffective=1 and isbilling=1 and iswin = 1 then adpayment/1000 else 0 end) adPayment
              |from log group by NotEmptyAppName(appid, appname)
            """.stripMargin)

        MySQLHandler.save2db(result, ConfigHandler.mediaAnalysisTableName)

        sc.stop()
    }

}
