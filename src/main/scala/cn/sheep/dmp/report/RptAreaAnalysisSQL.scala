package cn.sheep.dmp.report

import cn.sheep.dmp.utils.{ConfigHandler, MySQLHandler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 地域分布 - SQL
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
object RptAreaAnalysisSQL {


    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("地域分布")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sc)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        // 注册临时表
        rawDataFrame.registerTempTable("log")

        /**
          * 自定义的udf
          */
        sQLContext.udf.register("area_func", (boolean: Boolean, result: Double) => if (boolean) result else 0)


        /**
          * 把日志中的原始请求的日志查出来
          * select count(1)  from log where requestmode=1 and processnode>=1 group by provincename, cityname
          */
        val result = sQLContext.sql(
            """
              |select provincename, cityname,
              |sum(area_func(requestmode=1 and processnode>=1, 1)) rawReq,
              |sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) effReq,
              |sum(case when requestmode=1 and processnode=3 then 1 else 0 end) adReq,
              |sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) rtbReq,
              |sum(case when iseffective=1 and isbilling=1 and iswin = 1 then 1 else 0 end) winReq,
              |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) adShow,
              |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) adClick,
              |sum(case when iseffective=1 and isbilling=1 and iswin = 1 then winprice/1000 else 0 end) adCost,
              |sum(area_func(iseffective=1 and isbilling=1 and iswin = 1, adpayment/1000)) adPayment
              |from log group by provincename, cityname
            """.stripMargin)

        // 存储数据到MySQL中
        MySQLHandler.save2db(result, ConfigHandler.areaAnalysisTableName)


        sc.stop()
    }

}
