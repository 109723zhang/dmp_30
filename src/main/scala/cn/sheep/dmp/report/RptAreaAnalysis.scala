package cn.sheep.dmp.report

import cn.sheep.dmp.beans.ReportAreaAnalysis
import cn.sheep.dmp.utils.{ConfigHandler, MySQLHandler, RptKpiTools}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * 地域分布 - core
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
object RptAreaAnalysis {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("地域分布")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sc)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        import sQLContext.implicits._
        val resultDF = rawDataFrame.map(row => {

            val pname = row.getAs[String]("provincename")
            val cname = row.getAs[String]("cityname")

            ((pname, cname), RptKpiTools.offLineKpi(row))
        })
          .reduceByKey((list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2))
          .map(rs => ReportAreaAnalysis(rs._1._1, rs._1._2, rs._2(0), rs._2(1), rs._2(2), rs._2(3), rs._2(4), rs._2(5), rs._2(6), rs._2(7), rs._2(8)))
          .toDF


        MySQLHandler.save2db(resultDF, ConfigHandler.areaAnalysisTableName)

        sc.stop()
    }


}
