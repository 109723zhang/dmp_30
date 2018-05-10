package cn.sheep.dmp.report

import cn.sheep.dmp.beans.ReportLogDataAnalysis
import cn.sheep.dmp.utils.{ConfigHandler, MySQLHandler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * 统计日志文件中各省市的数据分布情况
  * 第二种方式：
  *     Core方式
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
object LogDataAnalysisCore {


    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("统计日志文件中各省市的数据分布情况")
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

        val sparkContext = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sparkContext)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        // (key, value)
        val result = rawDataFrame.map(row => {
            val pname = row.getAs[String]("provincename")
            val cname = row.getAs[String]("cityname")

            ((pname, cname), 1)
        }).reduceByKey(_ + _)

        import sQLContext.implicits._

        val resultDF = result.map(tp => ReportLogDataAnalysis(tp._1._1, tp._1._2, tp._2)).toDF

        //resultDF.coalesce(1).write.json(ConfigHandler.logdataAnalysisResultJsonPath)


        // 将结果写出到MySQL
//        resultDF.write.mode(SaveMode.Overwrite).jdbc(ConfigHandler.url, ConfigHandler.logdataAnalysisTableName, ConfigHandler.dbProps)

        MySQLHandler.save2db(resultDF, ConfigHandler.logdataAnalysisTableName)

        sparkContext.stop()

    }

}
