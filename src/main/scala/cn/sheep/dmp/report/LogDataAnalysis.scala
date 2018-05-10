package cn.sheep.dmp.report

import cn.sheep.dmp.utils.{ConfigHandler, FileHandler, MySQLHandler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * 统计日志文件中各省市的数据分布情况
  * 第一种方式：
  *     SQL方式
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
object LogDataAnalysis {


    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("统计日志文件中各省市的数据分布情况")
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

        val sparkContext = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sparkContext)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        // 按照需求进行相应的分析
        rawDataFrame.registerTempTable("log")
        // 根据省份和地市进行分组统计
        val result = sQLContext.sql(
            """
              |select count(*) ct, provincename, cityname
              |from log group by provincename, cityname
            """.stripMargin)

        FileHandler.deleteWillOutputDir(sparkContext, ConfigHandler.logdataAnalysisResultJsonPath)

        // 将结果写成json数据格式
        //result.coalesce(1).write.json(ConfigHandler.logdataAnalysisResultJsonPath)


        // 将结果写出到MySQL
        // result.write.mode(SaveMode.Overwrite).jdbc(ConfigHandler.url, ConfigHandler.logdataAnalysisTableName, ConfigHandler.dbProps)

        MySQLHandler.save2db(result, ConfigHandler.logdataAnalysisTableName)


        sparkContext.stop()
    }

}
