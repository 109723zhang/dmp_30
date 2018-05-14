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
        // 设置job运行时的序列化方式
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

        // 创建SparkContext实例
        val sparkContext = new SparkContext(sparkConf)
        // 创建SQLContext实例
        val sQLContext = new SQLContext(sparkContext)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        // 将DataFrame注册成一张临时表
        rawDataFrame.registerTempTable("log")

        // 根据省份和地市进行分组统计
        val result = sQLContext.sql(
            """
              |select count(*) ct, provincename, cityname from log group by provincename, cityname
            """.stripMargin)

        // 删除即将要输出的目录
        FileHandler.deleteWillOutputDir(sparkContext, ConfigHandler.logdataAnalysisResultJsonPath)

        // 将结果写成json数据格式
        //result.coalesce(1).write.json(ConfigHandler.logdataAnalysisResultJsonPath)

        // 将结果写出到MySQL
        // result.write.mode(SaveMode.Overwrite).jdbc(ConfigHandler.url, ConfigHandler.logdataAnalysisTableName, ConfigHandler.dbProps)

        // 将数据写入到MySQL中
        MySQLHandler.save2db(result, ConfigHandler.logdataAnalysisTableName)

        // 关闭sparkContext实例
        sparkContext.stop()
    }

}
