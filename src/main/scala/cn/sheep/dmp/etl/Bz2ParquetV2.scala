package cn.sheep.dmp.etl

import cn.sheep.dmp.beans.Log
import cn.sheep.dmp.utils.FileHandler
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * sheep.Old @ 64341393
  * Created 2018/5/7
  */
object Bz2ParquetV2 {

    def main(args: Array[String]): Unit = {

        // 检验参数
        if (args.length != 2) {
            println(
                """
                  |cn.sheep.dmp.etl.Bz2ParquetV2
                  |参数：dateInputPath, outputPath
                """.stripMargin)
            sys.exit()
        }

        // 模式匹配
        val Array(dataInputPath, outputPath) = args

        val sparkConf = new SparkConf()
          .setAppName("将原始日志文件转换成parquet文件格式")
          .setMaster("local[*]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          // 通常自定义的类，需要手动的注册成kryo的序列化方式
          .registerKryoClasses(Array(classOf[Log]))

        // 创建sparkcontext
        val sc = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sc)
        sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")

        // 读取数据
        val rawData: RDD[String] = sc.textFile(dataInputPath)

        // 处理数据
        val arrRdd: RDD[Array[String]] = rawData
          .map(line => line.split(",", -1))
          // 过滤掉不符合字段要求的数据
          .filter(_.length >= 85)

        val logRdd: RDD[Log] = arrRdd.map(Log(_))

        val dataFrame = sQLContext.createDataFrame(logRdd)

        FileHandler.deleteWillOutputDir(sc, outputPath)

        // 保存数据
        dataFrame.write.parquet(outputPath)

        sc.stop()
    }

}
