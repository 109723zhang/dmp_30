package cn.sheep.dmp.etl

import ch.hsr.geohash.GeoHash
import cn.sheep.dmp.utils.{BaiduLBSHandler, ConfigHandler, MySQLHandler}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

/**
  * 将日志文件中的经纬度抽取出来，转成商圈信息
  * sheep.Old @ 64341393
  * Created 2018/5/13
  */
object LatLng2Business {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("将日志文件中的经纬度抽取出来，转成商圈信息")
        // 设置job运行时的序列化方式
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

        // 创建SparkContext实例
        val sparkContext = new SparkContext(sparkConf)
        // 创建SQLContext实例
        val sQLContext = new SQLContext(sparkContext)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        // 从数据中抽取经纬度数据
        // 中国的经纬度范围大约为：纬度3.86~53.55，经度73.66~135.05
        rawDataFrame.select("long", "lat").filter(
            """
              |cast(long as double) >= 73 and cast(long as double) <= 136 and
              |cast(lat as double) >= 3 and cast(lat as double) <= 54
            """.stripMargin).distinct()
            .foreachPartition(iter => {
                val list = new ListBuffer[(String, String)]()
                iter.foreach(row => {
                    val lat = row.getAs[String]("lat")
                    val lng = row.getAs[String]("long")

                    val business = BaiduLBSHandler.parseBusinessTagBy(lng, lat)
                    val geoHashCode = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, lng.toDouble, 8)
                    if (StringUtils.isNotEmpty(business))  list.append((geoHashCode, business))
                })

                // 保存数据 MySQL
                MySQLHandler.saveBusiness(list)
            })


        sparkContext.stop()
    }

}
