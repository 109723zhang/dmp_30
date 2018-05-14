package cn.sheep.dmp.tags

import ch.hsr.geohash.GeoHash
import cn.sheep.dmp.utils.MySQLHandler
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * sheep.Old @ 64341393
  * Created 2018/5/14
  */
object Tags4Business extends Tags {
    /**
      * 定义一个打标签的接口（规范）
      *
      * @param args
      * @return
      */
    override def makeTags(args: Any*): List[(String, Int)] = {
        var list = List[(String, Int)]()

        // 参数解析
        val row = args(0).asInstanceOf[Row]

        import cn.sheep.dmp.beans.SheepString._

        val lng = row.getAs[String]("long")
        val lat = row.getAs[String]("lat")

        // 需要判断吗
        if (lng.toDoublePlus >= 73 && lng.toDoublePlus <= 136 && lat.toDoublePlus >= 3 && lat.toDoublePlus <= 54 ){
            // 查库
            val geoCode = GeoHash.geoHashStringWithCharacterPrecision(lat.toDoublePlus, lng.toDoublePlus, 8)
            val business = MySQLHandler.findBusinessBy(geoCode)
            if (StringUtils.isNotEmpty(business)) {
                business.split(";").foreach(b => list :+= ("BT"+b, 1))
            }
        }
        list
    }
}
