package cn.sheep.dmp.tags

import org.apache.spark.sql.Row

/**
  * sheep.Old @ 64341393
  * Created 2018/5/11
  */
object Tags4Device extends Tags {
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

        // 设备操作系统
        val client = row.getAs[Int]("client")
        client match {
            case 1 => list :+= ("D00010001", 1)
            case 2 => list :+= ("D00010002", 1)
            case 3 => list :+= ("D00010003", 1)
            case _ => list :+= ("D00010004", 1)
        }


        //获取设备联网方式字段
        val netName = row.getAs[String]("networkmannername").toUpperCase

        netName match {
            case "WIFI" => list :+= ("D00020001", 1)
            case "4G" => list :+= ("D00020002", 1)
            case "3G" => list :+= ("D00020003", 1)
            case "2G" => list :+= ("D00020004", 1)
            case _ => list :+= ("D00020005", 1)
        }



        //获取运营商名称字段
        val ispName = row.getAs[String]("ispname")
        ispName match {
            case "移动" => list :+= ("D00030001", 1)
            case "联通" => list :+= ("D00030002", 1)
            case "电信" => list :+= ("D00030003", 1)
            case _ => list :+= ("D00030004", 1)
        }

        list
    }
}
