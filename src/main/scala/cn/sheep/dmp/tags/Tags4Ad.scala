package cn.sheep.dmp.tags

import org.apache.spark.sql.Row

/**
  * 处理跟广告相关的标签
  * sheep.Old @ 64341393
  * Created 2018/5/11
  */
object Tags4Ad extends Tags {
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

        // 广告位类型
        val adType = row.getAs[Int]("adspacetype")
        adType match {
            case v if v > 9 => list :+= ("LC"+v, 1)
            case v if v > 0 && v < 10 => list :+= ("LC0"+v, 1)
        }

        // 渠道的
        val channel = row.getAs[Int]("adplatformproviderid")
        if (channel != 0) list :+= ("CN"+channel, 1)


        list
    }

}
