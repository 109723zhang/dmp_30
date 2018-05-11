package cn.sheep.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * sheep.Old @ 64341393
  * Created 2018/5/11
  */
object Tags4Area extends Tags {
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

        val pname = row.getAs[String]("provincename")
        val cname = row.getAs[String]("cityname")

        if (StringUtils.isNotEmpty(pname)) list :+= ("ZP"+pname, 1)
        if (StringUtils.isNotEmpty(cname)) list :+= ("ZC"+cname, 1)

        list
    }
}
