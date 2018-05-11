package cn.sheep.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * sheep.Old @ 64341393
  * Created 2018/5/11
  */
object Tags4App extends Tags {
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
        val appdict = args(1).asInstanceOf[Broadcast[Map[String, String]]]

        val appId = row.getAs[String]("appid")
        val appName = row.getAs[String]("appname")

        if (StringUtils.isNotEmpty(appName)) {
            list :+= ("APP"+appName, 1)
        } else if(StringUtils.isNotEmpty(appId)) {
            list :+= ("APP"+appdict.value.getOrElse(appId, appId), 1)
        }

        list
    }
}
