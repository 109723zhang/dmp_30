package cn.sheep.dmp.tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * sheep.Old @ 64341393
  * Created 2018/5/11
  */
object Tags4KeyWords extends Tags {
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
        val stopWordsDict = args(1).asInstanceOf[Broadcast[Map[String, Int]]]


        val kwds = row.getAs[String]("keywords").split("\\|")

        kwds
          .filter(word => word.length >= 3 && word.length <= 8 && !stopWordsDict.value.contains(word))
          .foreach(word => list :+= ("K"+word, 1))

        list
    }
}
