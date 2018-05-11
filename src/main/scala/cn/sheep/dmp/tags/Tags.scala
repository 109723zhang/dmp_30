package cn.sheep.dmp.tags

/**
  * sheep.Old @ 64341393
  * Created 2018/5/11
  */
trait Tags {

    /**
      * 定义一个打标签的接口（规范）
      * @param args
      * @return
      */
    def makeTags(args: Any*): List[(String, Int)]

}
