package cn.sheep.dmp.graphx

import cn.sheep.dmp.tags._
import cn.sheep.dmp.utils.{ConfigHandler, TagsHandler}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * 借助图计算中的连通图算法解决多个渠道用户身份识别问题
  * 最终得到当天的最终的用户标签数据
  * sheep.Old @ 64341393
  * Created 2018/5/14
  */
object Tags4TodayFinal {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("Final上下文标签")
        // 设置job运行时的序列化方式
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        // 创建SparkContext实例
        val sc = new SparkContext(sparkConf)
        // 创建SQLContext实例
        val sQLContext = new SQLContext(sc)

        // 读取数据
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        // app字典
        val appDict = sc.textFile(ConfigHandler.appDictPath).map(_.split("\t", -1)).filter(_.length >= 5).map(arr => (arr(4), arr(1))).collect().toMap
        val appDictBT = sc.broadcast(appDict)
        // 停用词字典
        val stopwordsDict = sc.textFile(ConfigHandler.stopwordPath).map((_, 0)).collect().toMap
        val stopWordsDictBT = sc.broadcast(stopwordsDict)


        val baseRdd: RDD[(List[String], Row)] = rawDataFrame.filter(TagsHandler.hasNeedOneUserId).map(row => {
            // 获取当前行上的所有的用户非空id
            val allUserIds = TagsHandler.getCurrentRowAllUserId(row)
            (allUserIds, row)
        })


        // 点集合
        val verties: RDD[(VertexId, List[(String, Int)])] = baseRdd.flatMap(tp => {

            val row = tp._2
            // 标签
            val adTag = Tags4Ad.makeTags(row)
            val areaTag = Tags4Area.makeTags(row)
            val deviceTag = Tags4Device.makeTags(row)
            val appTag = Tags4App.makeTags(row, appDictBT)
            val kwTag = Tags4KeyWords.makeTags(row, stopWordsDictBT)
            // 商圈标签的实现
            val bsTag = Tags4Business.makeTags(row)
            // 当前所打上的标签数据
            val currentRowTag = adTag ++ areaTag ++ deviceTag ++ appTag ++ kwTag ++ bsTag

            //  List[(String, Int)] = (标签: V>0, 用户的ID: 0)
            val VD = tp._1.map((_, 0)) ++ currentRowTag

            // 只有第一个人可以携带顶点VD，其他都不要携带?
            // why: 如果同一行上的多个顶点都携带VD, 因为同一行的数据属于一个用户的，将来肯定会聚合到一起，这样就会造成重复的叠加了
            tp._1.map(uId => {
                if (tp._1.head.equals(uId)) {
                    (uId.hashCode.toLong, VD)
                } else {
                    (uId.hashCode.toLong, List.empty)
                }
            })
        })


        // 边集合构建
        val edges: RDD[Edge[Int]] = baseRdd.flatMap(tp => {
            tp._1.tail.map(uId => Edge(tp._1.head.hashCode.toLong, uId.hashCode.toLong, 0)) // A B C : A->B A->C
        })


        // 图对象
        val graph = Graph(verties, edges)

        // 调用连通图算法，找到图中可以联通的分支，并取出每个连通分支中的所有点和其分支中最小的点的元组集合（vId, commonMinId）
        val cc = graph.connectedComponents().vertices


        // 认祖归宗
        cc.join(verties).map {
            case (uId, (commonId, tagsAndUserId)) => (commonId, tagsAndUserId)
        }.reduceByKey {
            case (list1, list2) => (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
        }.saveAsTextFile("F:\\dmp\\finalTags")


        sc.stop()
    }

}
