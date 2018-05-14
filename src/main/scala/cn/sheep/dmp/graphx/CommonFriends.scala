package cn.sheep.dmp.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 借助spark graphx 求共同好友 - 好友推荐
  * sheep.Old @ 64341393
  * Created 2018/5/13
  */
object CommonFriends {
    // 屏蔽日志
    Logger.getLogger("org").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
          .setAppName("好友推荐")
          .setMaster("local[*]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(conf)


        // step 1. 构建出点集合和边集合
        val vertexRDD: RDD[(VertexId, (String, Int))] = sc.makeRDD(Seq(
            (1L, ("老羊", 18)),
            (2L, ("文文", 16)),
            (6L, ("璐璐", 16)),
            (9L, ("慧慧", 16)),
            (133L, ("凉凉", 20)),

            (138L, ("超哥", 22)),
            (16L, ("兴兴", 24)),
            (21L, ("亮亮", 35)),
            (44L, ("峰峰", 40)),

            (5L, ("海哥", 30)),
            (7L, ("果果", 31)),
            (158L, ("老王", 38))
        ))

        /*边集合*/
        val edgeRDD: RDD[Edge[Int]]= sc.makeRDD(Seq(
            Edge(1, 133, 0),
            Edge(2, 133, 0),
            Edge(6, 133, 0),
            Edge(9, 133, 0),

            Edge(6, 138, 0),
            Edge(16, 138, 0),
            Edge(21, 138, 0),
            Edge(44, 138, 0),

            Edge(5, 158, 0),
            Edge(7, 158, 0)
        ))

        // step 2. 构建图对象
        val graph = Graph(vertexRDD, edgeRDD)



        // step 3. 调用图对象的api (userId, commonMinId)
//        val cc = graph.connectedComponents().vertices
//        graph.pageRank(0.001).vertices.foreach(println)


        /*cc.join(vertexRDD).map{
            case (userId, (commonMinId, (name, age))) => (commonMinId, name)
        }.reduceByKey((a, b) => a + "," + b)
            .foreach(println)*/



        // 将元组的数据进行交换 (commonMinId, userId)
        /*cc.map(tp => (tp._2, tp._1.toString)).reduceByKey((a,b) => a.concat(",").concat(b)).foreach(println)*/




        sc.stop()
    }

}
