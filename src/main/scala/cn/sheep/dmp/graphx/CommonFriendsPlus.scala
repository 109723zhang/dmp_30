package cn.sheep.dmp.graphx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sheep.Old @ 64341393
  * Created 2018/5/13
  */
object CommonFriendsPlus {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
          .setAppName("好友推荐")
          .setMaster("local[*]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(conf)

        val data = sc.textFile("E:\\data\\graphxdata\\Graph_03_data.txt").map(_.split("\t"))

        // 构建点集合 RDD[(Long, String)]
        val verties: RDD[(Long, String)] = data.flatMap(nameArray => {
            nameArray.map(name => (name.hashCode.toLong, name))
        })

        // 构建边集合 RDD[Edge[ED]]
        val edges: RDD[Edge[Int]] = data.flatMap(nameArray => {
            // 拿出数组的头元素
            val head = nameArray.head.hashCode.toLong
            nameArray.map(name => Edge(head, name.hashCode.toLong, 0))
        })

        // 构建图对象
        val graph = Graph(verties, edges)

        // 调用api
        val cc = graph.connectedComponents().vertices

        cc.join(verties).map {
            case (userId, (commonMinId, name)) => (commonMinId, Set(name))
        }.reduceByKey(_ ++ _).map(tp => tp._2.mkString(",")).foreach(println)

        sc.stop()
    }

}
