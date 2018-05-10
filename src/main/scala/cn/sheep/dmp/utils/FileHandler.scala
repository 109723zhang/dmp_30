package cn.sheep.dmp.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

/**
  * sheep.Old @ 64341393
  * Created 2018/5/7
  */
object FileHandler {


    def deleteWillOutputDir(sc: SparkContext, outputPath: String) = {
        val fs = FileSystem.get(sc.hadoopConfiguration)
        val path = new Path(outputPath)
        if (fs.exists(path)) {
            fs.delete(path, true)
        }
    }

}
