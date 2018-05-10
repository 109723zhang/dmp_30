package cn.sheep.dmp.utils

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
object MySQLHandler {


    def save2db(resultDF: DataFrame, tblName: String, partition: Int = 4) = {
        resultDF.coalesce(partition).write.mode(SaveMode.Overwrite).jdbc(
            ConfigHandler.url,
            tblName,
            ConfigHandler.dbProps
        )
    }

}
