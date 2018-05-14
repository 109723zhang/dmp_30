package cn.sheep.dmp.utils

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import scalikejdbc._

import scala.collection.mutable.ListBuffer

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

    def saveBusiness(list: ListBuffer[(String, String)]) = {
        DB.localTx{implicit session =>
            list.foreach(tp => {
                SQL("replace into business_dict_30 values(?,?)").bind(tp._1, tp._2).update().apply()
            })
        }

    }

    def findBusinessBy(geoHashCode: String) = {
        val list = DB.readOnly {implicit session =>
            SQL("select business from business_dict_30 where geo_hash_code=?").bind(geoHashCode)
              .map(rs => rs.string("business")).list().apply()
        }
        if (list.size != 0) list.head else null
    }

}
