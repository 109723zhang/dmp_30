package cn.sheep.dmp.utils

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import scalikejdbc.config.DBs

import scala.util.Random

/**
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
object ConfigHandler {

    private lazy val config: Config = ConfigFactory.load()

    // parquet文件所在的路径
    val parquetPath = config.getString("parquet.path")

    //停用词库的位置
    val stopwordPath = config.getString("stopword")

    val logdataAnalysisResultJsonPath = config.getString("rpt.logdataAnalysis")

    // 关于MySQL配置
    val dirver: String = config.getString("db.default.driver")
    val url: String = config.getString("db.default.url")
    val user: String = config.getString("db.default.user")
    val passwd: String = config.getString("db.default.password")
    val logdataAnalysisTableName: String = config.getString("db.logdataAnalysis.table")
    val areaAnalysisTableName: String = config.getString("db.areaAnalysis.table")
    val mediaAnalysisTableName: String = config.getString("db.mediaAnalysis.table")

    // 封转MySQL Props
    val dbProps = new Properties()
    dbProps.setProperty("driver", dirver)
    dbProps.setProperty("user", user)
    dbProps.setProperty("password", passwd)


    val appDictPath: String = config.getString("appdict")

    val redisHost = config.getString("redis.host")
    val redisPort = config.getInt("redis.port")
    val redisdbIndex = config.getInt("redis.index")



    // 解析百度的域名
    val lbsDomain: String = config.getString("baidu.domain")

    // 解析ak sk
    val AKSK: Array[String] = config.getString("baidu.aksk").split(",")


    // 加载scalike配置
    DBs.setup()



}
