package cn.sheep.dmp.etl

import java.io.File

import cn.sheep.dmp.utils.FileHandler
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将原始日志文件转换成parquet文件格式
  * sheep.Old @ 64341393
  * Created 2018/5/7
  */
object Bz2Parquet {

    def main(args: Array[String]): Unit = {

        // 检验参数
        if (args.length != 2) {
            println(
                """
                  |cn.sheep.dmp.etl.Bz2Parquet
                  |参数：dateInputPath, outputPath
                """.stripMargin)
            sys.exit()
        }

        // 模式匹配
        val Array(dataInputPath, outputPath) = args

        val sparkConf = new SparkConf()
          .setAppName("将原始日志文件转换成parquet文件格式")
          .setMaster("local[*]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


        // 创建sparkcontext
        val sc = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sc)
        sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")

        // 读取数据
        val rawData: RDD[String] = sc.textFile(dataInputPath)

        // 处理数据
        val arrRdd: RDD[Array[String]] = rawData
          .map(line => line.split(",", -1))
          // 过滤掉不符合字段要求的数据
          .filter(_.length >= 85)

        // RDD[Array[String]] => RDD[Row]
        import cn.sheep.dmp.beans.SheepString._
        val rowRdd: RDD[Row] = arrRdd.map(arr => {
            Row(
                arr(0), //	sessionid: String,
                arr(1).toIntPlus, //	advertisersid: Int,
                arr(2).toIntPlus, //	adorderid: Int,
                arr(3).toIntPlus, //	adcreativeid: Int,
                arr(4).toIntPlus, //	adplatformproviderid: Int,
                arr(5), //	sdkversion: String,
                arr(6), //	adplatformkey: String,
                arr(7).toIntPlus, //	putinmodeltype: Int,
                arr(8).toIntPlus, //	requestmode: Int,
                arr(9).toDoublePlus, //	adprice: Double,
                arr(10).toDoublePlus, //		adppprice: Double,
                arr(11), //		requestdate: String,
                arr(12), //		ip: String,
                arr(13), //		appid: String,
                arr(14), //		appname: String,
                arr(15), //		uuid: String,
                arr(16), //		device: String,
                arr(17).toIntPlus, //		client: Int,
                arr(18), //		osversion: String,
                arr(19), //		density: String,
                arr(20).toIntPlus, //		pw: Int,
                arr(21).toIntPlus, //		ph: Int,
                arr(22), //		long: String,
                arr(23), //		lat: String,
                arr(24), //		provincename: String,
                arr(25), //		cityname: String,
                arr(26).toIntPlus, //		ispid: Int,
                arr(27), //		ispname: String,
                arr(28).toIntPlus, //		networkmannerid: Int,
                arr(29), //		networkmannername: String,
                arr(30).toIntPlus, //		iseffective: Int,
                arr(31).toIntPlus, //		isbilling: Int,
                arr(32).toIntPlus, //		adspacetype: Int,
                arr(33), //		adspacetypename: String,
                arr(34).toIntPlus, //		devicetype: Int,
                arr(35).toIntPlus, //		processnode: Int,
                arr(36).toIntPlus, //		apptype: Int,
                arr(37), //		district: String,
                arr(38).toIntPlus, //		paymode: Int,
                arr(39).toIntPlus, //		isbid: Int,
                arr(40).toDoublePlus, //		bidprice: Double,
                arr(41).toDoublePlus, //		winprice: Double,
                arr(42).toIntPlus, //		iswin: Int,
                arr(43), //		cur: String,
                arr(44).toDoublePlus, //		rate: Double,
                arr(45).toDoublePlus, //		cnywinprice: Double,
                arr(46), //		imei: String,
                arr(47), //		mac: String,
                arr(48), //		idfa: String,
                arr(49), //		openudid: String,
                arr(50), //		androidid: String,
                arr(51), //		rtbprovince: String,
                arr(52), //		rtbcity: String,
                arr(53), //		rtbdistrict: String,
                arr(54), //		rtbstreet: String,
                arr(55), //		storeurl: String,
                arr(56), //		realip: String,
                arr(57).toIntPlus, //		isqualityapp: Int,
                arr(58).toDoublePlus, //		bidfloor: Double,
                arr(59).toIntPlus, //		aw: Int,
                arr(60).toIntPlus, //		ah: Int,
                arr(61), //		imeimd5: String,
                arr(62), //		macmd5: String,
                arr(63), //		idfamd5: String,
                arr(64), //		openudidmd5: String,
                arr(65), //		androididmd5: String,
                arr(66), //		imeisha1: String,
                arr(67), //		macsha1: String,
                arr(68), //		idfasha1: String,
                arr(69), //		openudidsha1: String,
                arr(70), //		androididsha1: String,
                arr(71), //		uuidunknow: String,
                arr(72), //		userid: String,
                arr(73).toIntPlus, //		iptype: Int,
                arr(74).toDoublePlus, //		initbidprice: Double,
                arr(75).toDoublePlus, //		adpayment: Double,
                arr(76).toDoublePlus, //		agentrate: Double,
                arr(77).toDoublePlus, //		lomarkrate: Double,
                arr(78).toDoublePlus, //		adxrate: Double,
                arr(79), //		title: String,
                arr(80), //		keywords: String,
                arr(81), //		tagid: String,
                arr(82), //		callbackdate: String,
                arr(83), //		channelid: String,
                arr(84).toIntPlus //		mediatype: Int
            )
        })
        /**
          * 通过StuctType构建schema信息
          */
        val schema = StructType(Seq(
            StructField("sessionid", StringType),
            StructField("advertisersid", IntegerType),
            StructField("adorderid", IntegerType),
            StructField("adcreativeid", IntegerType),
            StructField("adplatformproviderid", IntegerType),
            StructField("sdkversion", StringType),
            StructField("adplatformkey", StringType),
            StructField("putinmodeltype", IntegerType),
            StructField("requestmode", IntegerType),
            StructField("adprice", DoubleType),
            StructField("adppprice", DoubleType),
            StructField("requestdate", StringType),
            StructField("ip", StringType),
            StructField("appid", StringType),
            StructField("appname", StringType),
            StructField("uuid", StringType),
            StructField("device", StringType),
            StructField("client", IntegerType),
            StructField("osversion", StringType),
            StructField("density", StringType),
            StructField("pw", IntegerType),
            StructField("ph", IntegerType),
            StructField("long", StringType),
            StructField("lat", StringType),
            StructField("provincename", StringType),
            StructField("cityname", StringType),
            StructField("ispid", IntegerType),
            StructField("ispname", StringType),
            StructField("networkmannerid", IntegerType),
            StructField("networkmannername", StringType),
            StructField("iseffective", IntegerType),
            StructField("isbilling", IntegerType),
            StructField("adspacetype", IntegerType),
            StructField("adspacetypename", StringType),
            StructField("devicetype", IntegerType),
            StructField("processnode", IntegerType),
            StructField("apptype", IntegerType),
            StructField("district", StringType),
            StructField("paymode", IntegerType),
            StructField("isbid", IntegerType),
            StructField("bidprice", DoubleType),
            StructField("winprice", DoubleType),
            StructField("iswin", IntegerType),
            StructField("cur", StringType),
            StructField("rate", DoubleType),
            StructField("cnywinprice", DoubleType),
            StructField("imei", StringType),
            StructField("mac", StringType),
            StructField("idfa", StringType),
            StructField("openudid", StringType),
            StructField("androidid", StringType),
            StructField("rtbprovince", StringType),
            StructField("rtbcity", StringType),
            StructField("rtbdistrict", StringType),
            StructField("rtbstreet", StringType),
            StructField("storeurl", StringType),
            StructField("realip", StringType),
            StructField("isqualityapp", IntegerType),
            StructField("bidfloor", DoubleType),
            StructField("aw", IntegerType),
            StructField("ah", IntegerType),
            StructField("imeimd5", StringType),
            StructField("macmd5", StringType),
            StructField("idfamd5", StringType),
            StructField("openudidmd5", StringType),
            StructField("androididmd5", StringType),
            StructField("imeisha1", StringType),
            StructField("macsha1", StringType),
            StructField("idfasha1", StringType),
            StructField("openudidsha1", StringType),
            StructField("androididsha1", StringType),
            StructField("uuidunknow", StringType),
            StructField("userid", StringType),
            StructField("iptype", IntegerType),
            StructField("initbidprice", DoubleType),
            StructField("adpayment", DoubleType),
            StructField("agentrate", DoubleType),
            StructField("lomarkrate", DoubleType),
            StructField("adxrate", DoubleType),
            StructField("title", StringType),
            StructField("keywords", StringType),
            StructField("tagid", StringType),
            StructField("callbackdate", StringType),
            StructField("channelid", StringType),
            StructField("mediatype", IntegerType)
        ))

        val dataFrame = sQLContext.createDataFrame(rowRdd, schema)

        FileHandler.deleteWillOutputDir(sc, outputPath)

        // 保存数据
        dataFrame.write.parquet(outputPath)
        // 关闭sparkcontext
        sc.stop()

    }

}
