package cn.sheep.dmp.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * sheep.Old @ 64341393
  * Created 2018/5/11
  */
object TagsHandler {

    // 至少要有一个id不能为空
    val hasNeedOneUserId =
        """
          |imei!='' or idfa!='' or mac!='' or androidid != '' or openudid!='' or
          |imeimd5!='' or idfamd5!='' or macmd5!='' or androididmd5 != '' or openudidmd5!='' or
          |imeisha1!='' or idfasha1!='' or macsha1!='' or androididsha1 != '' or openudidsha1!=''
        """.stripMargin

    /**
      * 从一行数据中获取用户的某一个不为空的id
      *
      * @param row
      */
    def getAnyOneUserId(row: Row) = {

        row match {
            case v if StringUtils.isNotEmpty(v.getAs[String]("imei")) => "IM:" + v.getAs[String]("imei")
            case v if StringUtils.isNotEmpty(v.getAs[String]("idfa")) => "ID:" + v.getAs[String]("idfa")
            case v if StringUtils.isNotEmpty(v.getAs[String]("mac")) => "MC:" + v.getAs[String]("mac")
            case v if StringUtils.isNotEmpty(v.getAs[String]("androidid")) => "AD:" + v.getAs[String]("androidid")
            case v if StringUtils.isNotEmpty(v.getAs[String]("openudid")) => "OU:" + v.getAs[String]("openudid")

            case v if StringUtils.isNotEmpty(v.getAs[String]("imeimd5")) => "IMM:" + v.getAs[String]("imeimd5")
            case v if StringUtils.isNotEmpty(v.getAs[String]("idfamd5")) => "IDM:" + v.getAs[String]("idfamd5")
            case v if StringUtils.isNotEmpty(v.getAs[String]("macmd5")) => "MCM:" + v.getAs[String]("macmd5")
            case v if StringUtils.isNotEmpty(v.getAs[String]("androididmd5")) => "ADM:" + v.getAs[String]("androididmd5")
            case v if StringUtils.isNotEmpty(v.getAs[String]("openudidmd5")) => "OUM:" + v.getAs[String]("openudidmd5")

            case v if StringUtils.isNotEmpty(v.getAs[String]("imeisha1")) => "IMS:" + v.getAs[String]("imeisha1")
            case v if StringUtils.isNotEmpty(v.getAs[String]("idfasha1")) => "IDS:" + v.getAs[String]("idfasha1")
            case v if StringUtils.isNotEmpty(v.getAs[String]("macsha1")) => "MCS:" + v.getAs[String]("macsha1")
            case v if StringUtils.isNotEmpty(v.getAs[String]("androididsha1")) => "ADS:" + v.getAs[String]("androididsha1")
            case v if StringUtils.isNotEmpty(v.getAs[String]("openudidsha1")) => "OUS:" + v.getAs[String]("openudidsha1")
        }


    }


    /**
      * 就是要获取当前行所有用户的标识ID
      *
      * @param v
      */
    def getCurrentRowAllUserId(v: Row) = {

        var list = List[String]()

        if (StringUtils.isNotEmpty(v.getAs[String]("imei"))) list :+= "IM:" + v.getAs[String]("imei")
        if (StringUtils.isNotEmpty(v.getAs[String]("idfa"))) list :+= "ID:" + v.getAs[String]("idfa")
        if (StringUtils.isNotEmpty(v.getAs[String]("mac"))) list :+= "MC:" + v.getAs[String]("mac")
        if (StringUtils.isNotEmpty(v.getAs[String]("androidid"))) list :+= "AD:" + v.getAs[String]("androidid")
        if (StringUtils.isNotEmpty(v.getAs[String]("openudid"))) list :+= "OU:" + v.getAs[String]("openudid")
        if (StringUtils.isNotEmpty(v.getAs[String]("imeimd5"))) list :+= "IMM:" + v.getAs[String]("imeimd5")
        if (StringUtils.isNotEmpty(v.getAs[String]("idfamd5"))) list :+= "IDM:" + v.getAs[String]("idfamd5")
        if (StringUtils.isNotEmpty(v.getAs[String]("macmd5"))) list :+= "MCM:" + v.getAs[String]("macmd5")
        if (StringUtils.isNotEmpty(v.getAs[String]("androididmd5"))) list :+= "ADM:" + v.getAs[String]("androididmd5")
        if (StringUtils.isNotEmpty(v.getAs[String]("openudidmd5"))) list :+= "OUM:" + v.getAs[String]("openudidmd5")
        if (StringUtils.isNotEmpty(v.getAs[String]("imeisha1"))) list :+= "IMS:" + v.getAs[String]("imeisha1")
        if (StringUtils.isNotEmpty(v.getAs[String]("idfasha1"))) list :+= "IDS:" + v.getAs[String]("idfasha1")
        if (StringUtils.isNotEmpty(v.getAs[String]("macsha1"))) list :+= "MCS:" + v.getAs[String]("macsha1")
        if (StringUtils.isNotEmpty(v.getAs[String]("androididsha1"))) list :+= "ADS:" + v.getAs[String]("androididsha1")
        if (StringUtils.isNotEmpty(v.getAs[String]("openudidsha1"))) list :+= "OUS:" + v.getAs[String]("openudidsha1")

        list
    }

}
