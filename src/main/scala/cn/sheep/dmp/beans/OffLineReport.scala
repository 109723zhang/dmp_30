package cn.sheep.dmp.beans

/**
  * sheep.Old @ 64341393
  * Created 2018/5/9
  */
case class ReportLogDataAnalysis(provinceName: String, cityName: String, ct: Int)

case class ReportAreaAnalysis(provinceName: String,
                              cityName: String,
                              rawReq: Double,
                              effReq: Double,
                              adReq: Double,
                              rtbReq: Double,
                              winReq: Double,
                              adShow: Double,
                              adClick: Double,
                              adCost: Double,
                              adPayment: Double
                             )


case class ReportMediaAnalysis(appName: String,
                              rawReq: Double,
                              effReq: Double,
                              adReq: Double,
                              rtbReq: Double,
                              winReq: Double,
                              adShow: Double,
                              adClick: Double,
                              adCost: Double,
                              adPayment: Double
                             )

