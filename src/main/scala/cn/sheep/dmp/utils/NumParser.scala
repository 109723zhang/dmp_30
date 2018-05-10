package cn.sheep.dmp.utils

/**
  * sheep.Old @ 64341393
  * Created 2018/5/7
  */
object NumParser {


    def str2Int(str: String) = {
        try {
            str.toInt
        } catch {
            case _: Exception => 0
        }
    }


    def str2Double(str: String) = {
        try {
            str.toDouble
        } catch {
            case _: Exception => 0d
        }
    }

}
