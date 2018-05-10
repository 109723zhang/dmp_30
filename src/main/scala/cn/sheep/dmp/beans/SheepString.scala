package cn.sheep.dmp.beans

/**
  * sheep.Old @ 64341393
  * Created 2018/5/7
  */
class SheepString(val str: String) {

    def toIntPlus = try {
        str.toInt
    } catch {
        case _: Exception => 0
    }


    def toDoublePlus = try {
        str.toDouble
    } catch {
        case _: Exception => 0d
    }

}

object SheepString{
    implicit def str2SheepString(str: String) = new SheepString(str)
}
