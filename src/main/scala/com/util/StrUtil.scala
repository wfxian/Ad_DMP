package com.util

/**
  * 类型转换工具
  */
object StrUtil {

  //字符串转换成Int
  def toInt(str:String):Int={
    try{
      str.toInt
    }catch {
      case _ : Exception => 0
    }
  }

  //字符串转换成Double
  def toDouble(str:String):Double={
    try{
      str.toDouble
    }catch {
      case _ : Exception => 0.0
    }
  }
}
