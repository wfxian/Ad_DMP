package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

/**
  * 5)	关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能
  * 超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
  */
object TagsKeyWord extends Tags{

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String, Int)]()

    //类型转换
    val row: Row = args(1).asInstanceOf[Row]
    val broadCastValue: Array[String] = args(0).asInstanceOf[Array[String]]

    val keywords: String = row.getAs[String]("keywords")

//    var keyArr = Array[String]()
//    for (i <- 0 until broadCastValue.length){
//      if (!keywords.contains(i) && keywords.split("\\|").length >= 3 && keywords.split("\\|").length <= 8) {
//        keyArr = keywords.split("\\|")
//        keyArr
//      }
//    }
//    for (item <- keyArr) {
//      list :+= ("K" + item, 1)
//    }

    val kw: Array[String] = keywords.split("\\|")
    kw.filter(str=>{
      str.length >= 3 && str.length <= 8 && !broadCastValue.contains(str)
    }).foreach(x=> list :+= ("K" + x, 1))

    list
  }
}
