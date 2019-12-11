package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

/**
  * 广告类型标签
  * 1)	广告位类型（标签格式： LC03->1 或者 LC16->1）xx 为数字，小于 10 补 0，把广告位类型名称，LN 插屏->1
  */
object TagsAD extends Tags {

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //转换类型
    val row = args(0).asInstanceOf[Row]
    //获取广告的类型ID
    val adspacetype = row.getAs[Int]("adspacetype")

    adspacetype match {
      case v if v > 9 => list :+= ("LC"+v, 1)
      case v if v > 0 && v <= 9 => list :+= ("LC0"+v, 1)
    }
    val adspacetypename: String = row.getAs[String]("adspacetypename")
    if (!adspacetypename.isEmpty) {
      list :+= ("LN"+adspacetypename, 1)
    }
    //返回结果
    list
  }
}
