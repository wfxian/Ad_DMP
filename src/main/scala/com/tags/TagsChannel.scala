package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

/**
  * 渠道标签
  * 3)	渠道（标签格式： CNxxxx->1）xxxx 为渠道 ID(adplatformproviderid)
  */
object TagsChannel extends Tags{

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()
    //转换类型
    val row: Row = args(0).asInstanceOf[Row]
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
    if (adplatformproviderid.toString != null && !adplatformproviderid.equals(0)) {
      list :+= ("CN"+adplatformproviderid, 1)
    }else {
      list :+= ("CN未知", 1)
    }
    list
  }
}
