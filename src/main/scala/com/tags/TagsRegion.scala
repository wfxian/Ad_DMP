package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

/**
  * 6)	地域标签（省标签格式：ZPxxx->1, 地市标签格式: ZCxxx->1）xxx 为省或市名称
  */

object TagsRegion extends Tags{

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    //转换类型
    val row: Row = args(0).asInstanceOf[Row]
    //省份
    val provincename: String = row.getAs[String]("provincename")
    //市
    val cityname: String = row.getAs[String]("cityname")

    list :+= ("ZP"+provincename, 1)
    if (!cityname.isEmpty){
      list :+= ("ZC"+cityname, 1)
    }else{
      list :+= ("ZC未知", 1)
    }
    list
  }
}
