package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

/**
  * App名称
  * 2)	App 名称（标签格式： APPxxxx->1）xxxx 为 App 名称，
  * 使用缓存文件 appname_dict 进行名称转换；APP 爱奇艺->1
  */
object TagsAppName extends Tags {

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list: List[(String,Int)] = List[(String,Int)]()
    //转换类型
    val broadCast: collection.Map[String, String] = args(0).asInstanceOf[collection.Map[String, String]]
    val row: Row = args(1).asInstanceOf[Row]

    val appid: String = row.getAs[String]("appid")
    var appname: String = row.getAs[String]("appname")
    if (appname.isEmpty) {
      appname = broadCast.getOrElse(appid,"未知")
    }
    list :+= ("APP"+ appname, 1)
    list
  }

}
