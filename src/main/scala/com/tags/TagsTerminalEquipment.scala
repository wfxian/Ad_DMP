package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

/**
  * 4)	设备：
  * a)	(操作系统 -> 1)
  * b)	(联网方 -> 1)
  * c)	(运营商 -> 1)
  *
  * 设备操作系统
  * 1 Android D00010001
  * 2 IOS D00010002
  * 3 WinPhone D00010003
  * _ 其 他 D00010004
  *
  * 设 备 联 网 方 式WIFI D00020001 4G D00020002
  * 3G D00020003
  * 2G D00020004
  * _   D00020005
  *
  * 设备运营商方式
  * 移 动 D00030001 联 通 D00030002 电 信 D00030003
  * _ D00030004
  */
object TagsTerminalEquipment extends Tags{

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()
    //类型转换
    val row: Row = args(0).asInstanceOf[Row]

    //操作系统
    val client: Int = row.getAs[Int]("client")
    //联网方式
    val networkmannername: String = row.getAs[String]("networkmannername")
    //运营商
    val ispname: String = row.getAs[String]("ispname")


    client match {
      case 1 => list :+= ("D00010001",1)
      case 2 => list :+= ("D00010002",1)
      case 3 => list :+= ("D00010003",1)
      case _ => list :+= ("D00010004",1)
    }

    networkmannername match {
      case "Wifi" => list :+= ("D00020001",1)
      case "4G" => list :+= ("D00020002",1)
      case "3G" => list :+= ("D00020003",1)
      case "2G" => list :+= ("D00020004",1)
      case _ => list :+= ("D00020005",1)
    }

    ispname match {
      case "移动" => list :+= ("D00030001",1)
      case "联通" => list :+= ("D00030002",1)
      case "电信" => list :+= ("D00030003",1)
      case _ => list :+= ("D00030004",1)
    }
    list
  }
}
