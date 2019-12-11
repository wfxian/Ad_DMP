package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 标签工具类
  */
object TagsUtil {

  //获取所有不为空的ID
  val oneUserId =
    """
      |imei != '' or mac != '' or idfa != '' or openudid != '' or androidid != ''
      |""".stripMargin

  //获取唯一的不为空的用户Id
  def getOneUserId(row:Row):String={
    row match {
      case v if StringUtils.isNoneEmpty(row.getAs("imei")) => "IM:" + v.getAs("imei")
      case v if StringUtils.isNoneEmpty(row.getAs("mac")) => "MC:" + v.getAs("mac")
      case v if StringUtils.isNoneEmpty(row.getAs("idfa")) => "ID:" + v.getAs("idfa")
      case v if StringUtils.isNoneEmpty(row.getAs("openudid")) => "OP:" + v.getAs("openudid")
      case v if StringUtils.isNoneEmpty(row.getAs("androidid")) => "AD:" + v.getAs("androidid")
      case _ => "未知"
    }
  }


  //因业务需求，需要获取所有的用户
  def getAllUserId(row:Row) ={
    var list: List[String] = List[String]()
    if (!row.getAs[String]("imei").isEmpty) list :+= "IM" + row.getAs[String]("imei")
    if (!row.getAs[String]("mac").isEmpty) list :+= "MC" + row.getAs[String]("mac")
    if (!row.getAs[String]("idfa").isEmpty) list :+= "ID" + row.getAs[String]("idfa")
    if (!row.getAs[String]("openudid").isEmpty) list :+= "OP" + row.getAs[String]("openudid")
    if (!row.getAs[String]("androidid").isEmpty) list :+= "AD" + row.getAs[String]("androidid")
    list
  }
}
