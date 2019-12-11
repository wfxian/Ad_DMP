package com.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import scala.collection.mutable.ListBuffer

/**
  * 从高德地图中获取商圈信息
  */
object AmapUtil {
  def getBusinessFromAMap(long: Double, lat: Double): String = {
    //拼接经纬度
    val location: String = long + "," + lat
    //获取URL
    val urlStr: String = "https://restapi.amap.com/v3/geocode/regeo?location=" + location + "&key=2dfe484ee502e94400b365ca42ee63b0"
    //获取http请求
    val json: String = HttpUtil.get(urlStr)

    //对json进行解析
    val jsonObj: JSONObject = JSON.parseObject(json)

    //判断状态是否成功
    val status: Int = jsonObj.getIntValue("status")
    if (status == 0) return ""

    //如果status != 0 ,则继续运行
    val regeocode: JSONObject = jsonObj.getJSONObject("regeocode")
    //再判断
    if (regeocode == null || regeocode.keySet().isEmpty) return ""

    val addressComponent: JSONObject = regeocode.getJSONObject("addressComponent")
    //再判断
    if (addressComponent == null || addressComponent.keySet().isEmpty) return ""

    val businessAreas: JSONArray = addressComponent.getJSONArray("businessAreas")
    //再判断
    if (businessAreas == null || businessAreas.isEmpty) return ""

    var list: ListBuffer[String] = ListBuffer[String]()
    for (item <- businessAreas.toArray) {
      if (item.isInstanceOf[JSONObject]) {
        val json: JSONObject = item.asInstanceOf[JSONObject]
        val name: String = json.getString("name")
        list.append(name)
      }
    }
    list.mkString(",")
  }
}
