package com.tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisClusters, Tags}
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis


/**
  * 商圈标签
  */
object TagsBusiness extends Tags {

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list: List[(String, Int)] = List[(String,Int)]()
    //转换类型
    val row: Row = args(0).asInstanceOf[Row]
    //获取经纬度
    val long: String = row.getAs[String]("long")
    val lat: String = row.getAs[String]("lat")
    //通过获取到的经纬度获取商圈
    val business: String = getBusiness(long,lat)
    val lines: Array[String] = business.split(",")
    lines.foreach(t=>{
      list :+= (t, 1)
    })
    list
  }



  /**
    * 获取商圈
    *
    * @param long
    * @param lat
    * @return
    */
//  def getBusiness(long: String, lat: String):String = {
//    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble,long.toDouble,6)
//    //去查询数据库
//    var value: String = redis_queryBusiness(geoHash)
//    //查询高德地图
//    if (value == null || value.length == 0) {
//      value = AmapUtil.getBusinessFromAMap(long.toDouble,lat.toDouble)
//      //存储到Redis中
//      redis_insertBusiness(geoHash,value)
//    }
//    value
//  }
def getBusiness(long: String, lat: String):String = {
  try {
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble,long.toDouble,6)
    //去查询数据库
    var value: String = redis_queryBusiness(geoHash)
    //查询高德地图
    if (value == null || value.length == 0) {
      value = AmapUtil.getBusinessFromAMap(long.toDouble, lat.toDouble)
      //存储到Redis中
      redis_insertBusiness(geoHash, value)
    }
    value
  }catch {
    case e: Exception => "error!"
  }
}


  /**
    * 查询redis数据库
    * @param geoHash
    * @return
    */
  def redis_queryBusiness(geoHash: String): String = {
    val jedis: Jedis = JedisClusters.JedisD
    val str: String = jedis.get(geoHash)
    jedis.close()
    str
  }


  /**
    * 将数据存储到Redis
    * @param geoHash
    * @param result
    * @return
    */
  def redis_insertBusiness(geoHash: String, value: String):String = {
    val jedis: Jedis = JedisClusters.JedisD
    jedis.set(geoHash,value)
  }

}
