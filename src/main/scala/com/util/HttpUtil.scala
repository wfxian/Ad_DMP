package com.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * 发送请求GET
  */
object HttpUtil {

  def get(urlStr:String): String ={
    //获取客户端
    val client: CloseableHttpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(urlStr)
    //发送请求
    val response: CloseableHttpResponse = client.execute(httpGet)
    //将结果格式化
    EntityUtils.toString(response.getEntity,"UTF-8")
  }
}
