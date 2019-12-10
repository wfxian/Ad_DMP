package com.util

/**
  * SparkCore方式实现地域分布统计
  */

object RptUtils {

  //地域
  def region(provincename: String, cityname: String):List[String] = {
    List[String](provincename,cityname)
  }

  //原始请求,有效请求,广告请求
  def request(requestmode: Int,
          processnode: Int):List[Double] = {
    if (requestmode == 1 && processnode == 1) {
      List[Double](1,0,0)
    }else if (requestmode == 1 && processnode == 2) {
      List[Double](1,1,0)
    }else if (requestmode == 1 && processnode == 3) {
      List[Double](1,1,1)
    }else {
      List[Double](0,0,0)
    }
  }

  //参与竞价数,竞价成功数
  def bidding(iseffective: Int,
            isbilling: Int,
            isbid: Int,
            iswin: Int,
            adorderid: Int): List[Double] = {
    if (iseffective == 1 && isbilling == 1 && isbid == 1) {
      if (iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0) {
        List[Double](1,1)
      }else {
        List[Double](1,0)
      }
    }else{
      List[Double](0,0)
    }
  }

  //展示量,点击量
  def number(requestmode: Int,
             iseffective: Int): List[Double] = {
    if (iseffective == 1 && requestmode == 2) {
      List[Double](1, 0)
    } else if (iseffective == 1 && requestmode == 3) {
      List[Double](0, 1)
    } else {
      List[Double](0, 0)
    }
  }

  //广告成本,广告消费
  def ad(iseffective: Int,
         isbilling: Int,
         iswin: Int,
         adpayment: Double,
         winprice: Double): List[Double] = {
    if (iseffective == 1 && isbilling == 1 && iswin == 1){
      List[Double](winprice/1000,adpayment/1000)
    }else {
      List[Double](0,0)
    }
  }




}
