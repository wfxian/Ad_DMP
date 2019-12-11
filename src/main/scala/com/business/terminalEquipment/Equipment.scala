package com.business.terminalEquipment

import com.util.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 终端设备
  * 设备类
  */
object Equipment {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Equipment").master("local").getOrCreate()

    val df: DataFrame = spark.read.parquet("E:\\BigData\\teacher\\课件\\第四阶段_项目\\用户画像\\Spark用户画像分析\\write\\1\\part-00000-933f8c86-8e1e-45e0-b9ff-6851722a8de7-c000.snappy.parquet")

    val valueRdd: RDD[(String, List[Double])] = df.rdd.map(rdd => {
      //设备类型
      val devicetype: Int = rdd.getAs[Int]("devicetype")
      //总请求,有效请求,广告请求
      val requestmode: Int = rdd.getAs[Int]("requestmode")
      val processnode: Int = rdd.getAs[Int]("processnode")
      //参与竞价数,竞价成功数,展示量,点击量
      val iseffective: Int = rdd.getAs[Int]("iseffective")
      val isbilling: Int = rdd.getAs[Int]("isbilling")
      val isbid: Int = rdd.getAs[Int]("isbid")
      val iswin: Int = rdd.getAs[Int]("iswin")
      val adorderid: Int = rdd.getAs[Int]("adorderid")
      //广告成本,广告消费
      val adpayment: Double = rdd.getAs[Double]("adpayment")
      val winprice: Double = rdd.getAs[Double]("winprice")

      /**
        * 业务代码
        */
      //总请求,有效请求,广告请求
      val requestList: List[Double] = RptUtils.request(requestmode, processnode)
      //参与竞价数,竞价成功数
      val biddingList: List[Double] = RptUtils.bidding(iseffective, isbilling, isbid, iswin, adorderid)
      //展示量,点击量
      val numberList: List[Double] = RptUtils.number(requestmode, iseffective)
      //广告成本,广告消费
      val adList: List[Double] = RptUtils.ad(iseffective, isbilling, iswin, adpayment, winprice)

      var devType = ""
      if (devicetype == 1) {
        devType = "手机"
      }else if (devicetype == 2){
        devType = "平板"
      }else {
        devType = "其他"
      }
      (devType, requestList ++ biddingList ++ numberList ++ adList)
    })

    valueRdd.reduceByKey((list1,list2)=>{
      list1.zip(list2).map(x=>x._1+x._2)
    }).map(t=>{
      t._1 +","+ t._2.mkString(",")
    }).foreach(println)
  }
}
