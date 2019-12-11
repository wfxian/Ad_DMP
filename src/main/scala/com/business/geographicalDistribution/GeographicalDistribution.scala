package com.business.geographicalDistribution

import com.util.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 地域分布
  */
object GeographicalDistribution {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("GeographicalDistribution").master("local").getOrCreate()

    val df: DataFrame = spark.read.parquet("E:\\BigData\\teacher\\课件\\第四阶段_项目\\用户画像\\Spark用户画像分析\\write\\1\\part-00000-933f8c86-8e1e-45e0-b9ff-6851722a8de7-c000.snappy.parquet")

    df.createTempView("GeographicalDistribution")

    val yyy = spark.sql(
      """select
        provincename,
        cityname,
        sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as original_request,
        sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as valid_request,
        sum(case when requestmode=1 and processnode=3 then 1 else 0 end) as advertisement_request,
        sum(case when iseffective='1' and isbilling='1' and isbid='1' then 1 else 0 end) as participating_bidding_num,
        sum(case when iseffective='1' and isbilling='1' and iswin='1' and adorderid!=0 then 1 else 0 end) as successful_bidding_num,
        sum(case when requestmode=2 and iseffective='1' then 1 else 0 end) as show_num,
        sum(case when requestmode=3 and iseffective='1' then 1 else 0 end) as click_num,
        sum(case when iseffective='1' and isbilling='1' and iswin='1' then adpayment/1000 else 0 end) as dsp_advertising_cost,
        sum(case when iseffective='1' and isbilling='1' and iswin='1' then winprice/1000 else 0 end) as dsp_advertising_consumption
        from
        GeographicalDistribution
        group by
        provincename,cityname""")

    yyy.show()

    /**
      * 省市/城市	 原始请求	有效请求	广告请求	参与竞价数	竞价成功数	竞价成功率	展示量	点击量	点击率	广告成本	广告消费
      */
    val resultRdd: RDD[(String, List[Double])] = df.rdd.map(rdd => {
      //省
      val provincename: String = rdd.getAs[String]("provincename")
      //市
      val cityname: String = rdd.getAs[String]("cityname")
      //原始请求,有效请求,广告请求
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
        * 业务方法实现
        */
      //地域
      val regionList: List[String] = RptUtils.region(provincename, cityname)
      //原始请求,有效请求,广告请求
      val requestList: List[Double] = RptUtils.request(requestmode, processnode)
      //参与竞价数,竞价成功数
      val biddingList: List[Double] = RptUtils.bidding(iseffective, isbilling, isbid, iswin, adorderid)
      //展示量,点击量
      val numberList: List[Double] = RptUtils.number(requestmode, iseffective)
      //广告成本,广告消费
      val adList: List[Double] = RptUtils.ad(iseffective, isbilling, iswin, adpayment, winprice)

      (regionList.mkString(","), requestList ++ biddingList ++ numberList ++ adList)
    })

    resultRdd.reduceByKey((list1, list2) => {
      //list1(1,2,3,4)  list2(1,2,3,4)  zip(List(1,1),(2,2),(3,3,),(4,4))
      list1.zip(list2)
        //List((1+1),(2+2),(3+3),(4+4))
        .map(x=>x._1+x._2)
      //List(2,4,6,8)
    }).map(t=>{
      t._1+","+t._2.mkString(",")
    }).foreach(println)



  }
}
