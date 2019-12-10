package com.business.MediaAnalysis


import com.util.{JedisClusters, RptUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.{Jedis, JedisCluster}

/**
  * 媒体分析
  */
object MediaAnalysis {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("MediaAnalysis").master("local").getOrCreate()

    val df: DataFrame = spark.read.parquet("E:\\BigData\\teacher\\课件\\第四阶段_项目\\用户画像\\Spark用户画像分析\\write\\1\\part-00000-933f8c86-8e1e-45e0-b9ff-6851722a8de7-c000.snappy.parquet")
    val lines: RDD[String] = spark.sparkContext.textFile("E:\\BigData\\teacher\\课件\\第四阶段_项目\\用户画像\\Spark用户画像分析\\app_dict.txt")

    //    //过滤+切分
    //    val map: collection.Map[String, String] = lines.filter(_.split("\t", -1).length >= 5)
    //      .map(_.split("\t", -1))
    //      .map(rdd =>(rdd(4), rdd(1)))
    //      .collectAsMap()
    //    //广播
    //    val broadCast: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(map)
    //
    //    val valueRdd1: RDD[(String, List[Double])] = df.rdd.map(rdd => {
    //      //应用ID
    //      val appid: String = rdd.getAs[String]("appid")
    //      //应用名称
    //      var appname: String = rdd.getAs[String]("appname")
    //      //判断当前appname是否为空
    //      if (appname.isEmpty) {
    //        appname = broadCast.value.getOrElse(appid, "其他")
    //      }
    //      //总请求,有效请求,广告请求
    //      val requestmode: Int = rdd.getAs[Int]("requestmode")
    //      val processnode: Int = rdd.getAs[Int]("processnode")
    //      //参与竞价数,竞价成功数,展示量,点击量
    //      val iseffective: Int = rdd.getAs[Int]("iseffective")
    //      val isbilling: Int = rdd.getAs[Int]("isbilling")
    //      val isbid: Int = rdd.getAs[Int]("isbid")
    //      val iswin: Int = rdd.getAs[Int]("iswin")
    //      val adorderid: Int = rdd.getAs[Int]("adorderid")
    //      //广告成本,广告消费
    //      val adpayment: Double = rdd.getAs[Double]("adpayment")
    //      val winprice: Double = rdd.getAs[Double]("winprice")
    //
    //      /**
    //        * 业务代码
    //        */
    //      //总请求,有效请求,广告请求
    //      val requestList: List[Double] = RptUtils.request(requestmode, processnode)
    //      //参与竞价数,竞价成功数
    //      val biddingList: List[Double] = RptUtils.bidding(iseffective, isbilling, isbid, iswin, adorderid)
    //      //展示量,点击量
    //      val numberList: List[Double] = RptUtils.number(requestmode, iseffective)
    //      //广告成本,广告消费
    //      val adList: List[Double] = RptUtils.ad(iseffective, isbilling, iswin, adpayment, winprice)
    //
    //      (appname, requestList ++ biddingList ++ numberList ++ adList)
    //    })
    //
    //    valueRdd1.reduceByKey((list1, list2) => {
    //      list1.zip(list2).map(x => x._1 + x._2)
    //    }).map(t => {
    //      t._1 + "," + t._2.mkString(",")
    //    }).foreach(println)


    /**
      * 使用redis集群
      * 媒体指标分析：
      *
      * 首先将数据切分后，存入Redis数据库，K->appid，V->appName
      *
      * 然后读取redis内的字段文件，实现媒体指标统计
      *
      * 注意：一定要保存性能，（不能一条数据创建一次）
      */

    //像Redis中写入字典文件数据
    lines.filter(_.split("\t",-1).length >= 5).map(_.split("\t",-1)).map(x=>(x(4),x(1)))
      .foreachPartition(rdd=>{
        val jedis: Jedis = JedisClusters.JedisD
        rdd.foreach(x=>{
          jedis.set(x._1,x._2)
        })
      })


    val valueRdd2: RDD[(String, List[Double])] = df.rdd.map(rdd => {
      //应用ID
      val appid: String = rdd.getAs[String]("appid")
      //应用名称
      var appname: String = rdd.getAs[String]("appname")
      //判断当前appname是否为空
      if (appname.isEmpty) {
        appname = JedisClusters.JedisD.get(appid)
      }
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

      (appname, requestList ++ biddingList ++ numberList ++ adList)
    })

    valueRdd2.reduceByKey((list1, list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    }).map(t => {
      t._1 + "," + t._2.mkString(",")
    }).foreach(println)


  }

}
