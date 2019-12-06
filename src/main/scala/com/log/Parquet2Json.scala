package com.log

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 将统计的结果输出成 json 格式
  */
object Parquet2Json {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Parquet2Json").master("local").getOrCreate()

    val df: DataFrame = spark.read.parquet("E:\\BigData\\teacher\\课件\\第四阶段_项目\\用户画像\\Spark用户画像分析\\write\\1\\part-00000-933f8c86-8e1e-45e0-b9ff-6851722a8de7-c000.snappy.parquet")

    import org.apache.spark.sql.functions._

    val frame: DataFrame = df.groupBy(df.col("provincename") as "provincename" ,df.col("cityname") as "cityname").agg(count("cityname")as "ct").select("ct","provincename", "cityname")
    frame.show()
    //写入本地json
//    frame.write.json("E:\\BigData\\teacher\\课件\\第四阶段_项目\\用户画像\\Spark用户画像分析\\write\\1\\2.json")

    //写入Mysql
    //定义连接参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/Spark"
    val userName = "root"
    val passWd = "123456"

    val value: RDD[(Any, Any, Any)] = frame.rdd.map(x => {
      (x(0), x(1), x(2))
    })

    value.foreachPartition(f=>{
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      val sql = connection.prepareStatement("insert into ad (ct, provincename,cityname) values (?,?,?)")

      f.foreach(t => {
        sql.setString(1, t._1.toString)
        sql.setString(2, t._2.toString)
        sql.setString(3, t._3.toString)

        val i = sql.executeUpdate()
        if (i > 0) println("写入成功") else println("写入失败")
      })
      sql.close()
      connection.close()
    })





  }


}
