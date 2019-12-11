package com.tags

import com.typesafe.config.{Config, ConfigFactory}
import com.util.TagsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 标签类
  * 上下标签
  * 8)	上下文标签： 读取日志文件，将数据打上上述 6 类标签，并根据用户 ID 进行当前文件的合并，数据保存格式为：userId	K 青云志:3 D00030002:1 ……
  */
object TagsContext {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("TagsContext").master("local[*]").getOrCreate()
    import spark.implicits._

    //hbase配置
    val configuration: Configuration = HBaseConfiguration.create()
    //设置Zookeeper集群
    configuration.set("hbase.zookeeper.quorum","hadoop01:2181,hadoop02:2181,hadoop03:2181")
    //配置客户端连接的端口号
    configuration.set("hbase.zookeeper.property.clientPort","2181")
    val hbConn: Connection = ConnectionFactory.createConnection(configuration)

    val admin: Admin = hbConn.getAdmin
    if (!admin.tableExists(TableName.valueOf("wfx_AD"))){
      println("当前表可用!!")
      //创建对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf("wfx_AD"))
      //创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      //将列簇加入到表中
      tableDescriptor.addFamily(columnDescriptor)
      //创建表
      admin.createTable(tableDescriptor)
      admin.close()
      hbConn.close()
    }

    //创建Job
    val jobConf = new JobConf(configuration)
    //指定key的类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"wfx_AD")


    //获取数据
    val df: DataFrame = spark.read.parquet("E:\\BigData\\teacher\\课件\\第四阶段_项目\\用户画像\\Spark用户画像分析\\write\\1\\part-00000-933f8c86-8e1e-45e0-b9ff-6851722a8de7-c000.snappy.parquet")
    //获取appname_dict的数据，并进行广播
    val lines: RDD[String] = spark.sparkContext.textFile("E:\\BigData\\teacher\\课件\\第四阶段_项目\\用户画像\\Spark用户画像分析\\app_dict.txt")

    //获取stopwords.txt，并进行广播
    val lines1: RDD[String] = spark.sparkContext.textFile("E:\\BigData\\teacher\\课件\\第四阶段_项目\\用户画像\\Spark用户画像分析\\stopwords.txt")
    val coll: Array[String] = lines1.collect()

    //过滤+切分
    val collMap: collection.Map[String, String] = lines.filter(_.split("\t", -1).length >= 5)
      .map(_.split("\t", -1))
      .map(rdd => (rdd(4), rdd(1)))
      .collectAsMap()
    //广播
    val broadCast: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(collMap)
    val broadCast1: Broadcast[Array[String]] = spark.sparkContext.broadcast(coll)

    //打标签
    df.filter(TagsUtil.oneUserId).rdd.map(row => {
      //取到userId
      val userId: String = TagsUtil.getOneUserId(row)
      //广告标签
      val adTag: List[(String, Int)] = TagsAD.makeTags(row)
      //App名称
      val appTag: List[(String, Int)] = TagsAppName.makeTags(broadCast.value, row)
      //渠道
      val channelTag: List[(String, Int)] = TagsChannel.makeTags(row)
      //设备
      val terminalEquipmentTag: List[(String, Int)] = TagsTerminalEquipment.makeTags(row)
      //关键字
      val keyWordsTag: List[(String, Int)] = TagsKeyWord.makeTags(broadCast1.value, row)
      //地域标签
      val regionTag: List[(String, Int)] = TagsRegion.makeTags(row)
      //商圈标签
      val businessTag: List[(String, Int)] = TagsBusiness.makeTags(row)

      val TagList: List[(String, Int)] = adTag ++ appTag ++ channelTag ++ terminalEquipmentTag ++ keyWordsTag ++ regionTag ++ businessTag

      //输出
      (userId, TagList)
    })
      .reduceByKey {
        case (list1, list2) => {
          (list1 ++ list2).groupBy(_._1).map {
            case (k, list) => {
              (k, list.map(t => t._2).sum)
            }
          }.toList
        }
      }
//      .foreach(x => {
//        println(x._1 + "->" + x._2.mkString(","))
//      })
        .map{
      case (userId,userTags) =>{
        //设置rowkey
        val put = new Put(Bytes.toBytes(userId))
        //添加列的value
        put.addImmutable(Bytes.toBytes("tags"),
        Bytes.toBytes("2019-12-11"),
        Bytes.toBytes(userTags.mkString(",")))
        //设置返回对象和put（rowkey，列簇，列的值）
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobConf)


    spark.close()
  }
}
