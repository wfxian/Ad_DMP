package com.graphx

import com.tags._
import com.util.TagsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable

/**
  * 运用图计算，将计算后的结果写入Hbase
  * 标签类
  * 上下标签
  * 8)	上下文标签： 读取日志文件，将数据打上上述 6 类标签，并根据用户 ID 进行当前文件的合并，数据保存格式为：userId	K 青云志:3 D00030002:1 ……
  */
object TagsContextV2 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("TagsContextV2").master("local[*]").getOrCreate()

    //hbase配置
    val configuration: Configuration = HBaseConfiguration.create()
    //设置Zookeeper集群
    configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
    //配置客户端连接的端口号
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    val hbConn: Connection = ConnectionFactory.createConnection(configuration)

    val admin: Admin = hbConn.getAdmin
    if (!admin.tableExists(TableName.valueOf("wfx_AD"))) {
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
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "wfx_AD")


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
    val valueRdd: RDD[(List[String], Row)] = df.filter(TagsUtil.oneUserId).rdd.map(row => {
      //取到userId
      val userId: List[String] = TagsUtil.getAllUserId(row)
      //输出
      (userId, row)
    })

    //构建点 （userId.hashcode().toLong,Seq((userId,0),各种标签)）
    val VD: RDD[(Long, immutable.Seq[(Any, Int)])] = valueRdd.flatMap(rdd => {
      val row: Row = rdd._2
      /**
        * 得到标签
        */
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

      //将用户的id保存到标签中
      val vds = rdd._1.map((_, 0)) ++ TagList
      //每一个ID是否都携带一个标签？  否
      rdd._1.map(userid => {
        if (rdd._1.head.equals(userid)) {
          (userid.hashCode().toLong, vds)
        } else {
          (userid.hashCode().toLong, List.empty)
        }
      })
    })

    //构建边
    val ED: RDD[Edge[Int]] = valueRdd.flatMap(rdd => {
      // A B C   A->B  A->C
      rdd._1.map(userid => Edge(rdd._1.head.hashCode().toLong, userid.hashCode().toLong, 0))
    })

    //构建图
    val graph = Graph(VD, ED)

    //取出顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //join标签数据
    vertices.join(VD).map {
      //顶点ID，标签
      case (userId, (vd, tagsUserId)) => (vd, tagsUserId)
    }.reduceByKey {
      case (list1, list2) =>
        //每一条数据相加成一个集合
        (list1 ++ list2)
          //按照集合内的Tuple._1分组
          .groupBy(_._1)
          //聚合每个Tuple的Value值
          .mapValues(_.map(_._2).sum)
          .toList
    }.map {
      case (userId, userTags) => {
        //设置rowkey
        val put = new Put(Bytes.toBytes(userId))
        //添加列的value
        put.addImmutable(Bytes.toBytes("tags"),
          Bytes.toBytes("2019-12-11"),
          Bytes.toBytes(userTags.mkString(",")))
        //设置返回对象和put（rowkey，列簇，列的值）
        (new ImmutableBytesWritable(), put)
      }
      //将数据存入HBase
    }.saveAsHadoopDataset(jobConf)
    spark.close()
  }
}
