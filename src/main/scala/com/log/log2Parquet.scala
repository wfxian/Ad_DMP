package com.log

import com.util.{SchemaType, StrUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 将数据格式转换为Parquet格式
  */
object log2Parquet {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("parquet")
      .master("local")
      //设置序列化方式
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //判断路径
    if (args.length != 2) {
      println("目录不正确，退出")
      sys.exit()
    }

    //获取数据
    val Array(inputPath,outputPath) = args
    val lines = spark.sparkContext.textFile(inputPath)
    //先切分数据，按照逗号
    val rowRDD: RDD[Row] = lines.map(x => x.split(",", -1)).filter(_.length >= 85)
      .map(arr => {
        Row(
          //会话标识
          arr(0),
          //广告主 id
          StrUtil.toInt(arr(1)),
          //广告 id
          StrUtil.toInt(arr(2)),
          //广告创意 id	( >= 200000 : dsp)
          StrUtil.toInt(arr(3)),
          //广告平台商 id	(>= 100000: rtb)
          StrUtil.toInt(arr(4)),
          //sdk 版本号
          arr(5),
          //平台商 key
          arr(6),
          //针对广告主的投放模式,1：展示量投放 2：点击量投放
          StrUtil.toInt(arr(7)),
          //数据请求方式（1:请求、2:展示、3:点击）
          StrUtil.toInt(arr(8)),
          //广告价格
          StrUtil.toDouble(arr(9)),
          //平台商价格
          StrUtil.toDouble(arr(10)),
          //请求时间,格式为：yyyy-m-dd hh:mm:ss
          arr(11),
          //设备用户的真实 ip 地址
          arr(12),
          //应用 id
          arr(13),
          //应用名称
          arr(14),
          //设备唯一标识
          arr(15),
          //设备型号，如 htc、iphone
          arr(16),
          //设备类型 （1：android 2：ios 3：wp）
          StrUtil.toInt(arr(17)),
          //设备操作系统版本
          arr(18),
          //设备屏幕的密度
          arr(19),
          //设备屏幕宽度
          StrUtil.toInt(arr(20)),
          //设备屏幕高度
          StrUtil.toInt(arr(21)),
          //设备所在经度
          arr(22),
          //设备所在纬度
          arr(23),
          //设备所在省份名称
          arr(24),
          //设备所在城市名称
          arr(25),
          //运营商 id
          StrUtil.toInt(arr(26)),
          //运营商名称
          arr(27),
          //联网方式 id
          StrUtil.toInt(arr(28)),
          //联网方式名称
          arr(29),
          //有效标识（有效指可以正常计费的）(0：无效 1：有效
          StrUtil.toInt(arr(30)),
          //是否收费（0：未收费 1：已收费）
          StrUtil.toInt(arr(31)),
          //广告位类型（1：banner 2：插屏 3：全屏）
          StrUtil.toInt(arr(32)),
          //广告位类型名称（banner、插屏、全屏）
          arr(33),
          //设备类型（1：手机 2：平板）
          StrUtil.toInt(arr(34)),
          //流程节点（1：请求量 kpi 2：有效请求 3：广告请求）
          StrUtil.toInt(arr(35)),
          //应用类型 id
          StrUtil.toInt(arr(36)),
          //设备所在县名称
          arr(37),
          //针对平台商的支付模式，1：展示量投放(CPM) 2：点击
          StrUtil.toInt(arr(38)),
          //是否 rtb
          StrUtil.toInt(arr(39)),
          //rtb 竞价价格
          StrUtil.toDouble(arr(40)),
          //rtb 竞价成功价格
          StrUtil.toDouble(arr(41)),
          //是否竞价成功
          StrUtil.toInt(arr(42)),
          //values:usd|rmb 等
          arr(43),
          //汇率
          StrUtil.toDouble(arr(44)),
          //rtb 竞价成功转换成人民币的价格
          StrUtil.toDouble(arr(45)),
          //Imei（移动设备识别码）
          arr(46),
          //Mac（苹果设备）
          arr(47),
          //Idfa（广告标识符）
          arr(48),
          //Openudid（UDID的第三方解决方案）
          arr(49),
          //Androidid（安卓设备的唯一id）
          arr(50),
          //rtb 省
          arr(51),
          //rtb 市
          arr(52),
          //rtb 区
          arr(53),
          //rtb 街 道
          arr(54),
          //app 的市场下载地址
          arr(55),
          //真实 ip
          arr(56),
          //优选标识
          StrUtil.toInt(arr(57)),
          //底价
          StrUtil.toDouble(arr(58)),
          //广告位的宽
          StrUtil.toInt(arr(59)),
          //广告位的高
          StrUtil.toInt(arr(60)),
          //imei_md5
          arr(61),
          //mac_md5
          arr(62),
          //idfa_md5
          arr(63),
          //openudid_md5
          arr(64),
          //androidid_md5
          arr(65),
          //imei_sha1
          arr(66),
          //mac_sha1
          arr(67),
          //idfa_sha1
          arr(68),
          //openudid_sha1
          arr(69),
          //androidid_sha1
          arr(70),
          //uuid_unknow tanx 密文
          arr(71),
          //平台用户 id
          arr(72),
          //表示 ip 类型
          StrUtil.toInt(arr(73)),
          //初始出价
          StrUtil.toDouble(arr(74)),
          //转换后的广告消费
          StrUtil.toDouble(arr(75)),
          //代理商利润率
          StrUtil.toDouble(arr(76)),
          //代理利润率
          StrUtil.toDouble(arr(77)),
          //媒介利润率
          StrUtil.toDouble(arr(78)),
          //标题
          arr(79),
          //关键字
          arr(80),
          //广告位标识(当视频流量时值为视频 ID 号)
          arr(81),
          //回调时间 格式为:YYYY/mm/dd hh:mm:ss
          arr(82),
          //频道 ID
          arr(83),
          // 媒体类型：1 长尾媒体 2 视频媒体 3 独立媒体	默认:1
          StrUtil.toInt(arr(84))
        )
      })
    //构建DF
    val df: DataFrame = spark.createDataFrame(rowRDD,SchemaType.structType)
    //保存数据结果
    df.write.parquet(outputPath)
    //关闭
    spark.stop()
  }
}
