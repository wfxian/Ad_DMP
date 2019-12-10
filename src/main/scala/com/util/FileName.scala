package com.util

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  * 自定义Spark写出文件的文件名
  */

class FileName extends MultipleTextOutputFormat[Any, Any] {

  private val start_time = System.currentTimeMillis()
  //重写generateFileNameForKeyValue方法，该方法是负责自定义生成文件的文件名
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    //这里的key和value指的就是要写入文件的rdd对，再此，我定义文件名以key.txt来命名，当然也可以根据其他的需求来进行生成文件名
    val fileName = key +"-"+name
    fileName
  }

}
