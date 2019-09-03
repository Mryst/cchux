package com.cchux.util

import org.apache.commons.lang.time.FastDateFormat


/**
 * 时间处理的工具类
 */
object DateUtil {

  /**
   * 传递时间戳返回日期字符串
   * @param timestamp
   */
  def timestamp2Date(timestamp:Long, format:String) ={
    val dateFormat = FastDateFormat.getInstance(format)
    dateFormat.format(timestamp)
  }
}
