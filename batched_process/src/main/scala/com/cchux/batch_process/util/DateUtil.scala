package com.cchux.batch_process.util

import org.apache.commons.lang3.time.FastDateFormat


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

  def formatDateTime(date:String, format:String) = {
    val timestampFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val timestamp = timestampFormat.parse(date).getTime
    val formatDate = FastDateFormat.getInstance(format)
    formatDate.format(timestamp)
  }


}
