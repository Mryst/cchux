package com.cchux.task

//import com.cchux.bean.ClickLogWide
import com.cchux.bean.ClickLogWide
import org.apache.flink.streaming.api.scala.DataStream

/**
 * 定义业务指标分析的接口
 */
trait ProcessData {

  //根据预处理的数据进行业务的分析
  def process(preDataStream: DataStream[ClickLogWide])

}
