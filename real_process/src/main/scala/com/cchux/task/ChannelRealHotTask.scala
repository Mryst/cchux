package com.cchux.task

import com.cchux.bean.ClickLogWide
import com.cchux.util.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 完成实时频道 热点 分析业务开发
 */
object ChannelRealHotTask extends ProcessData {

  /**
   * 1. 创建实时热点样例类，专门用来计算实时热点的数据
   * @param channelId
   * @param visited
   */
  case class ChannelRealHot(channelId:String, visited:Long)

  override def process(preDataStream: DataStream[ClickLogWide]): Unit = {
    /**
     * 实现思路：
     * 1. 创建实时热点样例类，专门用来计算实时热点的数据
     * 2. 将预处理后的数据， 转换 为要分析出来的数据（频道、访问次数）样例类
     * 3. 按照 频道 进行分组（分流）
     * 4. 划分时间窗口（3秒一个窗口）
     * 5. 进行合并计数统计
     * 6. 打印测试
     * 7. 将计算后的数据下沉到Hbase
     */
    //2. 将预处理后的数据， 转换 为要分析出来的数据（频道、访问次数）样例类
    val channelRealHotDataStream: DataStream[ChannelRealHot] = preDataStream.map(log => {
      ChannelRealHot(log.channelID, log.count.toLong)
    })

    //3. 按照 频道 进行分组（分流）
    val groupedDataStream: KeyedStream[ChannelRealHot, String] = channelRealHotDataStream.keyBy(_.channelId)

    //4. 划分时间窗口（3秒一个窗口）
    val windowDataStream: WindowedStream[ChannelRealHot, String, TimeWindow] = groupedDataStream.timeWindow(Time.seconds(3))

    //5. 进行合并计数统计
    val sumDataStream: DataStream[ChannelRealHot] = windowDataStream.reduce((hot1, hot2) =>
      ChannelRealHot(hot1.channelId, hot1.visited + hot2.visited)
    )

    //7. 将计算后的数据下沉到Hbase
    sumDataStream.addSink(realHot=>{
      /**
       * 构建hbase的参数
       */
      val tableName = "channel_realHot"
      val cfName = "info"
      val rowKey = realHot.channelId
      val channelIdColName = "channelId"
      val visitedColName = "visited"

      //获取历史的数据+当前数据=写入数据
      val visitedInHbase: String = HBaseUtil.getData(tableName, rowKey, cfName, visitedColName)

      //总的访问量
      var totalVisited = 0L

      //判断是否有历史的数据
      if(StringUtils.isNotBlank(visitedInHbase)){
        totalVisited = visitedInHbase.toLong+realHot.visited

        //将访问量写入到数据库钟
        HBaseUtil.putData(tableName, rowKey, cfName, visitedColName, totalVisited)
      }else{
        totalVisited = realHot.visited

        //将数据写入到数据库
        HBaseUtil.putMapData(tableName, rowKey, cfName, Map(
          channelIdColName-> realHot.channelId,
          visitedColName -> totalVisited
        ))
      }
    })
  }
}
