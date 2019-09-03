package com.cchux.task

import com.cchux.bean.ClickLogWide
import com.cchux.util.HBaseUtil
import com.cchux.bean.ClickLogWide
import com.cchux.util.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 完成实时频道 用户新鲜度 分析业务开发
 */
object ChannelFreshnessTask extends ProcessData {

  //添加一个 ChannelFreshness 样例类，它封装要统计的四个业务字段：频道ID（channelID）、日期（date）、新用户（newCount）、老用户（oldCount）
  case class ChannelFreshness(channelId:String, date:String, newCount:Long, oldCount:Long)

  override def process(preDataStream: DataStream[ClickLogWide]): Unit = {
    /**
     * 1. 创建一个 ChannelFreshnessTask 单例对象
     * 2. 添加一个 ChannelFreshness 样例类，它封装要统计的四个业务字段：频道ID（channelID）、日期（date）、新用户
     * （newCount）、老用户（oldCount）
     * 3. 在 ChannelFreshnessTask 中编写一个 process 方法，接收预处理后的 DataStream
     * 4. 使用 flatMap 算子，将 ClickLog 对象转换为三个不同时间维度 ChannelFreshness
     * 5. 按照 频道ID 、 日期 进行分流
     * 6. 划分时间窗口（3秒一个窗口）
     * 7. 执行reduce合并计算
     * 8. 打印测试
     * 9. 将合并后的数据下沉到hbase
     */
      //老用户的判断条件：如果是之前的老用户并且是当前时间的新用户，那么老用户+1
      val  older = (isNew:Int, isDateNew:Int) => if(isNew == 0 && isDateNew==1) 1 else 0

    val frshnessDataStream: DataStream[ChannelFreshness] = preDataStream.flatMap(log => {
      List(
        //小时数据
        ChannelFreshness(log.channelID, log.yearMonthDayHour, log.isHourNew, older(log.isNew, log.isHourNew)),
        //日数据
        ChannelFreshness(log.channelID, log.yearMonthDay, log.isDayNew, older(log.isNew, log.isDayNew)),
        //月数据
        ChannelFreshness(log.channelID, log.yearMonth, log.isMonthNew, older(log.isNew, log.isMonthNew))
      )
    })

    //2. 按照 频道ID 、 年月日时 进行分流
    val groupedDataStream: KeyedStream[ChannelFreshness, String] = frshnessDataStream.keyBy(pvuv => pvuv.channelId + pvuv.date)

    //4. 划分时间窗口（3秒一个窗口）
    val windowDataStream: WindowedStream[ChannelFreshness, String, TimeWindow] = groupedDataStream.timeWindow(Time.seconds(3))

    //5. 进行合并计数统计
    val sumDataStream: DataStream[ChannelFreshness] = windowDataStream.reduce((f1, f2) =>
      ChannelFreshness(f1.channelId, f1.date, f1.newCount + f2.newCount, f1.oldCount + f2.oldCount)
    )

    //9. 将合并后的数据下沉到hbase
    sumDataStream.addSink(frshness=>{
      /**
       * 构建hbase的参数
       */
      val tableName = "channel_freshness"
      val cfName = "info"
      val rowKey = frshness.channelId + ":" + frshness.date
      val channelIdColName = "channelId"
      val dateColName = "date"
      val newCountColName = "newCount"
      val oldCountColName = "oldCount"

      //获取历史的数据+当前数据=写入数据
      val newCountInHbase = HBaseUtil.getData(tableName, rowKey, cfName, newCountColName)
      val oldCountInHbase = HBaseUtil.getData(tableName, rowKey, cfName, oldCountColName)

      var totalNewCount = 0L
      var totalOldCount = 0L

      //判断是否有历史的newcount数据
      if (StringUtils.isNotBlank(newCountInHbase)) {
        totalNewCount = newCountInHbase.toLong + frshness.newCount
      } else {
        totalNewCount = frshness.newCount
      }

      //判断是否有历史的oldcount数据
      if (StringUtils.isNotBlank(oldCountInHbase)) {
        totalOldCount = oldCountInHbase.toLong + frshness.oldCount
      } else {
        totalOldCount = frshness.oldCount
      }

      //将数据写入到数据库
      HBaseUtil.putMapData(tableName, rowKey, cfName, Map(
        channelIdColName -> frshness.channelId,
        dateColName -> frshness.date,
        newCountColName -> totalNewCount,
        oldCountColName -> totalOldCount
      ))
    })
  }
}
