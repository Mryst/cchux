package com.cchux.task

import com.cchux.bean.ClickLogWide
import com.cchux.task.ChannelRealHotTask.ChannelRealHot
import com.cchux.util.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 完成实时频道 PU/UV 分析业务开发
 * 小时
 * 日
 * 月
 */
object ChannelPvUvTask extends ProcessData {

  //1. 创建一个 ChannelPvUvTask 单例对象
  case class ChannelPvUv(channelId:String, date:String, pv:Long, uv:Long)

  override def process(preDataStream: DataStream[ClickLogWide]): Unit = {
    /**
     * 实现思路：
     * 1. 使用map算子，将 ClickLog 对象转换为 ChannelPvUv
     * 2. 按照 频道ID 、 年月日时 进行分流
     * 3. 划分时间窗口（3秒一个窗口）
     * 4. 执行reduce合并计算
     * 5. 将合并后的数据下沉到hbase
     * 判断hbase中是否已经存在结果记录
     * 若存在，则获取后进行累加
     * 若不存在，则直接写入
     */
    val channelPvUvDataStream: DataStream[ChannelPvUv] = preDataStream.flatMap(log => {
      List(
        //小时数据
        ChannelPvUv(log.channelID, log.yearMonthDayHour, log.count.toLong, log.isHourNew),
        //日数据
        ChannelPvUv(log.channelID, log.yearMonthDay, log.count.toLong, log.isDayNew),
        //月数据
        ChannelPvUv(log.channelID, log.yearMonth, log.count.toLong, log.isMonthNew)
      )
    })

    //2. 按照 频道ID 、 年月日时 进行分流
    val groupedDataStream: KeyedStream[ChannelPvUv, String] = channelPvUvDataStream.keyBy(pvuv => pvuv.channelId + pvuv.date)

    //4. 划分时间窗口（3秒一个窗口）
    val windowDataStream: WindowedStream[ChannelPvUv, String, TimeWindow] = groupedDataStream.timeWindow(Time.seconds(3))

    //5. 进行合并计数统计
    val sumDataStream: DataStream[ChannelPvUv] = windowDataStream.reduce((pvuv1, pvuv2) =>
      ChannelPvUv(pvuv1.channelId, pvuv1.date, pvuv1.pv + pvuv2.pv, pvuv1.uv + pvuv2.uv)
    )

    //5. 将合并后的数据下沉到hbase
    sumDataStream.addSink(pvuv => {
      /**
       * 构建hbase的参数
       */
      val tableName = "channel_pvuv"
      val cfName = "info"
      val rowKey = pvuv.channelId + ":" + pvuv.date
      val channelIdColName = "channelId"
      val dateColName = "date"
      val uvColName = "uv"
      val pvColName = "pv"

      //获取历史的数据+当前数据=写入数据
      val pvInHbase = HBaseUtil.getData(tableName, rowKey, cfName, pvColName)
      val uvInHbase = HBaseUtil.getData(tableName, rowKey, cfName, uvColName)

      //总的访问量
      var totalPv = 0L
      var totalUv = 0L

      //判断是否有历史的pv数据
      if (StringUtils.isNotBlank(pvInHbase)) {
        totalPv = pvInHbase.toLong + pvuv.pv
      } else {
        totalPv = pvuv.pv
      }

      //判断是否有历史的Uv数据
      if (StringUtils.isNotBlank(uvInHbase)) {
        totalUv = uvInHbase.toLong + pvuv.uv
      } else {
        totalUv = pvuv.uv
      }

      //将数据写入到数据库
      HBaseUtil.putMapData(tableName, rowKey, cfName, Map(
        channelIdColName -> pvuv.channelId,
        dateColName -> pvuv.date,
        pvColName -> totalPv,
        uvColName -> totalUv
      ))
    })
  }
}
