package com.cchux.task

import com.cchux.bean.{ClickLogWide, Message}
import com.cchux.util.{DateUtil, HBaseUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

/**
 * 数据预处理的业务开发
 *
 */
object PreprocessTask {

  //定义样例类，封装是否新用户
  case class IsNewWapper(isNew: Int, isHourNew: Int, isDayNew: Int, isMonthNew: Int)

  /**
   * 将点击流日志的字段进行拓宽操作
   *
   * @param waterMarkDataStream
   */
  def process(waterMarkDataStream: DataStream[Message]) = {

    waterMarkDataStream.map(msg => {
      /**
       * count 用户访问的次数
       * timestamp 用户访问的时间
       * address 国家省份城市（拼接）
       * yearMonth 年月
       * yearMonthDay 年月日
       * yearMonthDayHour 年月日时
       */
      //获取点击次数
      val count: Long = msg.count

      //获取访问时间
      val timestamp: Long = msg.timestamp

      //国家省份城市（拼接）
      val address: String = msg.message.country + msg.message.province + msg.message.city

      //年月
      val month = DateUtil.timestamp2Date(msg.timestamp, "yyyyMM")
      //年月日
      val day = DateUtil.timestamp2Date(msg.timestamp, "yyyyMMdd")
      //年月日时
      val hour = DateUtil.timestamp2Date(msg.timestamp, "yyyyMMddHH")

      //分析是否是新用户
      val newWapper = analysisIsNewUser(msg)

      //将拓宽后的对象返回
      ClickLogWide(
        msg.message.browserType, //浏览器类型
        msg.message.categoryID, //商品类型id
        msg.message.channelID, //频道id
        msg.message.city, //城市
        msg.message.country, //国家
        msg.message.entryTime, //进入时间
        msg.message.leaveTime, //离开时间
        msg.message.network, //运营商
        msg.message.produceID, //产品id
        msg.message.province, //省份
        msg.message.source, //数据来源
        msg.message.userID, //用户id

        count.toString, // 用户访问的次数
        timestamp.toString, // 用户访问的时间
        address, // 国家省份城市（拼接）
        month, // 年月
        day, // 年月日
        hour, // 年月日时
        newWapper.isNew, // 是否为访问某个频道的新用户
        newWapper.isHourNew, // 在某一小时内是否为某个频道的新用户
        newWapper.isDayNew, // 在某一天是否为某个频道的新用户
        newWapper.isMonthNew
      )
    })
  }


  /**
   * 判断是否是新用户
   * @param msg
   * @return
   */
  def analysisIsNewUser(msg: Message) ={
    //新用户用1标识   老用户用0标识
    var isNew:Int =  0     // 是否为访问某个频道的新用户
    var isHourNew:Int = 0     // 在某一小时内是否为某个频道的新用户
    var isDayNew:Int =  0     // 在某一天是否为某个频道的新用户
    var isMonthNew:Int =  0     //是否是月内的某个频道的新用户

    //需要访问hbase数据库，首先构造hbase的访问参数
    val tableName = "user_history"
    val cfName = "info"
    val rowKey = msg.message.userID+":"+msg.message.channelID
    val userIdColName = "userId"
    val channelIdColName = "channelId"
    val visitedColName = "lastVisitedTime"

    //访问数据库
    val lastVisitedTimeInHbase: String = HBaseUtil.getData(tableName, rowKey, cfName, visitedColName)

    //判断最后访问时间是否为空
    if(StringUtils.isBlank(lastVisitedTimeInHbase)){
      //如果数据库钟没有访问记录，那么肯定是新用户，不管是月、日、小时
      isNew = 1
      isHourNew = 1
      isDayNew =1
      isMonthNew=1

      //将访问记录写入到数据库钟
      HBaseUtil.putMapData(tableName, rowKey, cfName, Map(
        userIdColName -> msg.message.userID,
        channelIdColName -> msg.message.channelID,
        visitedColName -> msg.timestamp
      ))
    }else{
      //如果数据库钟有数据，那么就是老用户，但是需要判断是否是小时、日、月的老用户
      isNew = 0

      //获取最后访问时间的月、日、小时
      val hourInHbase: String = DateUtil.timestamp2Date(lastVisitedTimeInHbase.toLong, "yyyyMMddHH")  //2019083116
      val dayInHbase = DateUtil.timestamp2Date(lastVisitedTimeInHbase.toLong, "yyyyMMdd")
      val monthInHbase = DateUtil.timestamp2Date(lastVisitedTimeInHbase.toLong, "yyyyMM")

      //获取访问时间的月日小时
      val hourInData = DateUtil.timestamp2Date(msg.timestamp, "yyyyMMddHH")       //2019083117
      val dayInData = DateUtil.timestamp2Date(msg.timestamp, "yyyyMMdd")
      val monthInData = DateUtil.timestamp2Date(msg.timestamp, "yyyyMM")

      //判断是否是小时内的新用户
      if(hourInData > hourInHbase){
        //如果当前访问日志的小时数超过了数据库保存的最后访问时间的小时数，那么就是新用户（小时内）
        isHourNew = 1
      }
      else{
        isHourNew = 0
      }

      //判断是否是天内的新用户
      if(dayInData > dayInHbase){
        //如果当前访问日志的日期数超过了数据库保存的最后访问时间的日期数，那么就是新用户（日期内）
        isDayNew = 1
      }
      else{
        isDayNew = 0
      }

      //判断是否是月内的新用户
      if(monthInData > monthInHbase){
        //如果当前访问日志的月数超过了数据库保存的最后访问时间的月数，那么就是新用户（月内）
        isMonthNew = 1
      }
      else{
        isMonthNew = 0
      }

      //将最后访问时间更新掉
      HBaseUtil.putData(tableName, rowKey, cfName, visitedColName, msg.timestamp)
    }

    //将分析好的字段进行返回
    IsNewWapper(isNew, isHourNew, isDayNew, isMonthNew)
  }

}
