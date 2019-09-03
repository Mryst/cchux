package com.cchux.bean

import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * 点击日志实体对象
 */
case class ClickLog (var browserType:String,  //浏览器类型
                    var categoryID:String,    //商品类型id
                    var channelID:String,     //频道id
                    var city:String,          //城市
                    var country:String,       //国家
                    var entryTime:String,     //进入时间
                    var leaveTime:String,     //离开时间
                    var network:String,       //运营商
                    var produceID:String,     //产品id
                    var province:String,      //省份
                    var source:String,        //数据来源
                    var userID:String        //用户id
                    )

//点击日志的伴生对象
object ClickLog{

  //实现伴生对象的apply方法
  def apply(json:String): ClickLog = {

    //解析消息体内容
    val jsonObject: JSONObject = JSON.parseObject(json)

    //将字符串转换成对象返回
    ClickLog(
      jsonObject.getString("browserType"),  //浏览器类型
      jsonObject.getString("categoryID"),    //商品类型id
      jsonObject.getString("channelID"),     //频道id
      jsonObject.getString("city"),          //城市
      jsonObject.getString("country"),       //国家
      jsonObject.getString("entryTime"),     //进入时间
      jsonObject.getString("leaveTime"),     //离开时间
      jsonObject.getString("network"),       //运营商
      jsonObject.getString("produceID"),     //产品id
      jsonObject.getString("province"),      //省份
      jsonObject.getString("source"),        //数据来源
      jsonObject.getString("userID")       //用户id
    )
  }
}
