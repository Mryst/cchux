package com.cchux.bean

/**
 * 点击日志实体对象
 */
case class ClickLogWide (var browserType:String,  //浏览器类型
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
                     var userID:String,        //用户id

                     var count:String,        // 用户访问的次数
                     var timestamp:String,     // 用户访问的时间
                     var address:String,     // 国家省份城市（拼接）
                     var yearMonth:String,     // 年月
                     var yearMonthDay:String,     // 年月日
                     var yearMonthDayHour:String,     // 年月日时
                     var isNew:Int,     // 是否为访问某个频道的新用户
                     var isHourNew:Int,     // 在某一小时内是否为某个频道的新用户
                     var isDayNew:Int,     // 在某一天是否为某个频道的新用户
                     var isMonthNew:Int     //
                        )
