package com.cchux.bean

/**
 * 定义消息的实体对象
 */
case class Message(count:Long,    //点击次数
                   message: ClickLog,  //消息内容
                   timestamp:Long)    //点击时间
