package com.cchux.syncdb.bean

import com.alibaba.fastjson.{JSON, JSONObject}


/**
 * canal同步到kafka中的数据，转换成样例类
 * {"emptyCount":1,"logFileName":"mysql-bin.000001","dbName":"pyg","logFileOffset":228,"eventType":"INSERT","columnValueList":[{"columnName":"commodityId","columnValue":"1","isValid":true},{"columnName":"commodityName","columnValue":"耐克","isValid":true},{"columnName":"commodityTypeId","columnValue":"1","isValid":true},{"columnName":"originalPrice","columnValue":"888.0","isValid":true},{"columnName":"activityPrice","columnValue":"820.0","isValid":true}],"tableName":"commodity","timestamp":1567391796000}
 *
 */
case class Canal(
                  emptyCount:Long,  //操作编号
                  logFileName:String, //数据来自于binlog日志文件的名字
                  dbName:String,  //数据库名字
                  logFileOffset:Long, //偏移量
                  eventType:String,//操作类型
                  columnValueList:String, //列值列表集合
                  tableName:String, //表名
                  timestamp:Long  //操作时间
                )

object Canal{
  def apply(json:String): Canal = {

    //将字符串转换成json对象
    val jsonObject: JSONObject = JSON.parseObject(json)

    Canal(
      jsonObject.getLong("emptyCount"),//操作编号
      jsonObject.getString("logFileName"), //数据来自于binlog日志文件的名字
      jsonObject.getString("dbName"),  //数据库名字
      jsonObject.getLong("logFileOffset"),//偏移量
      jsonObject.getString("eventType"),//操作类型
      jsonObject.getString("columnValueList"), //列值列表集合
      jsonObject.getString("tableName"), //表名
      jsonObject.getLong("timestamp")//操作时间
    )
  }
}
