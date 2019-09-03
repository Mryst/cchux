package com.cchux.syncdb.bean

/**
 * 定义可以操作hbase的样例类
 * 向hbase写数据的时候需要哪些字段
 */
case class HbaseOperation(
                         var opType:String,       //操作类型
                         var tableName:String,    //表名
                         var cfName:String = "info",       //列族名
                         var rowKey:String,       //主键列
                         val colName:String = "",      //列名
                         val colValue:String =""      //列值
                         )
