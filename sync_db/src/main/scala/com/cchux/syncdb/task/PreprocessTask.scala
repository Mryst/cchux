package com.cchux.syncdb.task

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.cchux.syncdb.bean.{Canal, HbaseOperation}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer


/**
 * 将原始日志的bean对象转换成HbaseOperation
 */
object PreprocessTask {

  //定义样例类，用来存储每个列的对象
  case class ColNameValuePair(columnName:String, columnValue:String, isValid:Boolean)

  /**
   * 将原始日志的bean对象转换成可以方便hbase处理的列的集合
   * @param waterMarkDataStream
   * @return
   */
  def process(waterMarkDataStream: DataStream[Canal]) = {
    /**
     * 将原始日志的列值列表转换成HbaseOperation的集合
     */
    waterMarkDataStream.flatMap(log=>{
      //操作类型
      val opType = log.eventType
      //表名
      val tableName = s"mysql.${log.dbName}.${log.tableName}"
      //列族
      val cfName = "info"
      //rowkey
      //1：需要表中有主键列， 2：主键列是第一列
      val colNameValuePairs: List[ColNameValuePair] = parseColNameValuePairListByJson(log.columnValueList)
      //获取表的第一列作为rowkey
      val rowKey: String = colNameValuePairs(0).columnValue

      //进行模式匹配：插入操作需要取到所有的列，修改操作需要去isvalid是true的列，删除操作不需要取任何的列
      val updateColNameValuePairList: List[HbaseOperation] = opType match {
        case "INSERT" => {
          //如果是插入操作获取到所有的列值列表
          colNameValuePairs.map(colNameValue => {
            HbaseOperation(opType, tableName, cfName, rowKey, colNameValue.columnName, colNameValue.columnValue)
          }).toList
        }
        case "UPDATE" => {
          //如果是更新操作需要获取更新的字段的列值列表
          colNameValuePairs.filter(_.isValid).map(colNameValue => {
            HbaseOperation(opType, tableName, cfName, rowKey, colNameValue.columnName, colNameValue.columnValue)
          }).toList
        }
        case "DELETE" => {
          //删除操作不需要关心列的情况，因为根据rowkey删除
          List(HbaseOperation(opType, tableName, cfName, rowKey))
        }
      }

      //将要操作的字段列表集合返回
      updateColNameValuePairList
    })
  }

  /**
   * 解析列值列表
   * [{"columnName":"commodityId","columnValue":"2","isValid":false},{"columnName":"commodityName","columnValue":"阿迪达斯","isValid":false},{"columnName":"commodityTypeId","columnValue":"1","isValid":false},{"columnName":"originalPrice","columnValue":"900.0","isValid":false},{"columnName":"activityPrice","columnValue":"870.0","isValid":false}]
   */
  def parseColNameValuePairListByJson(columnValueList: String) = {
    //需要将json字符串转换成json对象
    val jsonArray: JSONArray = JSON.parseArray(columnValueList)

    //定义一个集合用来返回数据
    val colNameValuePairList: ListBuffer[ColNameValuePair] = ListBuffer[ColNameValuePair]()

    //循环遍历json集合列表
    for (i<-0  until jsonArray.size()){
      //{"columnName":"commodityId","columnValue":"2","isValid":false}
      val jsonObject: JSONObject = jsonArray.getJSONObject(i)

      //创建bean对象
      val colNameValuePair = ColNameValuePair(
        jsonObject.getString("columnName"),
        jsonObject.getString("columnValue"),
        jsonObject.getBoolean("isValid")
      )

      //将创建的bean对象放到集合中
      colNameValuePairList.append(colNameValuePair)
    }

    //将集合转换成列表返回
    colNameValuePairList.toList
  }

}
