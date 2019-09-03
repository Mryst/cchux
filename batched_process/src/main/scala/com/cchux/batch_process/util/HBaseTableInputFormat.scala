package com.cchux.batch_process.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.beans.BeanProperty

/**
 * 定义hbase操作的数据源对象
 * @param tableName   hbase的表名
 *
 *                    返回值类型：Tuple2[param1, parm2]：param1表示rowkey，param2：数据集
 */
class HBaseTableInputFormat(var tableName:String) extends TableInputFormat[org.apache.flink.api.java.tuple.Tuple2[String, String]]{

  //定义一个连接对象
  var conn: Connection = _

  //获取一个扫描器
  override def getScanner: Scan = {
    scan = new Scan()
    scan
  }

  //获取表名
  override def getTableName: String = tableName

  //将获取到的数据转换成元组对象
  override def mapResultToTuple(result: Result): org.apache.flink.api.java.tuple.Tuple2[String, String] = {
    //获取rowkey
    val rowkey = Bytes.toString(result.getRow)

    // 获取列单元格
    val cellArray: Array[Cell] = result.rawCells()   //获取单元格的集合

    //定义jsonobject对象
    val jsonObject = new JSONObject

    //循环遍历单元格对象列表
    for(i <- 0 until cellArray.size) {
      //获取到列族的名字
      val columnFamilyName = Bytes.toString(CellUtil.cloneFamily(cellArray(i)))
      //获取列名
      val columnName = Bytes.toString(CellUtil.cloneQualifier(cellArray(i)))
      //获取列的值
      val value = Bytes.toString(CellUtil.cloneValue(cellArray(i)))

      jsonObject.put(columnName, value)
    }

    //jsonObject：{"orderId":500,"userId":"5"......}
    //将获取到的列的集合的json字符串转换成元组对象（rowkey， 列的集合字符串json对象）
    new org.apache.flink.api.java.tuple.Tuple2[String, String](rowkey, JSON.toJSONString(jsonObject, SerializerFeature.DisableCircularReferenceDetect))
  }

  override def close(): Unit = {
    if(table != null) table.close()
    if(conn != null) conn.close()
  }
}
