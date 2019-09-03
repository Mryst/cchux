package com.cchux.batch_process.task

import com.cchux.batch_process.bean.OrderRecordWide
import com.cchux.batch_process.util.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._

/**
 * 统计时间维度的各种支付方式的订单数，订单金额
 */
object PaymethodTask {

  //1. 创建样例类 MerchantCountMoney ，包含以下字段：商家ID、订单数、支付金额
  case class  MerchantCountMoney(payMethod:String, date:String, totalCount:Long, totalMoney:Double)

  def process(preDataSet: DataSet[OrderRecordWide]) = {
    /**
     * 1. 创建样例类 MerchantCountMoney ，包含以下字段：支付方式、时间维度、订单数、支付金额
     * 2. 使用 flatMap 操作生成不同维度的数据
     * 3. 使用 groupBy 按照 支付方式 和 日期 进行分组
     * 4. 使用 reduceGroup 进行聚合计算
     * 5. 测试：打印结果
     * 6. 将使用 collect 收集计算结果，并转换为List
     * 7. 使用 foreach 将数据下沉到HBase的 analysis_merchant 表中
     */
    val merchantDataSet = preDataSet.flatMap(record => {
      List(
        MerchantCountMoney(record.payMethod, record.yearMonthDay, 1, record.payAmount),
        MerchantCountMoney(record.payMethod, record.yearMonth, 1, record.payAmount),
        MerchantCountMoney(record.payMethod, record.year, 1, record.payAmount)
      )
    })


    //3. 使用 groupBy 按照 支付方式 和 日期 进行分组
    val groupedDataSet = merchantDataSet.groupBy(record => {
      record.payMethod + record.date
    })

    //4. 使用 reduceGroup 进行聚合计算
    val reduceDataSet: DataSet[MerchantCountMoney] = groupedDataSet.reduceGroup(iter=>{
      iter.reduce((m1, m2)=>{
        MerchantCountMoney(m1.payMethod, m1.date, m1.totalCount+m2.totalCount, m1.totalMoney+m2.totalMoney)
      })
    })

    //5: 使用 foreach 将数据下沉到HBase的
    reduceDataSet.collect().foreach(pay=>{
      /**
       * 构建hbase的参数
       */
      val tableName = "analysis_paymethod"
      val cfName = "info"
      val rowKey = pay.payMethod+":"+pay.date
      val dateColName = "date"
      val payMethodColName = "payMethod"
      val countColName  = "count"
      val moneyColName = "money"

      //首先获取历史的总金额和订单数量
      val countInHbase: String = HBaseUtil.getData(tableName, rowKey, cfName, countColName)
      val moneyInHbase: String = HBaseUtil.getData(tableName, rowKey, cfName, moneyColName)

      //定义总金额和总的订单数量
      var totalCount = 0L
      var totalMoney:Double = 0

      if(StringUtils.isNotBlank(countInHbase)){
        totalCount = countInHbase.toLong +pay.totalCount
      }else {
        totalCount = pay.totalCount
      }

      if(StringUtils.isNotBlank(moneyInHbase)){
        totalMoney = moneyInHbase.toDouble +pay.totalMoney
      }else {
        totalMoney = pay.totalMoney
      }

      //将数据保存到hbase中
      HBaseUtil.putMapData(tableName, rowKey, cfName, Map(
        dateColName -> pay.date,
        payMethodColName -> pay.payMethod,
        countColName-> totalCount,
        moneyColName -> totalMoney
      ))

    })
  }
}
