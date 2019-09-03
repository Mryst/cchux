package com.cchux.batch_process.task

import com.cchux.batch_process.bean.{OrderRecord, OrderRecordWide}
import com.cchux.batch_process.util.DateUtil
import org.apache.flink.api.scala._

/**
 * 对hbase原始的字段进行拓宽
 */
object PreprocessTask {
  def process(orderRecordDataSet: DataSet[OrderRecord]) = {
    orderRecordDataSet.map(record => {
      //获取支付时间
      val payTime = record.payTime

      //年月日
      var yearMonthDay = DateUtil.formatDateTime(payTime, "yyyyMMdd")
      var yearMonth = DateUtil.formatDateTime(payTime, "yyyyMM")
      var year = DateUtil.formatDateTime(payTime, "yyyy")

      //将拓宽后字段的对象返回
      OrderRecordWide(
        record.orderId, // 订单ID
        record.userId, // ⽤户ID
        record.merchantId, // 商家ID
        record.orderAmount, // 下单金额
        record.payAmount, // ⽀付金额
        record.payMethod, // ⽀付⽅方式
        record.payTime, // ⽀付时间
        record.benefitAmount, // 红包金额
        record.voucherAmount, // 代⾦券金额
        record.commodityId, // 产品id
        record.activityNum, // 活动编号（⼤大于0代表有活动）
        record.createTime, // 创建时间
        yearMonthDay,
        yearMonth,
        year
      )
    })
  }

}
