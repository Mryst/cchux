package com.cchux.batch_process.bean

import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * 定义订单的原始样例类
 * //(1,{"benefitAmount":"8.0","orderAmount":"1234.0","payAmount":"100.0","activityNum":"0","createTime":"2018-11-29 00:00:00","merchantId":"1","orderId":"1","payTime":"2018-11-29 00:00:00","payMethod":"3","voucherAmount":"9.0","commodityId":"1102","userId":"1"})
 *
 */
case class OrderRecord(
                        var orderId:String,// 订单ID
                        var userId:String,// ⽤户ID
                        var merchantId:String,// 商家ID
                        var orderAmount:Double,// 下单金额
                        var payAmount:Double,// ⽀付金额
                        var payMethod:String,// ⽀付⽅方式
                        var payTime:String,// ⽀付时间
                        var benefitAmount:Double,// 红包金额
                        var voucherAmount:Double,// 代⾦券金额
                        var commodityId:String,// 产品id
                        var activityNum:String,// 活动编号（⼤大于0代表有活动）
                        var createTime:String// 创建时间
                      )

object OrderRecord{
  def apply(json:String): OrderRecord ={
    val jsonObject: JSONObject = JSON.parseObject(json)

    OrderRecord(
     jsonObject.getString("orderId"),// 订单ID
     jsonObject.getString("userId"),// ⽤户ID
     jsonObject.getString("merchantId"),// 商家ID
     jsonObject.getDouble("orderAmount"),// 下单金额
     jsonObject.getDouble("payAmount"),// ⽀付金额
     jsonObject.getString("payMethod"),// ⽀付⽅方式
     jsonObject.getString("payTime"),// ⽀付时间
     jsonObject.getDouble("benefitAmount"),// 红包金额
     jsonObject.getDouble("voucherAmount"),// 代⾦券金额
     jsonObject.getString("commodityId"),// 产品id
     jsonObject.getString("activityNum"),// 活动编号（⼤大于0代表有活动）
     jsonObject.getString("createTime")// 创建时间
    )
  }
}