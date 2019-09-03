package com.cchux.batch_process

import com.cchux.batch_process.bean.{OrderRecord, OrderRecordWide}
import com.cchux.batch_process.task.{PaymethodTask, PreprocessTask}
import com.cchux.batch_process.util.HBaseTableInputFormat
import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * 离线计算的驱动类
 * 按照天、月、年维度统计不同支付方式的订单数量、支付金额
 */
object App {
  def main(args: Array[String]): Unit = {
    /**
     * 实现思路：
     * 1：构建批处理运行环境
     * 2：构建source数据源
     * 3：进行transformation计算
     * 4：将数据落地，将数据回写到hbase中，供bi展示
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    //2：构建source数据源
    //(1,{"benefitAmount":"8.0","orderAmount":"1234.0","payAmount":"100.0","activityNum":"0","createTime":"2018-11-29 00:00:00","merchantId":"1","orderId":"1","payTime":"2018-11-29 00:00:00","payMethod":"3","voucherAmount":"9.0","commodityId":"1102","userId":"1"})
    //(10,{"benefitAmount":"8.0","orderAmount":"3000.0","payAmount":"258.0","activityNum":"0","createTime":"2018-11-29 00:00:00","merchantId":"5","orderId":"10","payTime":"2018-11-29 00:00:00","payMethod":"3","voucherAmount":"8.0","commodityId":"1101","userId":"5"})
    val hbaseDataSet: DataSet[tuple.Tuple2[String, String]] = env.createInput(new HBaseTableInputFormat("mysql.pyg.orderRecord"))

    //3：进行transformation计算
    val orderRecordDataSet: DataSet[OrderRecord] = hbaseDataSet.map(log => {
      OrderRecord(log.f1)
    })

    //进行业务统计之前需要进行数据的预处理操作，需要将原始的数据拓宽年、月、日三个属性
    val preDataSet: DataSet[OrderRecordWide] = PreprocessTask.process(orderRecordDataSet)

    //天、月、年维度统计不同支付方式的订单数量、支付金额
    PaymethodTask.process(preDataSet)


    hbaseDataSet.print()
  }
}
