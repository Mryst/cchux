package com.cchux.syncdb

import java.util.Properties

import com.cchux.syncdb.bean.Canal
import com.cchux.syncdb.task.PreprocessTask
import com.cchux.syncdb.util.{GlobalConfigUtil, HBaseUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 数据实时同步程序开发
 * 驱动类
 */
object App {
  def main(args: Array[String]): Unit = {
    /**
     * 实现思路：
     * 1：初始化运行环境
     * 2：设置并行度
     * 3：添加checkpoint
     * 4：添加水印支持
     * 5：业务处理
     */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2：设置并行度
    env.setParallelism(1)

    //3：添加checkpoint
    // 5秒启动一次checkpoint
    env.enableCheckpointing(5000)
    // 设置checkpoint只checkpoint一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // checkpoint超时的时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 允许的最大checkpoint并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭的时，触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint的地址
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink-checkpoint/"))

    /**
     * kafka整合flink
     * 1. 配置Kafka连接属性
     * 2. 使用 FlinkKafkaConsumer09 整合Kafka
     * 3. 添加一个source到当前Flink环境
     * 4. 启动 zookeeper
     * 5. 启动 kafka
     * 6. 运行Flink程序测试是否能够从Kafka中消费到数据
     */

    val props = new Properties()
    props.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
    props.setProperty("zookeeper.connect", GlobalConfigUtil.zookeeperConnect)
    props.setProperty("group.id",GlobalConfigUtil.groupId)
    props.setProperty("enable.auto.commit",GlobalConfigUtil.enableAutoCommit)
    props.setProperty("auto.commit.interval.ms",GlobalConfigUtil.autoCommitIntervalMS)
    //设置消费数据的位置
    props.setProperty("auto.offset.reset",GlobalConfigUtil.autoOffsetReset)

    //消费到kafka的数据要进行反序列化
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer")

    //设置kafka集群分区的动态感知, 每三秒钟检测一下分区的变化
    props.setProperty("flink.partition-discovery.interval-millis", "3000")

    //指定消费数据的主题
    val topic = GlobalConfigUtil.topic

    //创建消费者实例
    val consumer: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](
      topic,
      new SimpleStringSchema(),
      props
    )

    //构建数据源
    val kafkaStream: DataStream[String] = env.addSource(consumer)

    //将kafka消费出来的字符串转换成bean对象
    val canalDataStream: DataStream[Canal] = kafkaStream.map(log => {
      Canal(log)
    })

    //添加水印支持
    val waterMarkDataStream: DataStream[Canal] = canalDataStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Canal](Time.seconds(2)) {
        override def extractTimestamp(element: Canal): Long = {
          element.timestamp
        }
      }
    )

    //将kafka消费出来的数据进行预处理，将原始日志的bean对象转换成可以操作hbase的bean对象
    //因为原始日志是mysql中的一行数据，但是hbase是列式数据库，所以说需要将mysql的每条数据转换成一个的列表
    val preDataStream = PreprocessTask.process(waterMarkDataStream)

    //将数据更新到hbase数据库中
    preDataStream.addSink(hbaseOperation=>{
      hbaseOperation.opType match {
        case "DELETE"=>{
          //删除操作
          HBaseUtil.deleteData(hbaseOperation.tableName, hbaseOperation.rowKey, hbaseOperation.cfName)
        }
        case _=>{
          //更新操作
          HBaseUtil.putData(hbaseOperation.tableName, hbaseOperation.rowKey, hbaseOperation.cfName, hbaseOperation.colName, hbaseOperation.colValue)
        }
      }
    })

    waterMarkDataStream.print()
    env.execute()
  }
}
