package com.cchux


import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cchux.bean.{ClickLog, Message}
import com.cchux.task.{ChannelFreshnessTask, ChannelPvUvTask, ChannelRealHotTask, PreprocessTask}
import com.cchux.util.GlobalConfigUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09



object APP {

  def main(args: Array[String]): Unit = {

    /*
      * 1. 创建main方法，获取StreamExecutionEnvironment运行环境
      * 2. 设置流处理的时间为 EventTime ，使用数据发生的时间来进行数据处理
      * 3. 将Flink默认的开发环境并行度设置为1
      */

    //获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置流处理的时间为 EventTime ，使用数据发生的时间来进行数据处理
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //3. 将Flink默认的开发环境并行度设置为1
    env.setParallelism(1)

    //添加checkpoint的支持
    env.enableCheckpointing(5000)
    //设置checkpoint执行且仅执行一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    //checkpoint超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //允许的最大checkpoin并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //当程序关闭时，出发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置checkpoint的地址
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/flink-checkpoint"))

    /*
      * kafka整合flink
      * 1. 配置Kafka连接属性
      * 2. 使用 FlinkKafkaConsumer09 整合Kafka
      * 3. 添加一个source到当前Flink环境
      * 4. 启动 zookeeper
      * 5. 启动 kafka
      * 6. 运行Flink程序测试是否能够从Kafka中消费到数据
      */

      val props = new Properties()

      props.setProperty("bootstrap.servers",GlobalConfigUtil.bootstrapServers)
      props.setProperty("zookeeper.connect",GlobalConfigUtil.zookeeperConnect)
      props.setProperty("group.id",GlobalConfigUtil.groupId)
      props.setProperty("enable.auto.commit",GlobalConfigUtil.enableAutoCommit)
      props.setProperty("auto.commit.interval.ms",GlobalConfigUtil.autoCommitIntervalMS)

      //设置消费数据的位置
    props.setProperty("auto.offset.reset",GlobalConfigUtil.autoOffsetReset)

    //消费到kafka的数据要进行反序列化
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer")

    //设置kafka集群分区的动态感知，没三秒钟检测一下分区的变化
    props.setProperty("flink.partition-discovery.interval-millis", "3000")

    //指定消费数据的主题
    val topic = GlobalConfigUtil.topic

    //创建消费者实例
    val consumer: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](
      topic,
      new SimpleStringSchema(),
      props
    )
    val kafkaStream: DataStream[String] = env.addSource(consumer)

    val messageDataStream: DataStream[Message] = kafkaStream.map(msg => {
      //将字符串转换成对象
      //JsonObject:{"count":1,"message":"{\"browserType\":\"火狐\",\"categoryID\":8,\"channelID\":18,\"city\":\"ShiJiaZhuang\",\"country\":\"china\",\"entryTime\":1544634060000,\"leaveTime\":1544634060000,\"network\":\"电信\",\"produceID\":8,\"province\":\"HeNan\",\"source\":\"必应跳转\",\"userID\":4}","timestamp":"1567234919908"}
      //JsonArray:[{"count":1,"message":"{\"browserType\":\"火狐\",\"categoryID\":8,\"channelID\":18,\"city\":\"ShiJiaZhuang\",\"country\":\"china\",\"entryTime\":1544634060000,\"leaveTime\":1544634060000,\"network\":\"电信\",\"produceID\":8,\"province\":\"HeNan\",\"source\":\"必应跳转\",\"userID\":4}","timestamp":"1567234919908"}]

      val jsonObject: JSONObject = JSON.parseObject(msg)

      //获取点击次数
      val count = jsonObject.getLong("count")
      val message = jsonObject.getString("message")
      val timestamp = jsonObject.getString("timestamp")

      Message(count, ClickLog(message), timestamp.toLong)
    })


    //添加水印支持
    val waterMarkDataStream: DataStream[Message] = messageDataStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Message](Time.seconds(0)) {
        override def extractTimestamp(element: Message): Long = {
          element.timestamp
        }
      }
    )


    //业务处理方法
    process(waterMarkDataStream)

    //打印测试
    //messageDataStream.print()

    //启动任务
    env.execute()

  }

  def process(waterMarkDataStream: DataStream[Message]) = {
    //todo 1：完成点击流日志 数据预处理 业务开发
    val preDataStream = PreprocessTask.process(waterMarkDataStream)

    //todo 2：完成实时频道 热点 分析业务开发
    //ChannelRealHotTask.process(preDataStream)

    //todo 3：完成实时频道 PU/UV 分析业务开发
    //ChannelPvUvTask.process(preDataStream)

    //todo 4：完成实时频道 用户新鲜度 分析业务开发
    //ChannelFreshnessTask.process(preDataStream)

    preDataStream.print()
  }

}
