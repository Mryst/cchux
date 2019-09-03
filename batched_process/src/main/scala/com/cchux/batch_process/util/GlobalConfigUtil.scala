package com.cchux.batch_process.util

import com.typesafe.config.ConfigFactory

/**
 * 读取配置文件公共类
 */
object GlobalConfigUtil {

  //读取配置文件信息，读取规则：application.conf->application.json->application.properties
  private lazy val config = ConfigFactory.load()

  //# Kafka集群地址
  def  bootstrapServers = config.getString("bootstrap.servers")
  //# ZooKeeper集群地址
  def zookeeperConnect = config.getString("zookeeper.connect")
  //# Kafka Topic名称
  def topic = config.getString("input.topic")
  //# 消费组ID
  def groupId = config.getString("group.id")
  //# 自动提交拉取到消费端的消息offset到kafka
  def enableAutoCommit = config.getString("enable.auto.commit")
  //# 自动提交offset到zookeeper的时间间隔单位（毫秒）
  def autoCommitIntervalMS = config.getString("auto.commit.interval.ms")
  //# 每次消费最新的数据
  def autoOffsetReset = config.getString("auto.offset.reset")

  def main(args: Array[String]): Unit = {
    println(bootstrapServers)
  }
}
