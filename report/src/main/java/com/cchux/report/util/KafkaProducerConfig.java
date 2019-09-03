package com.cchux.report.util;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 读取配置文件信息
 */
@Configuration
public class KafkaProducerConfig {

    //#kafka的服务器地址
    @Value("${kafka.bootstrap_servers_config}")
    private String  bootstrap_servers;
    //#如果出现发送失败的情况，允许重试的次数
    @Value("${kafka.retries_config}")
    private int retries_config;
    //#每个批次发送多大的数据
    @Value("${kafka.batch_size_config}")
    private int batch_size_config;
    //#定时发送，达到1ms发送
    @Value("${kafka.linger_ms_config}")
    private int linger_ms_config;
    //#缓存的大小
    @Value("${kafka.buffer_memory_config}")
    private String buffer_memory_config;
    //#TOPIC的名字
    @Value("kafka.topic")
    private String topic;

    @Bean
    public  KafkaTemplate kafkaTemplate(){
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        config.put(ProducerConfig.RETRIES_CONFIG, retries_config);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, batch_size_config);
        config.put(ProducerConfig.LINGER_MS_CONFIG, linger_ms_config);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, buffer_memory_config);

        //将数据写入到kafka集群的时候，需要指定key和value的序列化
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //创建生产者实例
        ProducerFactory producerFactory = new DefaultKafkaProducerFactory(config);
        return new KafkaTemplate(producerFactory);
    }

//    public static void main(String[] args) {
//        KafkaProducerConfig config = new KafkaProducerConfig();
//
//        System.out.println(config.bootstrap_servers);
//    }
}
