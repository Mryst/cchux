package com.cchux.report.controller;


import com.alibaba.fastjson.JSON;
import com.cchux.report.bean.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/")
public class ReportController {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("/receive")
    public Map receiver(@RequestBody String json){

        Map<String,String> result = new HashMap<String,String>();
        //将上传的数据封装到map的对象中
        Message message = new Message();
        //接收传过来的消息体
        message.setMessage(json);
        //设置点击次数
        message.setCount(1);
        //设置访问时间，也就是eventTime
        message.setTimestamp(System.currentTimeMillis()+"");
        //需要将数据写入到kafka集群中
        String messageJson = JSON.toJSONString(message);

        //将数据写入到kafka集群
        kafkaTemplate.send(topic,messageJson);
        //返回值

        result.put("result","ok");

        return result;

    }


}
